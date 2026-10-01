"""Spark/Delta-native control-total computation — the only thing that changed
when this platform was ported from Trino/Iceberg.

Deliberately uses the **DataFrame API**, not SQL strings, unlike the Trino version
(`control_totals.py` in `lakehouse-recon-platform` builds parameterised SQL). Both
are legitimate; the contrast is the point — this module demonstrates Spark
transformations directly, since that is specifically what gets probed in a
Databricks-platform context.

Two Spark concepts are explicit here on purpose, not left implicit:

* **Lazy evaluation.** Every ``groupBy``/``agg``/``withColumn`` below is a
  transformation — Spark builds a logical plan and does nothing until ``collect()``
  is called at the bottom of :meth:`SparkControlTotals.fetch`. That single
  ``collect()` is the only action in the whole module; everything above it is
  plan-building, and Catalyst is free to reorder/fuse/push-down across the whole
  chain before anything actually runs.
* **coalesce vs. repartition**, used for the reason each is actually right for,
  not interchangeably: :func:`compact_before_write` uses ``coalesce`` because it's
  cheap and the goal is fewer output files, not perfectly even ones.
  ``deduplicate_bronze`` uses a window function (not partition-count manipulation
  at all) because the correctness property needed is "first arrival wins per key,"
  which ``coalesce``/``repartition`` cannot express.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from databricks_lakehouse_recon.reconciliation import ControlTotal, Layer


def deduplicate_bronze(bronze: DataFrame, *, key_column: str = "event_id", order_by: str = "ingested_at") -> DataFrame:
    """First-arrival-wins dedup over a column Spark has no built-in op for.

    Bronze is append-only and keeps at-least-once duplicates on purpose (see
    ``events.py``/``generator.py``) — this is the read-time view that makes a
    duplicate-bearing bronze table comparable to a deduplicated silver table,
    without mutating bronze itself.
    """
    ranked = bronze.withColumn(
        "_rn", F.row_number().over(Window.partitionBy(key_column).orderBy(F.col(order_by).asc()))
    )
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def compact_before_write(df: DataFrame, target_partitions: int) -> DataFrame:
    """Cheap partition reduction before a write — accepts imbalance for speed.

    ``coalesce`` avoids a full shuffle by merging existing partitions on the same
    executor. The trade-off, stated rather than hidden: if the upstream data is
    already skewed, coalesce preserves that skew instead of fixing it. That's the
    right call here because the goal is avoiding a pile of tiny output files
    after a selective filter, not perfectly even partitions.
    """
    return df.coalesce(target_partitions)


def rebalance_for_wide_operation(df: DataFrame, target_partitions: int, by: str | None = None) -> DataFrame:
    """Full-shuffle rebalance before a skew-sensitive wide operation (large join/groupBy).

    ``repartition`` costs more than ``coalesce`` — it moves data across the
    network — but it's the only one of the two that can genuinely increase
    partition count or repartition by a key, which is what even distribution
    ahead of a large join actually needs.
    """
    return df.repartition(target_partitions, by) if by else df.repartition(target_partitions)


class SparkControlTotals:
    """Produces :class:`ControlTotal` objects from a Delta table via Spark aggregation."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def fetch(
        self,
        table: str,
        layer: Layer,
        *,
        partition_col: str = "business_date",
        amount_column: str = "signed_amount_minor",
        key_column: str = "event_id",
        dedupe: bool = False,
    ) -> list[ControlTotal]:
        df = self.spark.read.table(table)
        if "business_date" not in df.columns and partition_col == "business_date":
            df = df.withColumn("business_date", F.to_date("occurred_at"))
        if dedupe:
            df = deduplicate_bronze(df, key_column=key_column)

        # --- everything above this line is lazy: no Spark job has run yet ---
        aggregated = df.groupBy(partition_col).agg(
            F.count("*").alias("row_count"),
            F.coalesce(F.sum(amount_column), F.lit(0)).alias("amount_minor_sum"),
            F.countDistinct(key_column).alias("distinct_event_ids"),
        ).orderBy(partition_col)

        rows = aggregated.collect()  # <-- the one action; triggers the whole plan above
        return [
            ControlTotal(
                layer=layer,
                partition_key=str(row[partition_col]),
                row_count=int(row["row_count"]),
                amount_minor_sum=int(row["amount_minor_sum"]),
                distinct_event_ids=int(row["distinct_event_ids"]),
            )
            for row in rows
        ]
