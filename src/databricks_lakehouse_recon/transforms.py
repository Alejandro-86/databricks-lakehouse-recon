"""Bronze -> silver -> gold, in PySpark's DataFrame API.

Three jobs, same split of responsibility as the dbt models in
`lakehouse-recon-platform`: silver deduplicates and quarantines, gold builds the
star schema. The difference here is these are plain Spark jobs, not dbt-databricks
models — deliberately, so the Spark-specific mechanics (the explicit
coalesce/repartition choices, the single action per job) are visible in Python
rather than hidden behind a templating layer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks_lakehouse_recon.config import Settings
from databricks_lakehouse_recon.spark_control_totals import compact_before_write, deduplicate_bronze


def build_silver(spark: SparkSession, settings: Settings) -> tuple[DataFrame, DataFrame]:
    """Returns (silver_transactions, rejected) — write both, don't drop rejects silently."""
    bronze = spark.read.table(settings.bronze_transactions)
    deduped = deduplicate_bronze(bronze)

    valid_mask = (
        F.col("event_id").isNotNull()
        & F.col("currency").isNotNull()
        & (F.col("signed_amount_minor") != 0)
    )

    silver = (
        deduped.filter(valid_mask)
        .withColumn("business_date", F.to_date("occurred_at"))
        .select(
            "event_id", "account_id", "amount_minor", "signed_amount_minor", "currency",
            "direction", "transaction_type", "occurred_at", "business_date",
            "source_system", "ingested_at", "batch_id",
        )
    )

    rejected = (
        deduped.filter(~valid_mask)
        .withColumn(
            "rejection_reason",
            F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
            .when(F.col("currency").isNull(), F.lit("missing_currency"))
            .when(F.col("signed_amount_minor") == 0, F.lit("zero_amount"))
            .otherwise(F.lit("unknown")),
        )
    )

    # Selective filters typically drop most rows; coalesce avoids a pile of tiny
    # files on write without paying for a full shuffle to rebalance them.
    return compact_before_write(silver, 4), compact_before_write(rejected, 1)


def build_gold(spark: SparkSession, settings: Settings, *, inject_break: bool = False) -> dict[str, DataFrame]:
    """Returns the star schema: fct_transaction + dim_account.

    ``inject_break`` mirrors the dbt ``--vars 'inject_break: true'`` demo in
    `lakehouse-recon-platform` — dropping FX conversions is the same "someone
    added a WHERE clause for a good local reason and silently changed a
    reported total" incident shape, reproduced here on Delta.
    """
    silver = spark.read.table(settings.silver_transactions)
    if inject_break:
        silver = silver.filter(F.col("transaction_type") != "fx_conversion")

    fact = silver.select(
        "event_id", "account_id", "currency", "business_date", "occurred_at",
        "direction", "transaction_type", "signed_amount_minor", "source_system",
    )

    dim_account = silver.groupBy("account_id").agg(
        F.min("occurred_at").alias("first_seen_at"),
        F.max("occurred_at").alias("last_seen_at"),
        F.count("*").alias("transaction_count"),
        F.sum("signed_amount_minor").alias("net_amount_minor"),
    )

    return {"fct_transaction": fact, "dim_account": dim_account}
