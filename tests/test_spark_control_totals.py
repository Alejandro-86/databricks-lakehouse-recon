from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import Row

from databricks_lakehouse_recon.reconciliation import Layer
from databricks_lakehouse_recon.spark_control_totals import (
    SparkControlTotals,
    compact_before_write,
    deduplicate_bronze,
    rebalance_for_wide_operation,
)
from databricks_lakehouse_recon.spark_session import replace_table


def _bronze_df(spark):
    rows = [
        Row(event_id="E1", account_id="A1", signed_amount_minor=100,
            occurred_at=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 9, 1, 10, 0, 1, tzinfo=UTC)),
        Row(event_id="E1", account_id="A1", signed_amount_minor=100,  # duplicate re-delivery
            occurred_at=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 9, 1, 10, 5, 0, tzinfo=UTC)),
        Row(event_id="E2", account_id="A2", signed_amount_minor=-50,
            occurred_at=datetime(2026, 9, 1, 11, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 9, 1, 11, 0, 1, tzinfo=UTC)),
    ]
    return spark.createDataFrame(rows)


def test_deduplicate_bronze_keeps_first_arrival(spark):
    original = _bronze_df(spark)
    both_e1 = [r["ingested_at"] for r in original.filter("event_id = 'E1'").collect()]
    earliest = min(both_e1)

    deduped = deduplicate_bronze(original)
    assert deduped.count() == 2
    kept = deduped.filter("event_id = 'E1'").collect()[0]
    # Compare against the earliest of the two source rows rather than a hardcoded
    # tz-aware literal — Spark's collect() returns timestamps in the session's
    # local timezone, not UTC, so a literal would be environment-dependent.
    assert kept["ingested_at"] == earliest


def test_spark_control_totals_fetch_matches_manual_aggregation(spark, settings):
    bronze_schema = f"{settings.catalog}.{settings.bronze_schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
    source = _bronze_df(spark)
    df = source.withColumn("business_date", source.occurred_at.cast("date"))
    replace_table(spark, df, settings.bronze_transactions)

    totals = SparkControlTotals(spark).fetch(settings.bronze_transactions, Layer.BRONZE, dedupe=True)
    assert len(totals) == 1
    assert totals[0].row_count == 2
    assert totals[0].distinct_event_ids == 2
    assert totals[0].amount_minor_sum == 50  # 100 + -50, duplicate excluded


def test_compact_before_write_reduces_partition_count(spark):
    df = spark.range(100).repartition(20)
    assert df.rdd.getNumPartitions() == 20
    compacted = compact_before_write(df, 4)
    assert compacted.rdd.getNumPartitions() == 4


def test_rebalance_increases_partitions_with_full_shuffle(spark):
    df = spark.range(100).coalesce(1)
    assert df.rdd.getNumPartitions() == 1
    rebalanced = rebalance_for_wide_operation(df, 8)
    assert rebalanced.rdd.getNumPartitions() == 8
