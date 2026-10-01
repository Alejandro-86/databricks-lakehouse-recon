"""Medallion bootstrap on Unity Catalog + Delta.

Silver and gold are built by ``transforms.py``, not defined here as DDL — same
split as the Trino/Iceberg version, for the same reason: two places defining the
same table is how "the repo doesn't match prod" incidents start.

**A real Delta-vs-Iceberg difference, found by running this, not assumed:**
Iceberg supports *hidden partitioning* — ``PARTITIONED BY (month(occurred_at))``
is valid DDL, partitioning directly on a transform of another column. Delta has
no equivalent; ``PARTITIONED BY (DATE(occurred_at))`` fails with
``DELTA_OPERATION_NOT_ALLOWED: Partitioning by expressions is not supported``.

Databricks' own idiom for this is a **generated column** —
``event_date DATE GENERATED ALWAYS AS (CAST(occurred_at AS DATE))`` — a physical
column Delta computes and keeps in sync automatically. That feature needs
protocol/table-feature support that isn't reliably available outside a real
Unity Catalog-backed workspace (confirmed by hitting
``UNSUPPORTED_FEATURE.TABLE_OPERATION: ... does not support generated columns``
against OSS ``delta-spark`` locally) — so this bootstrap uses the equally valid,
simpler alternative: a **plain column populated at write time**
(``ingestion.py`` sets ``event_date`` before every write), partitioned on
directly. Both are legitimate; know which one you're looking at on a real
workspace, where generated columns are the more idiomatic choice.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from databricks_lakehouse_recon.config import Settings

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    event_id STRING,
    account_id STRING,
    amount_minor BIGINT,
    signed_amount_minor BIGINT,
    currency STRING,
    direction STRING,
    transaction_type STRING,
    occurred_at TIMESTAMP,
    event_date DATE,
    source_system STRING,
    ingested_at TIMESTAMP,
    batch_id STRING
)
USING DELTA
PARTITIONED BY (event_date)
""".strip()


def bootstrap(spark: SparkSession, settings: Settings) -> None:
    """Idempotent: safe to run every time a job starts."""
    for schema in (settings.bronze_schema, settings.silver_schema, settings.gold_schema):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{schema}")
    spark.sql(BRONZE_DDL.format(table=settings.bronze_transactions))
