"""Landing events in bronze — two paths, one Databricks-only, one laptop-testable.

:func:`ingest_batch` takes a list of events directly and writes them with plain
Spark — this is what the test suite and local demo use, and needs no workspace.

:func:`autoload_stream` is the real Databricks-native path: **Auto Loader**
(``cloudFiles``), which incrementally discovers new files landing in a Unity
Catalog volume and streams them into Delta with exactly-once processing via its
own checkpoint, instead of a cron job re-scanning everything each run. It only
runs on an actual Databricks cluster (``cloudFiles`` is a Databricks-only source),
so it's written for correctness and documented here, not exercised in CI.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_lakehouse_recon.config import Settings
from databricks_lakehouse_recon.events import TransactionEvent

EVENT_JSON_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("account_id", StringType()),
        StructField("amount_minor", LongType()),
        StructField("currency", StringType()),
        StructField("direction", StringType()),
        StructField("transaction_type", StringType()),
        StructField("occurred_at", TimestampType()),
        StructField("source_system", StringType()),
    ]
)


def ingest_batch(
    spark: SparkSession, settings: Settings, events: Iterable[TransactionEvent], batch_id: str | None = None
) -> int:
    """Batch-write events to bronze. Used by tests and the local CLI demo."""
    stamp = datetime.now(UTC)
    this_batch = batch_id or f"batch-{stamp.strftime('%Y%m%d%H%M%S')}"
    rows = [event.to_row(stamp, this_batch) for event in events]
    if not rows:
        return 0
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").saveAsTable(settings.bronze_transactions)
    return len(rows)


def autoload_stream(spark: SparkSession, settings: Settings) -> StreamingQuery:  # pragma: no cover — Databricks-only
    """Incremental file ingestion via Auto Loader. Requires a real Databricks cluster.

    Each new JSON file landing in the Unity Catalog volume is picked up exactly
    once, tracked via the checkpoint — the Databricks-native replacement for a
    cron job that has to re-list and re-diff a whole directory every run.
    """
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(EVENT_JSON_SCHEMA)
        .load(settings.autoloader_source_path)
    )
    enriched = raw.withColumn("ingested_at", F.current_timestamp()).withColumn(
        "batch_id", F.lit("autoloader")
    )
    return (
        enriched.writeStream.format("delta")
        .option("checkpointLocation", settings.autoloader_checkpoint_path)
        .outputMode("append")
        .toTable(settings.bronze_transactions)
    )


def parse_json_line(raw: str) -> TransactionEvent | None:
    """Mirrors the dead-letter instinct from the Trino version: a message that
    can't parse is data to inspect, not an exception to crash on."""
    try:
        payload = json.loads(raw)
        return TransactionEvent.model_validate(payload)
    except Exception:
        return None
