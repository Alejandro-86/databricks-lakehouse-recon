"""Builds a local, Delta-enabled SparkSession for tests and local development.

On a real Databricks workspace none of this is needed — ``spark`` is already
injected into the notebook/job context with Delta and Unity Catalog configured,
and table metadata is persisted centrally by the workspace's own metastore.

Locally there is no such service, and **Spark's default session catalog is
purely in-memory** — a table created by one process launch is invisible to the
next, even pointed at the same warehouse directory on disk (confirmed by
running exactly that: ``CREATE TABLE`` in one Python process, then
``spark.read.table(...)`` in a fresh one, which raised
``TABLE_OR_VIEW_NOT_FOUND`` despite the Delta files sitting there on disk).
``enableHiveSupport()`` gives the local session a persistent (Derby-backed)
metastore under ``./metastore_db``, so running ``dbx-lakehouse demo`` and then
``dbx-lakehouse maintain`` as two separate shell commands — the normal way to
use this CLI — actually works.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

_DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.3.1"


def get_local_spark(app_name: str = "databricks-lakehouse-recon", warehouse_path: str = "./warehouse") -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", _DELTA_PACKAGE)
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("spark.sql.shuffle.partitions", "4")  # small local runs don't need Spark's default 200
        .enableHiveSupport()
        .getOrCreate()
    )


def replace_table(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Write ``df`` as ``table``, replacing anything already there.

    Explicit drop-then-create rather than ``.mode("overwrite").saveAsTable()``:
    the overwrite path goes through Spark's ``OverwriteByExpression`` V2
    command, which requires the table to implement ``SupportsTruncate`` — a
    capability this combination of Spark 3.5 and OSS ``delta-spark`` (outside a
    real Unity Catalog-backed workspace) does not reliably expose, failing with
    ``does not support truncate in batch mode``. A real Databricks workspace
    likely doesn't need this workaround; it's here because the failure was
    hit and reproduced, not assumed.
    """
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    df.write.format("delta").saveAsTable(table)
