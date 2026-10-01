"""Delta Lake table maintenance — deliberately different from the Iceberg
operations in `lakehouse-recon-platform`, because the two formats genuinely
differ here and getting the syntax right under questioning matters more than
having "an answer."

| Operation | Iceberg (Trino) | Delta (Databricks) |
|---|---|---|
| Compaction | ``ALTER TABLE ... EXECUTE optimize(...)`` | ``OPTIMIZE table [ZORDER BY (cols)]`` |
| Orphan/old file cleanup | ``remove_orphan_files`` | ``VACUUM table [RETAIN n HOURS]`` |
| Time travel | ``FOR VERSION/TIMESTAMP AS OF`` | ``VERSION AS OF`` / ``TIMESTAMP AS OF`` (same keywords, no ``FOR``) |
| Rollback | ``CALL system.rollback_to_snapshot(...)`` | ``RESTORE TABLE table TO VERSION AS OF n`` |
| History/audit | ``"table$snapshots"`` metadata table | ``DESCRIBE HISTORY table`` |
| Schema evolution | ``ADD COLUMN`` | ``ADD COLUMN`` + ``mergeSchema`` write option |
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def optimize_sql(table: str, zorder_by: list[str] | None = None) -> str:
    """Compaction. ZORDER co-locates rows by the given columns within each file,
    so a filter on those columns can skip whole files — Delta's answer to the same
    small-files-kill-scan-performance problem Iceberg's ``optimize`` solves."""
    if zorder_by:
        return f"OPTIMIZE {table} ZORDER BY ({', '.join(zorder_by)})"
    return f"OPTIMIZE {table}"


def vacuum_sql(table: str, retain_hours: int = 168) -> str:
    """Deletes files no longer referenced by the Delta log and older than the
    retention window. Too short a window breaks any reader still mid-query
    against an older version — 168h (7 days) is Delta's own default for a reason."""
    if retain_hours < 0:
        raise ValueError("retain_hours must not be negative")
    return f"VACUUM {table} RETAIN {retain_hours} HOURS"


def describe_history_sql(table: str, limit: int | None = None) -> str:
    """Delta's transaction log, human-readable — the audit trail time travel reads from."""
    suffix = f" LIMIT {int(limit)}" if limit else ""
    return f"DESCRIBE HISTORY {table}{suffix}"


def time_travel_sql(table: str, *, version: int | None = None, as_of: str | None = None) -> str:
    if (version is None) == (as_of is None):
        raise ValueError("pass exactly one of version or as_of")
    if version is not None:
        return f"SELECT * FROM {table} VERSION AS OF {int(version)}"
    return f"SELECT * FROM {table} TIMESTAMP AS OF '{as_of}'"


def restore_sql(table: str, version: int) -> str:
    """Rollback as a new, non-destructive transaction — the same operational
    argument for an open table format as Iceberg's rollback: a bad publish is
    one statement from reversed, and the reversal is itself auditable history,
    not a restore-from-backup."""
    return f"RESTORE TABLE {table} TO VERSION AS OF {int(version)}"


def add_column_sql(table: str, column: str, sql_type: str) -> str:
    return f"ALTER TABLE {table} ADD COLUMNS ({column} {sql_type})"


class DeltaMaintenance:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def optimize(self, table: str, zorder_by: list[str] | None = None) -> None:
        self.spark.sql(optimize_sql(table, zorder_by))

    def vacuum(self, table: str, retain_hours: int = 168) -> None:
        self.spark.sql(vacuum_sql(table, retain_hours))

    def history(self, table: str, limit: int | None = None) -> DataFrame:
        return self.spark.sql(describe_history_sql(table, limit))

    def latest_version(self, table: str) -> int:
        row = self.history(table, limit=1).collect()[0]
        return int(row["version"])

    def restore(self, table: str, version: int) -> None:
        self.spark.sql(restore_sql(table, version))

    def add_column(self, table: str, column: str, sql_type: str) -> None:
        self.spark.sql(add_column_sql(table, column, sql_type))
