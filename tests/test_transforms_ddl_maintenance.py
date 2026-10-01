from __future__ import annotations

from datetime import UTC

from databricks_lakehouse_recon.ddl import bootstrap
from databricks_lakehouse_recon.generator import TransactionGenerator
from databricks_lakehouse_recon.ingestion import ingest_batch
from databricks_lakehouse_recon.maintenance import DeltaMaintenance
from databricks_lakehouse_recon.pipeline import run_reconciliation
from databricks_lakehouse_recon.spark_session import replace_table
from databricks_lakehouse_recon.transforms import build_gold, build_silver


def _seed_medallion(spark, settings, *, count=300, inject_break=False):
    bootstrap(spark, settings)
    events = TransactionGenerator(seed=11, days=3, duplicate_rate=0.0).generate(count)
    ingest_batch(spark, settings, events)

    silver, rejected = build_silver(spark, settings)
    replace_table(spark, silver, settings.silver_transactions)
    replace_table(spark, rejected, settings.silver_transactions + "_rejected")

    gold = build_gold(spark, settings, inject_break=inject_break)
    replace_table(spark, gold["fct_transaction"], settings.gold_fact)
    replace_table(spark, gold["dim_account"], settings.gold_dim_account)


def test_bootstrap_creates_schemas_and_bronze_table(spark, settings):
    bootstrap(spark, settings)
    schemas = [r["namespace"] for r in spark.sql(f"SHOW SCHEMAS IN {settings.catalog}").collect()]
    assert settings.bronze_schema in schemas
    assert settings.silver_schema in schemas
    assert settings.gold_schema in schemas


def test_clean_medallion_reconciles(spark, settings):
    _seed_medallion(spark, settings)
    report = run_reconciliation(spark, settings)
    assert report.passed, report.summary()


def test_injected_break_is_caught(spark, settings):
    _seed_medallion(spark, settings, inject_break=True)
    report = run_reconciliation(spark, settings)
    assert not report.passed
    assert any(b.check == "amount_sum" for b in report.breaks)


def test_rejected_rows_are_quarantined_not_dropped(spark, settings):
    bootstrap(spark, settings)
    from datetime import datetime

    from databricks_lakehouse_recon.events import Direction, TransactionEvent, TransactionType

    good = TransactionEvent(event_id="EVT-00000001", account_id="A1", amount_minor=100, currency="GBP",
                             direction=Direction.CREDIT, transaction_type=TransactionType.FEE,
                             occurred_at=datetime(2026, 9, 1, tzinfo=UTC), source_system="s")
    ingest_batch(spark, settings, [good])
    # A null event_id can't be constructed via the Pydantic model (by design), so
    # insert the bad row directly to simulate a source that bypassed validation.
    # Column order matches BRONZE_DDL: ... occurred_at, event_date, source_system, ...
    spark.sql(
        f"INSERT INTO {settings.bronze_transactions} VALUES "
        f"(NULL, 'A2', 50, 50, 'GBP', 'credit', 'fee', timestamp'2026-09-01', date'2026-09-01', 's', "
        f"timestamp'2026-09-01', 'b1')"
    )
    silver, rejected = build_silver(spark, settings)
    assert silver.count() == 1
    assert rejected.count() == 1
    assert rejected.collect()[0]["rejection_reason"] == "missing_event_id"


def test_optimize_and_history_round_trip(spark, settings):
    _seed_medallion(spark, settings, count=50)
    ops = DeltaMaintenance(spark)
    ops.optimize(settings.bronze_transactions)
    history_rows = ops.history(settings.bronze_transactions).collect()
    assert len(history_rows) >= 2  # at least: CREATE TABLE/WRITE, then OPTIMIZE
    assert any(row["operation"] == "OPTIMIZE" for row in history_rows)


def test_restore_rolls_back_to_an_earlier_version(spark, settings):
    bootstrap(spark, settings)
    # duplicate_rate=0.0: this test asserts exact row counts, which the default
    # fault-injecting generator deliberately won't guarantee.
    events_v1 = TransactionGenerator(seed=1, duplicate_rate=0.0).generate(10)
    ingest_batch(spark, settings, events_v1, batch_id="v1")
    ops = DeltaMaintenance(spark)
    v1 = ops.latest_version(settings.bronze_transactions)

    events_v2 = TransactionGenerator(seed=2, duplicate_rate=0.0).generate(10)
    ingest_batch(spark, settings, events_v2, batch_id="v2")
    assert spark.read.table(settings.bronze_transactions).count() == 20

    ops.restore(settings.bronze_transactions, v1)
    assert spark.read.table(settings.bronze_transactions).count() == 10


def test_add_column_schema_evolution(spark, settings):
    bootstrap(spark, settings)
    ops = DeltaMaintenance(spark)
    ops.add_column(settings.bronze_transactions, "settlement_ref", "STRING")
    columns = spark.read.table(settings.bronze_transactions).columns
    assert "settlement_ref" in columns
