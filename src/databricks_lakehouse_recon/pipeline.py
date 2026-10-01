"""Which layers get compared, with which checks — orchestration only.

Identical shape to `lakehouse-recon-platform`'s pipeline.py: bronze is compared
deduplicated (so expected at-least-once duplicates don't masquerade as breaks),
then bronze->silver, silver->gold, a standalone dedup check, and fact->dimension
referential integrity.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from databricks_lakehouse_recon.config import Settings
from databricks_lakehouse_recon.reconciliation import (
    AmountSumCheck,
    DeduplicationCheck,
    Layer,
    ReconciliationReport,
    ReferentialIntegrityCheck,
    RowCountCheck,
    reconcile,
)
from databricks_lakehouse_recon.spark_control_totals import SparkControlTotals

DATE_COL = "business_date"


def run_reconciliation(spark: SparkSession, settings: Settings) -> ReconciliationReport:
    totals = SparkControlTotals(spark)
    amount_check = AmountSumCheck(
        tolerance_minor=settings.amount_tolerance_minor, relative_tolerance=settings.relative_tolerance
    )

    bronze_deduped = totals.fetch(settings.bronze_transactions, Layer.BRONZE, partition_col=DATE_COL, dedupe=True)
    silver = totals.fetch(settings.silver_transactions, Layer.SILVER, partition_col=DATE_COL)
    gold = totals.fetch(settings.gold_fact, Layer.GOLD, partition_col=DATE_COL)

    report = ReconciliationReport()
    report.results.extend(reconcile([RowCountCheck(), amount_check], bronze_deduped, silver).results)
    report.results.extend(reconcile([RowCountCheck(), amount_check], silver, gold).results)
    report.results.extend(reconcile([DeduplicationCheck()], silver, silver).results)

    fact_accounts = [
        r["account_id"]
        for r in spark.read.table(settings.gold_fact).select("account_id").distinct().collect()
    ]
    dim_accounts = [
        r["account_id"]
        for r in spark.read.table(settings.gold_dim_account).select("account_id").distinct().collect()
    ]
    referential = reconcile(
        [ReferentialIntegrityCheck(dimension="dim_account", fact_keys=fact_accounts, dimension_keys=dim_accounts)]
    )
    report.results.extend(referential.results)
    return report
