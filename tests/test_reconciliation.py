from __future__ import annotations

import pytest

from databricks_lakehouse_recon.reconciliation import (
    AmountSumCheck,
    ControlTotal,
    DeduplicationCheck,
    Layer,
    ReconciliationFailure,
    ReferentialIntegrityCheck,
    RowCountCheck,
    reconcile,
)

from .conftest import total


def test_distinct_cannot_exceed_rows():
    with pytest.raises(ValueError, match="cannot exceed"):
        ControlTotal(Layer.BRONZE, "2026-09-01", row_count=5, amount_minor_sum=0, distinct_event_ids=6)


def test_matching_counts_pass():
    src = [total(Layer.BRONZE, "2026-09-01", 100, 5000)]
    tgt = [total(Layer.SILVER, "2026-09-01", 100, 5000)]
    assert RowCountCheck().run(src, tgt).passed


def test_missing_partition_is_caught():
    src = [total(Layer.BRONZE, "2026-09-01", 10, 1), total(Layer.BRONZE, "2026-09-02", 10, 1)]
    tgt = [total(Layer.SILVER, "2026-09-01", 10, 1)]
    result = RowCountCheck().run(src, tgt)
    assert not result.passed
    assert result.breaks[0].partition_key == "2026-09-02"


def test_distinct_target_mode_tolerates_bronze_duplicates():
    source = [total(Layer.SOURCE, "2026-09-01", 100, 5000)]
    bronze = [total(Layer.BRONZE, "2026-09-01", rows=112, amount=5600, distinct=100)]
    assert RowCountCheck(expect_distinct_target=True).run(source, bronze).passed


def test_amount_tolerance_scales_with_relative():
    check = AmountSumCheck(relative_tolerance=0.001)
    src = [total(Layer.SILVER, "2026-09-01", 10, 1_000_000)]
    tgt = [total(Layer.GOLD, "2026-09-01", 10, 1_000_500)]
    assert check.run(src, tgt).passed


def test_offsetting_errors_dont_cancel_across_partitions():
    src = [total(Layer.SILVER, "2026-09-01", 5, 1000), total(Layer.SILVER, "2026-09-02", 5, 1000)]
    tgt = [total(Layer.GOLD, "2026-09-01", 5, 1500), total(Layer.GOLD, "2026-09-02", 5, 500)]
    assert len(AmountSumCheck().run(src, tgt).breaks) == 2


def test_deduplication_check_catches_survivors():
    silver = [total(Layer.SILVER, "2026-09-01", rows=105, amount=1000, distinct=100)]
    result = DeduplicationCheck().run(silver, silver)
    assert not result.passed
    assert result.breaks[0].delta == 5


def test_referential_integrity_reports_orphans_individually():
    check = ReferentialIntegrityCheck(dimension="dim_account", fact_keys=["A", "B", "C"], dimension_keys=["A"])
    result = check.run()
    assert {b.partition_key for b in result.breaks} == {"dim_account:B", "dim_account:C"}


def test_report_raises_on_error():
    report = reconcile([RowCountCheck()], [total(Layer.BRONZE, "d", 10, 1)], [total(Layer.SILVER, "d", 9, 1)])
    with pytest.raises(ReconciliationFailure):
        report.raise_for_status()
