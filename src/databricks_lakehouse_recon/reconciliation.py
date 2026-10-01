"""The reconciliation engine — unchanged in design from `lakehouse-recon-platform`.

This module imports nothing from PySpark or Delta. That is the point: the engine
that decides whether a migration can be trusted should not care which warehouse
produced the numbers. Porting this platform from Trino/Iceberg to Databricks/Delta
meant writing a new adapter (`spark_control_totals.py`) and changing nothing here —
which is exactly the claim worth making about good separation of concerns, not just
asserting it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum


class Layer(StrEnum):
    SOURCE = "source"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


class ReconciliationFailure(RuntimeError):
    def __init__(self, report: ReconciliationReport) -> None:
        self.report = report
        super().__init__(report.summary())


@dataclass(frozen=True, slots=True)
class ControlTotal:
    layer: Layer
    partition_key: str
    row_count: int
    amount_minor_sum: int
    distinct_event_ids: int

    def __post_init__(self) -> None:
        if self.row_count < 0 or self.distinct_event_ids < 0:
            raise ValueError("counts must not be negative")
        if self.distinct_event_ids > self.row_count:
            raise ValueError(
                f"distinct_event_ids ({self.distinct_event_ids}) cannot exceed "
                f"row_count ({self.row_count}) for partition {self.partition_key!r}"
            )


@dataclass(frozen=True, slots=True)
class Break:
    check: str
    partition_key: str
    source_layer: Layer
    target_layer: Layer
    expected: int
    actual: int
    severity: Severity = Severity.ERROR
    note: str = ""

    @property
    def delta(self) -> int:
        return self.actual - self.expected

    def describe(self) -> str:
        detail = f" ({self.note})" if self.note else ""
        return (
            f"[{self.severity.value.upper()}] {self.check} @ {self.partition_key}: "
            f"{self.source_layer.value}={self.expected} vs "
            f"{self.target_layer.value}={self.actual} (delta {self.delta:+d}){detail}"
        )


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    breaks: tuple[Break, ...]
    partitions_compared: int

    @property
    def passed(self) -> bool:
        return not any(b.severity is Severity.ERROR for b in self.breaks)


def _index(totals: Sequence[ControlTotal]) -> dict[str, ControlTotal]:
    indexed: dict[str, ControlTotal] = {}
    for total in totals:
        if total.partition_key in indexed:
            raise ValueError(f"duplicate control total for partition {total.partition_key!r}")
        indexed[total.partition_key] = total
    return indexed


def _layer_of(totals: Sequence[ControlTotal], default: Layer) -> Layer:
    return totals[0].layer if totals else default


def _aligned_keys(source: dict[str, ControlTotal], target: dict[str, ControlTotal]) -> list[str]:
    """Union, not intersection — a partition missing on one side is the case this exists to catch."""
    return sorted(set(source) | set(target))


class ReconciliationCheck:
    name: str = "check"

    def run(self, source: Sequence[ControlTotal], target: Sequence[ControlTotal]) -> CheckResult:
        raise NotImplementedError


class RowCountCheck(ReconciliationCheck):
    name = "row_count"

    def __init__(self, *, expect_distinct_source: bool = False, expect_distinct_target: bool = False) -> None:
        self.expect_distinct_source = expect_distinct_source
        self.expect_distinct_target = expect_distinct_target

    def run(self, source: Sequence[ControlTotal], target: Sequence[ControlTotal]) -> CheckResult:
        src, tgt = _index(source), _index(target)
        source_layer, target_layer = _layer_of(source, Layer.BRONZE), _layer_of(target, Layer.SILVER)
        breaks: list[Break] = []
        keys = _aligned_keys(src, tgt)
        for key in keys:
            s, t = src.get(key), tgt.get(key)
            expected = 0
            if s is not None:
                expected = s.distinct_event_ids if self.expect_distinct_source else s.row_count
            actual = 0
            if t is not None:
                actual = t.distinct_event_ids if self.expect_distinct_target else t.row_count
            if expected != actual:
                note = "partition missing from target layer" if t is None else (
                    "partition present in target but absent from source" if s is None else ""
                )
                breaks.append(Break(self.name, key, source_layer, target_layer, expected, actual, note=note))
        return CheckResult(self.name, tuple(breaks), len(keys))


class AmountSumCheck(ReconciliationCheck):
    name = "amount_sum"

    def __init__(self, *, tolerance_minor: int = 0, relative_tolerance: float = 0.0) -> None:
        if tolerance_minor < 0:
            raise ValueError("tolerance_minor must not be negative")
        if not 0.0 <= relative_tolerance <= 0.01:
            raise ValueError("relative_tolerance must be between 0 and 0.01")
        self.tolerance_minor = tolerance_minor
        self.relative_tolerance = relative_tolerance

    def allowed_drift(self, expected: int) -> int:
        return max(self.tolerance_minor, int(abs(expected) * self.relative_tolerance))

    def run(self, source: Sequence[ControlTotal], target: Sequence[ControlTotal]) -> CheckResult:
        src, tgt = _index(source), _index(target)
        source_layer, target_layer = _layer_of(source, Layer.BRONZE), _layer_of(target, Layer.SILVER)
        breaks: list[Break] = []
        keys = _aligned_keys(src, tgt)
        for key in keys:
            s, t = src.get(key), tgt.get(key)
            expected = s.amount_minor_sum if s is not None else 0
            actual = t.amount_minor_sum if t is not None else 0
            drift = abs(actual - expected)
            allowance = self.allowed_drift(expected)
            if drift > allowance:
                breaks.append(
                    Break(
                        self.name, key, source_layer, target_layer, expected, actual,
                        note=f"drift {drift} minor units exceeds allowance {allowance}",
                    )
                )
        return CheckResult(self.name, tuple(breaks), len(keys))


class DeduplicationCheck(ReconciliationCheck):
    name = "deduplication"

    def run(self, source: Sequence[ControlTotal], target: Sequence[ControlTotal]) -> CheckResult:
        subjects = target or source
        breaks = [
            Break(self.name, t.partition_key, t.layer, t.layer, t.distinct_event_ids, t.row_count,
                  note="duplicate event ids survived into this layer")
            for t in subjects
            if t.row_count != t.distinct_event_ids
        ]
        return CheckResult(self.name, tuple(breaks), len(subjects))


class ReferentialIntegrityCheck(ReconciliationCheck):
    name = "referential_integrity"

    def __init__(self, *, dimension: str, fact_keys: Sequence[str], dimension_keys: Sequence[str],
                 severity: Severity = Severity.ERROR) -> None:
        self.dimension = dimension
        self.fact_keys = set(fact_keys)
        self.dimension_keys = set(dimension_keys)
        self.severity = severity

    def run(self, source: Sequence[ControlTotal] = (), target: Sequence[ControlTotal] = ()) -> CheckResult:
        orphans = sorted(self.fact_keys - self.dimension_keys)
        breaks = [
            Break(self.name, f"{self.dimension}:{key}", Layer.GOLD, Layer.GOLD, 1, 0,
                  severity=self.severity, note=f"fact references {self.dimension} key with no dimension row")
            for key in orphans
        ]
        return CheckResult(self.name, tuple(breaks), len(self.fact_keys))


@dataclass
class ReconciliationReport:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def breaks(self) -> tuple[Break, ...]:
        return tuple(b for r in self.results for b in r.breaks)

    @property
    def errors(self) -> tuple[Break, ...]:
        return tuple(b for b in self.breaks if b.severity is Severity.ERROR)

    @property
    def warnings(self) -> tuple[Break, ...]:
        return tuple(b for b in self.breaks if b.severity is Severity.WARNING)

    @property
    def passed(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        if self.passed and not self.warnings:
            checks = ", ".join(r.check for r in self.results) or "none"
            return f"reconciliation PASSED ({len(self.results)} checks: {checks})"
        status = "PASSED with warnings" if self.passed else "FAILED"
        head = f"reconciliation {status}: {len(self.errors)} error(s), {len(self.warnings)} warning(s)"
        return "\n".join([head, *(f"  {b.describe()}" for b in self.breaks)])

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "checks": [{"check": r.check, "passed": r.passed, "partitions_compared": r.partitions_compared,
                        "breaks": len(r.breaks)} for r in self.results],
            "breaks": [{"check": b.check, "partition_key": b.partition_key, "source_layer": str(b.source_layer),
                        "target_layer": str(b.target_layer), "expected": b.expected, "actual": b.actual,
                        "delta": b.delta, "severity": str(b.severity), "note": b.note} for b in self.breaks],
        }

    def raise_for_status(self) -> None:
        if not self.passed:
            raise ReconciliationFailure(self)


def reconcile(checks: Sequence[ReconciliationCheck], source: Sequence[ControlTotal] = (),
              target: Sequence[ControlTotal] = ()) -> ReconciliationReport:
    return ReconciliationReport(results=[check.run(source, target) for check in checks])
