from __future__ import annotations

from datetime import UTC, datetime

import pytest

from databricks_lakehouse_recon.config import Settings
from databricks_lakehouse_recon.events import Direction, TransactionEvent, TransactionType
from databricks_lakehouse_recon.reconciliation import ControlTotal, Layer


@pytest.fixture(scope="session")
def spark():
    import shutil

    from databricks_lakehouse_recon.spark_session import get_local_spark

    # Start clean: a leftover warehouse dir from a prior interrupted run collides
    # with pytest's truncated tmp_path names, which are deterministic per test —
    # a stale, non-empty table directory is not the same thing as a stale table.
    shutil.rmtree("./test-warehouse", ignore_errors=True)
    session = get_local_spark(app_name="pytest", warehouse_path="./test-warehouse")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Same ``catalog.schema.table`` addressing code path as production, pointed
    at the one catalog that genuinely exists outside a real Unity Catalog
    metastore: ``spark_catalog``, Spark's own default/session catalog.

    OSS ``delta-spark``'s ``DeltaCatalog`` is a ``DelegatingCatalogExtension`` —
    designed specifically to *replace* ``spark_catalog``, not to be registered
    as an arbitrary second named catalog. Real Unity Catalog multi-catalog
    behaviour needs a real workspace (or a UC-enabled server), neither of which
    is available to a local pytest run. Test isolation therefore comes from a
    unique **schema** name per test, not a unique catalog name — ``Settings``
    already supports that independently of ``catalog``.
    """
    suffix = tmp_path.name.replace("-", "_")
    return Settings(
        catalog="spark_catalog",
        bronze_schema=f"bronze_{suffix}",
        silver_schema=f"silver_{suffix}",
        gold_schema=f"gold_{suffix}",
    )


@pytest.fixture
def event() -> TransactionEvent:
    return TransactionEvent(
        event_id="EVT-00000001",
        account_id="ACC-00001",
        amount_minor=12_345,
        currency="gbp",
        direction=Direction.CREDIT,
        transaction_type=TransactionType.TRANSFER,
        occurred_at=datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
        source_system="core-ledger",
    )


def total(layer: Layer, key: str, rows: int, amount: int, distinct: int | None = None) -> ControlTotal:
    return ControlTotal(layer=layer, partition_key=key, row_count=rows, amount_minor_sum=amount,
                         distinct_event_ids=rows if distinct is None else distinct)
