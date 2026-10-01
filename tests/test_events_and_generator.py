from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from databricks_lakehouse_recon.events import BRONZE_COLUMNS, Direction, TransactionEvent
from databricks_lakehouse_recon.generator import TransactionGenerator


def test_currency_normalised_to_upper(event):
    assert event.currency == "GBP"


def test_bad_currency_rejected(event):
    with pytest.raises(ValidationError, match="3-letter ISO code"):
        TransactionEvent.model_validate({**event.model_dump(), "currency": "POUNDS"})


def test_zero_amount_rejected(event):
    with pytest.raises(ValidationError, match="must not be zero"):
        TransactionEvent.model_validate({**event.model_dump(), "amount_minor": 0})


def test_debit_signed_negative(event):
    debit = TransactionEvent.model_validate({**event.model_dump(), "direction": Direction.DEBIT, "amount_minor": 500})
    assert debit.signed_amount_minor == -500


def test_row_matches_declared_columns(event):
    row = event.to_row(datetime.now(UTC), "batch-1")
    assert tuple(row) == BRONZE_COLUMNS


def test_same_seed_same_stream():
    a = TransactionGenerator(seed=7).generate(50)
    b = TransactionGenerator(seed=7).generate(50)
    assert [e.event_id for e in a] == [e.event_id for e in b]


def test_duplicates_emitted_but_not_double_counted():
    gen = TransactionGenerator(seed=3, duplicate_rate=0.5)
    events = gen.generate(200)
    assert gen.manifest.duplicates > 0
    assert gen.manifest.unique_events == len({e.event_id for e in events})


def test_invalid_duplicate_rate_rejected():
    with pytest.raises(ValueError):
        TransactionGenerator(duplicate_rate=1.0)
