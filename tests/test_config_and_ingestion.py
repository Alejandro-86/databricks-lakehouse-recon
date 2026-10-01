from __future__ import annotations

from databricks_lakehouse_recon.config import Settings
from databricks_lakehouse_recon.ingestion import parse_json_line


def test_unity_catalog_three_level_naming():
    s = Settings(catalog="lakehouse_recon")
    assert s.bronze_transactions == "lakehouse_recon.bronze.transactions_raw"
    assert s.silver_transactions == "lakehouse_recon.silver.silver_transactions"
    assert s.gold_fact == "lakehouse_recon.gold.fct_transaction"


def test_parse_json_line_rejects_malformed_payload():
    assert parse_json_line("{not json") is None


def test_parse_json_line_accepts_a_valid_event():
    payload = (
        '{"event_id": "EVT-00000099", "account_id": "A1", "amount_minor": 100, '
        '"currency": "GBP", "direction": "credit", "transaction_type": "fee", '
        '"occurred_at": "2026-09-01T00:00:00Z", "source_system": "s"}'
    )
    event = parse_json_line(payload)
    assert event is not None
    assert event.event_id == "EVT-00000099"
