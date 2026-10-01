from __future__ import annotations

from fastapi.testclient import TestClient

from databricks_lakehouse_recon.serving import build_app

ROWS = [
    {"account_id": "ACC-00001", "transaction_count": 12, "net_amount_minor": 50_000},
    {"account_id": "ACC-00002", "transaction_count": 3, "net_amount_minor": -1_200},
]


def test_list_accounts_returns_every_row():
    client = TestClient(build_app(ROWS))
    resp = client.get("/accounts")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


def test_get_account_returns_the_matching_row():
    client = TestClient(build_app(ROWS))
    resp = client.get("/accounts/ACC-00001")
    assert resp.status_code == 200
    assert resp.json()["net_amount_minor"] == 50_000


def test_unknown_account_is_404():
    client = TestClient(build_app(ROWS))
    resp = client.get("/accounts/NOPE")
    assert resp.status_code == 404
