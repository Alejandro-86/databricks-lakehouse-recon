"""Thin serving API over gold — the Topic 3 architecture, as real code.

A Delta table is the governed system of record, but it's batch/micro-batch, not
built for an app's low-latency point-lookup reads. The honest answer isn't
"query Delta from the app directly" — it's a serving layer in front of it. This
is that layer: FastAPI + Pydantic, the same stack as every other project in this
search, reading gold once at startup (small-data demo) rather than hitting Spark
per request, which would reintroduce exactly the latency problem the layer
exists to avoid. At real scale this cache would be a sync job into Postgres/Redis
— same shape as `growth-data-platform`'s reverse-ETL pattern — not an in-process
dict; that's the honest boundary of what a demo this size should claim.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class AccountSummary(BaseModel):
    account_id: str
    transaction_count: int
    net_amount_minor: int


def build_app(gold_rows: list[dict[str, Any]]) -> FastAPI:
    index = {row["account_id"]: row for row in gold_rows}
    app = FastAPI(title="lakehouse-recon serving layer")

    @app.get("/accounts/{account_id}", response_model=AccountSummary)
    def get_account(account_id: str) -> AccountSummary:
        row = index.get(account_id)
        if row is None:
            raise HTTPException(status_code=404, detail="account not found")
        return AccountSummary(
            account_id=account_id,
            transaction_count=int(row["transaction_count"]),
            net_amount_minor=int(row["net_amount_minor"]),
        )

    @app.get("/accounts", response_model=list[AccountSummary])
    def list_accounts() -> list[AccountSummary]:
        return [
            AccountSummary(
                account_id=str(r["account_id"]),
                transaction_count=int(r["transaction_count"]),
                net_amount_minor=int(r["net_amount_minor"]),
            )
            for r in gold_rows
        ]

    return app
