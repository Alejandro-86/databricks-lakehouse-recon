"""Typed financial events — same two decisions as the Trino/Iceberg version, and for
the same reasons: integer minor units (so reconciliation can run at zero tolerance)
and a stable idempotency key (because any real ingestion path redelivers at least
once, and pretending otherwise is how a dedup bug survives to production).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Direction(StrEnum):
    DEBIT = "debit"
    CREDIT = "credit"


class TransactionType(StrEnum):
    TRANSFER = "transfer"
    CARD_PAYMENT = "card_payment"
    FX_CONVERSION = "fx_conversion"
    FEE = "fee"
    REFUND = "refund"


class TransactionEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=8)
    account_id: str = Field(min_length=1)
    amount_minor: int
    currency: str
    direction: Direction
    transaction_type: TransactionType
    occurred_at: datetime
    source_system: str = Field(min_length=1)

    @field_validator("currency")
    @classmethod
    def _currency_upper(cls, v: str) -> str:
        v = v.strip().upper()
        if len(v) != 3 or not v.isalpha():
            raise ValueError(f"currency must be a 3-letter ISO code, got {v!r}")
        return v

    @field_validator("amount_minor")
    @classmethod
    def _non_zero(cls, v: int) -> int:
        if v == 0:
            raise ValueError("amount_minor must not be zero")
        return v

    @field_validator("occurred_at")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        return v if v.tzinfo else v.replace(tzinfo=UTC)

    @property
    def signed_amount_minor(self) -> int:
        magnitude = abs(self.amount_minor)
        return -magnitude if self.direction is Direction.DEBIT else magnitude

    def amount_major(self) -> Decimal:
        return Decimal(self.signed_amount_minor).scaleb(-2)

    def to_row(self, ingested_at: datetime, batch_id: str) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "account_id": self.account_id,
            "amount_minor": self.amount_minor,
            "signed_amount_minor": self.signed_amount_minor,
            "currency": self.currency,
            "direction": str(self.direction),
            "transaction_type": str(self.transaction_type),
            "occurred_at": self.occurred_at,
            "event_date": self.occurred_at.date(),
            "source_system": self.source_system,
            "ingested_at": ingested_at,
            "batch_id": batch_id,
        }


BRONZE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "account_id",
    "amount_minor",
    "signed_amount_minor",
    "currency",
    "direction",
    "transaction_type",
    "occurred_at",
    "event_date",
    "source_system",
    "ingested_at",
    "batch_id",
)
