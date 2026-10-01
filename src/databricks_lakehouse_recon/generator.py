"""Seeded synthetic event stream with injectable duplicates and late arrivals.

Same reasoning as the Trino/Iceberg version: a reconciliation platform demoed on
clean data proves nothing, and a seeded generator keeps a failing demo reproducible.
"""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from databricks_lakehouse_recon.events import Direction, TransactionEvent, TransactionType

CURRENCIES = ("GBP", "EUR", "USD")


@dataclass
class GenerationManifest:
    unique_events: int = 0
    duplicates: int = 0
    row_count_by_date: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    amount_by_date: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record(self, event: TransactionEvent, *, is_duplicate: bool) -> None:
        if is_duplicate:
            self.duplicates += 1
            return
        key = event.occurred_at.date().isoformat()
        self.unique_events += 1
        self.row_count_by_date[key] += 1
        self.amount_by_date[key] += event.signed_amount_minor


class TransactionGenerator:
    def __init__(
        self,
        *,
        seed: int = 42,
        accounts: int = 25,
        days: int = 5,
        start: datetime | None = None,
        duplicate_rate: float = 0.03,
    ) -> None:
        if not 0.0 <= duplicate_rate < 1.0:
            raise ValueError("duplicate_rate must be in [0, 1)")
        if accounts < 1 or days < 1:
            raise ValueError("accounts and days must be positive")
        self.rng = random.Random(seed)
        self.accounts = [f"ACC-{i:05d}" for i in range(accounts)]
        self.days = days
        self.start = start or datetime(2026, 9, 1, tzinfo=UTC)
        self.duplicate_rate = duplicate_rate
        self.manifest = GenerationManifest()

    def _one(self, index: int) -> TransactionEvent:
        occurred = self.start + timedelta(
            days=self.rng.randrange(self.days), seconds=self.rng.randrange(86_400)
        )
        return TransactionEvent(
            event_id=f"EVT-{index:08d}",
            account_id=self.rng.choice(self.accounts),
            amount_minor=self.rng.randrange(50, 5_000_00),
            currency=self.rng.choice(CURRENCIES),
            direction=self.rng.choice(list(Direction)),
            transaction_type=self.rng.choice(list(TransactionType)),
            occurred_at=occurred,
            source_system="core-ledger",
        )

    def stream(self, count: int) -> Iterator[TransactionEvent]:
        if count < 1:
            raise ValueError("count must be positive")
        emitted: list[TransactionEvent] = []
        for index in range(count):
            event = self._one(index)
            emitted.append(event)
            self.manifest.record(event, is_duplicate=False)
            yield event
            if emitted and self.rng.random() < self.duplicate_rate:
                repeat = self.rng.choice(emitted)
                self.manifest.record(repeat, is_duplicate=True)
                yield repeat

    def generate(self, count: int) -> list[TransactionEvent]:
        return list(self.stream(count))
