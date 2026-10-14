"""Idempotent DB writes."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Engine

from txnpipe.common.db import alerts, transactions
from txnpipe.common.schemas import TransactionEvent


@dataclass(frozen=True)
class AlertRow:
    transaction_id: str
    account_id: str
    rule: str
    severity: str
    details: dict[str, Any]


class Repository(Protocol):
    def begin(self) -> Any:
        """Context manager that yields a connection and commits on clean exit."""
        ...

    def insert_transactions(
        self, conn: Any, items: Sequence[tuple[TransactionEvent, int, int]]
    ) -> set[str]: ...

    def insert_alerts(self, conn: Any, rows: Sequence[AlertRow]) -> None: ...

    def mark_flagged(self, conn: Any, transaction_ids: Sequence[str]) -> None: ...


class PgRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        with self.engine.begin() as conn:
            yield conn

    def insert_transactions(
        self, conn: Connection, items: Sequence[tuple[TransactionEvent, int, int]]
    ) -> set[str]:
        """Insert rows; returns the ids that were new. Existing ids are silently skipped."""
        if not items:
            return set()
        rows = [
            {
                "transaction_id": str(e.transaction_id),
                "account_id": e.account_id,
                "merchant_id": e.merchant_id,
                "amount": e.amount,
                "currency": e.currency,
                "country": e.country,
                "channel": e.channel.value,
                "event_time": e.timestamp,
                "kafka_partition": partition,
                "kafka_offset": offset,
            }
            for e, partition, offset in items
        ]
        stmt = (
            insert(transactions)
            .values(rows)
            .on_conflict_do_nothing(index_elements=["transaction_id"])
            .returning(transactions.c.transaction_id)
        )
        return {str(r[0]) for r in conn.execute(stmt)}

    def insert_alerts(self, conn: Connection, rows: Sequence[AlertRow]) -> None:
        if not rows:
            return
        stmt = (
            insert(alerts)
            .values(
                [
                    {
                        "transaction_id": r.transaction_id,
                        "account_id": r.account_id,
                        "rule": r.rule,
                        "severity": r.severity,
                        "details": r.details,
                    }
                    for r in rows
                ]
            )
            .on_conflict_do_nothing(index_elements=["transaction_id", "rule"])
        )
        conn.execute(stmt)

    def mark_flagged(self, conn: Connection, transaction_ids: Sequence[str]) -> None:
        if not transaction_ids:
            return
        conn.execute(
            update(transactions)
            .where(transactions.c.transaction_id.in_(list(transaction_ids)))
            .values(is_flagged=True)
        )
