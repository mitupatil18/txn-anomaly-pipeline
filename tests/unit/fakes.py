from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import redis.exceptions as rexc

from txnpipe.common.schemas import TransactionEvent
from txnpipe.consumer.records import Record
from txnpipe.consumer.repository import AlertRow
from txnpipe.consumer.rules import AccountState


class FakeRepo:
    """In-memory stand-in with commit/rollback semantics and failure injection."""

    def __init__(self) -> None:
        self.txns: dict[str, tuple[int, int]] = {}
        self.alerts: dict[tuple[str, str], AlertRow] = {}
        self.flagged: set[str] = set()
        self.commits = 0
        self.fail_times = 0
        self.fail_with: Exception = rexc.ConnectionError("db down")
        self.poison_id: str | None = None
        self.attempts = 0

    @contextmanager
    def begin(self) -> Iterator[dict[str, Any]]:
        self.attempts += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise self.fail_with
        stage: dict[str, Any] = {"txns": {}, "alerts": {}, "flagged": set()}
        yield stage
        self.txns.update(stage["txns"])
        self.alerts.update(stage["alerts"])
        self.flagged |= stage["flagged"]
        self.commits += 1

    def insert_transactions(
        self, conn: dict[str, Any], items: Sequence[tuple[TransactionEvent, int, int]]
    ) -> set[str]:
        new: set[str] = set()
        for event, partition, offset in items:
            tid = str(event.transaction_id)
            if tid == self.poison_id:
                raise ValueError("poison")
            if tid not in self.txns and tid not in conn["txns"]:
                conn["txns"][tid] = (partition, offset)
                new.add(tid)
        return new

    def insert_alerts(self, conn: dict[str, Any], rows: Sequence[AlertRow]) -> None:
        for r in rows:
            conn["alerts"].setdefault((r.transaction_id, r.rule), r)

    def mark_flagged(self, conn: dict[str, Any], transaction_ids: Sequence[str]) -> None:
        conn["flagged"] |= set(transaction_ids)


class FakeState:
    def __init__(self) -> None:
        self.data: dict[str, AccountState] = {}
        self.persisted: list[list[str]] = []
        self.fail_persist = False
        self.fail_fetch_times = 0

    def fetch(self, events: list[TransactionEvent]) -> dict[str, AccountState]:
        if self.fail_fetch_times > 0:
            self.fail_fetch_times -= 1
            raise rexc.ConnectionError("redis down")
        return {e.account_id: self.data.get(e.account_id, AccountState()) for e in events}

    def persist(self, events: list[TransactionEvent], states: dict[str, AccountState]) -> None:
        if self.fail_persist:
            raise rexc.ConnectionError("redis down")
        self.persisted.append([str(e.transaction_id) for e in events])
        self.data.update(states)


class FakeDlq:
    def __init__(self) -> None:
        self.published: list[tuple[Record, str, str, int]] = []

    def publish(self, record: Record, error_type: str, error_message: str, attempts: int) -> None:
        self.published.append((record, error_type, error_message, attempts))


def rec(value: bytes | None, offset: int = 0, partition: int = 0) -> Record:
    return Record("transactions.raw", partition, offset, b"acc_1", value)
