"""Processing of one message or one batch (spec section 7.2)."""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from txnpipe.common import metrics as m
from txnpipe.common.schemas import TransactionEvent, parse_event
from txnpipe.consumer.dlq import DlqPublisher
from txnpipe.consumer.records import Record
from txnpipe.consumer.repository import AlertRow, Repository
from txnpipe.consumer.retry import RetryExhausted, RetryPolicy, retry_call
from txnpipe.consumer.rules import AccountState, Rule
from txnpipe.consumer.state import StateStore, apply_event

log = logging.getLogger(__name__)


@dataclass
class _Outcome:
    fresh: list[TransactionEvent]
    states: dict[str, AccountState]
    alerts: list[AlertRow]
    duplicates: int


class Handler:
    def __init__(
        self,
        *,
        repo: Repository,
        state: StateStore,
        dlq: DlqPublisher,
        rules: list[Rule],
        policy: RetryPolicy,
        window_seconds: int = 60,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.repo = repo
        self.state = state
        self.dlq = dlq
        self.rules = rules
        self.policy = policy
        self.window = window_seconds
        self.clock = clock
        self.sleep = sleep

    def process_batch(self, records: list[Record]) -> None:
        """Handle every record. Returns only once each is stored, skipped or in the DLQ.

        Raises only if the DLQ itself is unavailable; the caller must then not commit offsets.
        """
        if not records:
            return
        started = time.perf_counter()
        valid: list[tuple[TransactionEvent, Record]] = []
        for rec in records:
            try:
                valid.append((parse_event(rec.value), rec))
            except ValueError as exc:
                self._dead_letter(rec, "validation", str(exc), attempts=0)
        if valid:
            self._process_valid(valid)
        m.TXN_BATCH_SIZE.observe(len(records))
        m.TXN_PROCESSING_SECONDS.observe((time.perf_counter() - started) / len(records))

    def _process_valid(self, valid: list[tuple[TransactionEvent, Record]]) -> None:
        try:
            outcome = retry_call(
                lambda: self._attempt(valid),
                self.policy,
                sleep=self.sleep,
                on_retry=self._on_retry,
            )
        except RetryExhausted as exc:
            for _, rec in valid:
                self._dead_letter(rec, "transient_exhausted", str(exc.last), exc.attempts)
            return
        except Exception as exc:
            if len(valid) > 1:
                # Isolate the poison message: reprocess one by one.
                log.warning("batch failed, retrying per message", extra={"error": str(exc)})
                for item in valid:
                    self._process_valid([item])
            else:
                log.exception("unexpected error")
                self._dead_letter(valid[0][1], "unexpected", repr(exc), attempts=1)
            return
        self._after_commit(outcome)

    def _on_retry(self, attempt: int, exc: BaseException, kind: str) -> None:
        m.TXN_RETRIES.labels(error_type=kind).inc()
        log.warning("transient error, retrying", extra={"attempt": attempt, "kind": kind})

    def _attempt(self, valid: list[tuple[TransactionEvent, Record]]) -> _Outcome:
        """One DB transaction: dedupe-insert, evaluate rules, insert alerts, commit."""
        with self.repo.begin() as conn:
            new_ids = self.repo.insert_transactions(
                conn, [(e, r.partition, r.offset) for e, r in valid]
            )
            fresh: list[TransactionEvent] = []
            taken: set[str] = set()
            for event, _ in valid:
                tid = str(event.transaction_id)
                if tid in new_ids and tid not in taken:
                    taken.add(tid)
                    fresh.append(event)
            states = self.state.fetch(fresh) if fresh else {}
            alert_rows: list[AlertRow] = []
            for event in fresh:
                current = states.get(event.account_id, AccountState())
                for rule in self.rules:
                    result = rule.evaluate(event, current)
                    if result:
                        alert_rows.append(
                            AlertRow(
                                str(event.transaction_id),
                                event.account_id,
                                result.rule,
                                result.severity,
                                result.details,
                            )
                        )
                # Overlay: later events in this batch must see this one (Redis is updated later).
                states[event.account_id] = apply_event(current, event, self.window)
            self.repo.insert_alerts(conn, alert_rows)
            self.repo.mark_flagged(conn, sorted({a.transaction_id for a in alert_rows}))
        return _Outcome(fresh, states, alert_rows, len(valid) - len(fresh))

    def _after_commit(self, outcome: _Outcome) -> None:
        if outcome.fresh:
            try:
                self.state.persist(outcome.fresh, outcome.states)
            except Exception:
                m.TXN_STATE_UPDATE_FAILURES.inc()
                log.exception("redis state update failed after DB commit")
        now = self.clock()
        for event in outcome.fresh:
            m.TXN_E2E_LATENCY.observe(max(0.0, now - event.event_epoch))
        for a in outcome.alerts:
            m.TXN_ALERTS.labels(rule=a.rule, severity=a.severity).inc()
        if outcome.fresh:
            m.TXN_PROCESSED.labels(result="ok").inc(len(outcome.fresh))
        if outcome.duplicates:
            m.TXN_PROCESSED.labels(result="duplicate").inc(outcome.duplicates)

    def _dead_letter(self, rec: Record, error_type: str, message: str, attempts: int) -> None:
        self.dlq.publish(rec, error_type, message, attempts)
        m.TXN_PROCESSED.labels(result="dlq").inc()
        log.warning(
            "message sent to DLQ",
            extra={"error_type": error_type, "partition": rec.partition, "offset": rec.offset},
        )
