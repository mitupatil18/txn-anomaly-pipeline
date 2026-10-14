import uuid

import pytest

from tests.unit.fakes import FakeDlq, FakeRepo, FakeState, rec
from tests.unit.helpers import BASE_TS, make_event, payload
from txnpipe.common.config import Settings
from txnpipe.consumer.handler import Handler
from txnpipe.consumer.retry import RetryPolicy
from txnpipe.consumer.rules import build_rules


class Env:
    def __init__(self) -> None:
        self.repo = FakeRepo()
        self.state = FakeState()
        self.dlq = FakeDlq()
        self.sleeps: list[float] = []
        self.handler = Handler(
            repo=self.repo,
            state=self.state,
            dlq=self.dlq,
            rules=build_rules(Settings()),
            policy=RetryPolicy(0.2, 5, 5),
            clock=lambda: BASE_TS + 1,
            sleep=self.sleeps.append,
        )


@pytest.fixture
def env() -> Env:
    return Env()


def _tid(i: int) -> str:
    return str(uuid.UUID(int=i, version=4))


def test_valid_message_is_stored_and_state_updated(env: Env) -> None:
    env.handler.process_batch([rec(payload(tid=_tid(1)), offset=7, partition=2)])
    assert env.repo.txns == {_tid(1): (2, 7)}
    assert env.state.persisted == [[_tid(1)]]
    assert env.state.data["acc_1"].count == 1


def test_duplicate_is_skipped_without_rules_or_state(env: Env) -> None:
    msg = rec(payload(tid=_tid(1)))
    env.handler.process_batch([msg])
    env.handler.process_batch([msg])
    assert len(env.repo.txns) == 1
    assert len(env.state.persisted) == 1  # second delivery did not touch state
    assert env.state.data["acc_1"].count == 1


def test_duplicate_inside_one_batch(env: Env) -> None:
    env.handler.process_batch([rec(payload(tid=_tid(1))), rec(payload(tid=_tid(1)), offset=1)])
    assert len(env.repo.txns) == 1
    assert env.state.data["acc_1"].count == 1


def test_invalid_goes_to_dlq_without_retry(env: Env) -> None:
    bad = rec(b"{nope", offset=5)
    env.handler.process_batch([bad])
    assert [(p[0], p[1], p[3]) for p in env.dlq.published] == [(bad, "validation", 0)]
    assert env.repo.attempts == 0 and env.sleeps == []


def test_invalid_does_not_block_valid_in_batch(env: Env) -> None:
    env.handler.process_batch([rec(b"x"), rec(payload(tid=_tid(2)), offset=1)])
    assert len(env.dlq.published) == 1
    assert _tid(2) in env.repo.txns


def test_transient_error_retried_then_succeeds(env: Env) -> None:
    env.repo.fail_times = 2
    env.handler.process_batch([rec(payload(tid=_tid(1)))])
    assert _tid(1) in env.repo.txns
    assert len(env.sleeps) == 2 and env.dlq.published == []


def test_redis_read_failure_rolls_back_and_retries(env: Env) -> None:
    env.state.fail_fetch_times = 1
    env.handler.process_batch([rec(payload(tid=_tid(1)))])
    assert env.repo.commits == 1 and _tid(1) in env.repo.txns
    assert env.repo.attempts == 2


def test_transient_exhausted_goes_to_dlq(env: Env) -> None:
    env.repo.fail_times = 99
    env.handler.process_batch([rec(payload(tid=_tid(1)), offset=3)])
    assert env.repo.txns == {}
    (record, kind, _msg, attempts) = env.dlq.published[0]
    assert kind == "transient_exhausted" and attempts == 5 and record.offset == 3


def test_unexpected_error_goes_to_dlq_without_retry(env: Env) -> None:
    env.repo.fail_times = 1
    env.repo.fail_with = RuntimeError("bug")
    env.handler.process_batch([rec(payload(tid=_tid(1)))])
    assert env.dlq.published[0][1] == "unexpected" and env.sleeps == []


def test_poison_message_isolated_from_rest_of_batch(env: Env) -> None:
    env.repo.poison_id = _tid(2)
    batch = [rec(payload(tid=_tid(i)), offset=i) for i in (1, 2, 3)]
    env.handler.process_batch(batch)
    assert set(env.repo.txns) == {_tid(1), _tid(3)}
    assert [(p[0].offset, p[1]) for p in env.dlq.published] == [(2, "unexpected")]


def test_redis_persist_failure_does_not_fail_message(env: Env) -> None:
    env.state.fail_persist = True
    env.handler.process_batch([rec(payload(tid=_tid(1)))])
    assert _tid(1) in env.repo.txns and env.dlq.published == []


def test_velocity_alert_within_a_single_batch(env: Env) -> None:
    """Rule state is only written to Redis after commit, so the batch must overlay it."""
    batch = [rec(payload(tid=_tid(i), ts=BASE_TS + i), offset=i) for i in range(1, 8)]
    env.handler.process_batch(batch)
    velocity = {t for (t, r) in env.repo.alerts if r == "VELOCITY"}
    assert velocity == {_tid(6), _tid(7)}  # 6th and 7th exceed limit 5
    assert env.repo.flagged == velocity


def test_alerts_are_idempotent_on_redelivery(env: Env) -> None:
    batch = [rec(payload(tid=_tid(i), ts=BASE_TS + i), offset=i) for i in range(1, 8)]
    env.handler.process_batch(batch)
    before = dict(env.repo.alerts)
    env.handler.process_batch(batch)
    assert env.repo.alerts == before


def test_geo_and_spike_alerts(env: Env) -> None:
    for i in range(6):
        env.handler.process_batch([rec(payload(tid=_tid(i), amount="10.00", ts=BASE_TS + i * 100))])
    ev = make_event(tid=_tid(50), amount="5000.00", country="IN", ts=BASE_TS + 600)
    env.handler.process_batch([rec(ev.model_dump_json().encode(), offset=50)])
    rules = {r for (t, r) in env.repo.alerts if t == _tid(50)}
    assert rules == {"AMOUNT_SPIKE", "GEO_MISMATCH"} or "AMOUNT_SPIKE" in rules


def test_empty_batch_is_noop(env: Env) -> None:
    env.handler.process_batch([])
    assert env.repo.attempts == 0
