import random

import pytest
import redis.exceptions as rexc
from sqlalchemy import exc as sa_exc

from txnpipe.consumer.retry import (
    RetryExhausted,
    RetryPolicy,
    backoff_delay,
    classify_transient,
    retry_call,
)

POLICY = RetryPolicy(base_seconds=0.2, max_seconds=5, max_attempts=5)


class _MaxRng(random.Random):
    def uniform(self, a: float, b: float) -> float:
        return b


def test_backoff_ceiling_grows_then_caps() -> None:
    ceilings = [backoff_delay(i, POLICY, _MaxRng()) for i in range(8)]
    assert ceilings[:5] == pytest.approx([0.2, 0.4, 0.8, 1.6, 3.2])
    assert ceilings[5:] == [5, 5, 5]


def test_jitter_within_bounds() -> None:
    rng = random.Random(1)
    for attempt in range(6):
        d = backoff_delay(attempt, POLICY, rng)
        assert 0 <= d <= min(5, 0.2 * 2**attempt)


def test_backoff_without_rng() -> None:
    assert 0 <= backoff_delay(0, POLICY) <= 0.2


def test_retries_then_succeeds() -> None:
    calls: list[int] = []
    sleeps: list[float] = []
    seen: list[tuple[int, str]] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise rexc.ConnectionError("down")
        return "ok"

    out = retry_call(
        flaky, POLICY, sleep=sleeps.append, on_retry=lambda a, e, k: seen.append((a, k))
    )
    assert out == "ok" and len(calls) == 3 and len(sleeps) == 2
    assert seen == [(1, "redis"), (2, "redis")]


def test_exhaustion_after_max_attempts() -> None:
    calls: list[int] = []

    def always() -> None:
        calls.append(1)
        raise TimeoutError("slow")

    sleeps: list[float] = []
    with pytest.raises(RetryExhausted) as info:
        retry_call(always, POLICY, sleep=sleeps.append)
    assert len(calls) == 5 and info.value.attempts == 5
    assert len(sleeps) == 4  # no sleep after the final attempt


def test_non_transient_not_retried() -> None:
    calls: list[int] = []

    def bad() -> None:
        calls.append(1)
        raise ValueError("boom")

    with pytest.raises(ValueError):
        retry_call(bad, POLICY, sleep=lambda s: None)
    assert len(calls) == 1


def test_classification() -> None:
    op = sa_exc.OperationalError("s", {}, Exception("conn"))
    integrity = sa_exc.IntegrityError("s", {}, Exception("dup"))
    assert classify_transient(op) == "db"
    assert classify_transient(integrity) is None
    assert classify_transient(sa_exc.TimeoutError()) == "db"
    assert classify_transient(rexc.TimeoutError()) == "redis"
    assert classify_transient(ConnectionResetError()) == "timeout"
    assert classify_transient(KeyError()) is None
