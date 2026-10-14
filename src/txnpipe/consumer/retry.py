"""Exponential backoff with full jitter, for transient failures only."""

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

import redis.exceptions as redis_exc
from sqlalchemy import exc as sa_exc

T = TypeVar("T")


class RetryExhausted(Exception):
    """Transient failures persisted through every attempt."""

    def __init__(self, attempts: int, last: BaseException) -> None:
        super().__init__(f"gave up after {attempts} attempts: {last}")
        self.attempts = attempts
        self.last = last


@dataclass(frozen=True)
class RetryPolicy:
    base_seconds: float = 0.2
    max_seconds: float = 5.0
    max_attempts: int = 5


def classify_transient(exc: BaseException) -> str | None:
    """Return an error-type label if ``exc`` is worth retrying, else ``None``."""
    if isinstance(exc, sa_exc.DBAPIError):
        if exc.connection_invalidated or isinstance(
            exc, sa_exc.OperationalError | sa_exc.InterfaceError
        ):
            return "db"
        return None
    if isinstance(exc, sa_exc.TimeoutError):
        return "db"
    if isinstance(exc, redis_exc.ConnectionError | redis_exc.TimeoutError):
        return "redis"
    if isinstance(exc, TimeoutError | ConnectionError):
        return "timeout"
    return None


def backoff_delay(attempt: int, policy: RetryPolicy, rng: random.Random | None = None) -> float:
    """Full jitter: uniform(0, min(cap, base * 2**attempt)); ``attempt`` is 0-based."""
    ceiling = min(policy.max_seconds, policy.base_seconds * (2**attempt))
    return (rng or random).uniform(0, ceiling)


def retry_call(
    fn: Callable[[], T],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, BaseException, str], None] | None = None,
) -> T:
    """Run ``fn``; retry transient errors; re-raise anything else immediately."""
    for attempt in range(policy.max_attempts):
        try:
            return fn()
        except Exception as exc:
            kind = classify_transient(exc)
            if kind is None:
                raise
            if attempt + 1 >= policy.max_attempts:
                raise RetryExhausted(attempt + 1, exc) from exc
            if on_retry:
                on_retry(attempt + 1, exc, kind)
            sleep(backoff_delay(attempt, policy, rng))
    raise RuntimeError("max_attempts must be >= 1")  # pragma: no cover
