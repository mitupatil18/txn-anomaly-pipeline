"""Redis-backed rule state.

Layout (see spec 5.5): ``vel:{acc}`` ZSET, ``amt:{acc}`` HASH, ``geo:{acc}`` HASH.
State is *read* before rules run and *written* only after the DB commit.
"""

from dataclasses import replace
from typing import Protocol

import redis

from txnpipe.common.schemas import TransactionEvent
from txnpipe.consumer.rules.base import AccountState

EWMA_ALPHA = 0.1
AMT_TTL_SECONDS = 7 * 24 * 3600
GEO_TTL_SECONDS = 24 * 3600


class StateStore(Protocol):
    def fetch(self, events: list[TransactionEvent]) -> dict[str, AccountState]: ...

    def persist(self, events: list[TransactionEvent], states: dict[str, AccountState]) -> None: ...


def apply_event(state: AccountState, event: TransactionEvent, window_seconds: int) -> AccountState:
    """Return the state after ``event``. Pure; used for in-batch overlay and the Redis write."""
    ts = event.event_epoch
    amount = float(event.amount)
    newest = max([ts, *state.velocity_ts])
    velocity = tuple(t for t in (*state.velocity_ts, ts) if t >= newest - window_seconds)
    ewma = amount if state.count == 0 else EWMA_ALPHA * amount + (1 - EWMA_ALPHA) * state.ewma
    newer = state.last_ts is None or ts >= state.last_ts
    return replace(
        state,
        velocity_ts=velocity,
        count=state.count + 1,
        ewma=ewma,
        last_country=event.country if newer else state.last_country,
        last_ts=ts if newer else state.last_ts,
    )


class RedisState:
    def __init__(self, client: redis.Redis, window_seconds: int = 60) -> None:
        self.client = client
        self.window = window_seconds

    def fetch(self, events: list[TransactionEvent]) -> dict[str, AccountState]:
        """One pipelined round trip for every account touched by ``events``."""
        oldest: dict[str, float] = {}
        for e in events:
            oldest[e.account_id] = min(oldest.get(e.account_id, e.event_epoch), e.event_epoch)
        accounts = list(oldest)
        pipe = self.client.pipeline(transaction=False)
        for acc in accounts:
            pipe.zrangebyscore(f"vel:{acc}", oldest[acc] - self.window, "+inf", withscores=True)
            pipe.hgetall(f"amt:{acc}")
            pipe.hgetall(f"geo:{acc}")
        results = pipe.execute()
        states: dict[str, AccountState] = {}
        for i, acc in enumerate(accounts):
            vel, amt, geo = results[3 * i : 3 * i + 3]
            states[acc] = AccountState(
                velocity_ts=tuple(score for _, score in vel),
                count=int(amt.get("count", 0)),
                ewma=float(amt.get("ewma", 0.0)),
                last_country=geo.get("country"),
                last_ts=float(geo["ts"]) if "ts" in geo else None,
            )
        return states

    def persist(self, events: list[TransactionEvent], states: dict[str, AccountState]) -> None:
        """Write the post-batch state for every account in ``events``."""
        pipe = self.client.pipeline(transaction=False)
        for e in events:
            pipe.zadd(f"vel:{e.account_id}", {str(e.transaction_id): e.event_epoch})
        for acc in {e.account_id for e in events}:
            st = states[acc]
            newest = max(st.velocity_ts, default=0.0)
            pipe.zremrangebyscore(f"vel:{acc}", "-inf", newest - self.window)
            pipe.expire(f"vel:{acc}", self.window + 60)
            pipe.hset(f"amt:{acc}", mapping={"count": st.count, "ewma": st.ewma})
            pipe.expire(f"amt:{acc}", AMT_TTL_SECONDS)
            if st.last_country is not None and st.last_ts is not None:
                pipe.hset(f"geo:{acc}", mapping={"country": st.last_country, "ts": st.last_ts})
                pipe.expire(f"geo:{acc}", GEO_TTL_SECONDS)
        pipe.execute()
