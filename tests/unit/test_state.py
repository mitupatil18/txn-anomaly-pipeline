from typing import Any

from tests.unit.helpers import BASE_TS, make_event
from txnpipe.consumer.rules import AccountState
from txnpipe.consumer.state import EWMA_ALPHA, RedisState, apply_event


def test_apply_event_updates_all_dimensions() -> None:
    s = apply_event(AccountState(), make_event(amount="100.00", country="IN"), 60)
    assert (s.count, s.ewma, s.last_country, s.last_ts) == (1, 100.0, "IN", BASE_TS)
    s = apply_event(
        s,
        make_event(
            tid="a" * 8 + "-0000-4000-8000-000000000000",
            amount="200.00",
            country="US",
            ts=BASE_TS + 10,
        ),
        60,
    )
    assert s.count == 2
    assert s.ewma == EWMA_ALPHA * 200 + (1 - EWMA_ALPHA) * 100
    assert s.last_country == "US" and len(s.velocity_ts) == 2


def test_apply_event_prunes_window_and_ignores_older_geo() -> None:
    s = apply_event(AccountState(), make_event(ts=BASE_TS), 60)
    s = apply_event(s, make_event(ts=BASE_TS + 120), 60)
    assert s.velocity_ts == (BASE_TS + 120,)
    older = apply_event(s, make_event(ts=BASE_TS + 10, country="FR"), 60)
    assert older.last_country == "US" and older.last_ts == BASE_TS + 120


class FakePipeline:
    def __init__(self, store: dict[str, Any]) -> None:
        self.store, self.ops = store, []

    def __getattr__(self, name: str) -> Any:
        def record(*args: Any, **kwargs: Any) -> "FakePipeline":
            self.ops.append((name, args, kwargs))
            return self

        return record

    def execute(self) -> list[Any]:
        out = []
        for name, args, _ in self.ops:
            if name == "zrangebyscore":
                out.append(self.store.get(("vel", args[0]), []))
            elif name == "hgetall":
                out.append(self.store.get(("h", args[0]), {}))
        return out


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[Any, Any] = {}
        self.last: FakePipeline | None = None

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        self.last = FakePipeline(self.store)
        return self.last


def test_redis_state_fetch_parses_snapshot() -> None:
    r = FakeRedis()
    r.store[("vel", "vel:acc_1")] = [("t1", BASE_TS - 5), ("t2", BASE_TS - 1)]
    r.store[("h", "amt:acc_1")] = {"count": "7", "ewma": "42.5"}
    r.store[("h", "geo:acc_1")] = {"country": "IN", "ts": str(BASE_TS - 3)}
    states = RedisState(r, 60).fetch([make_event(), make_event(account="acc_2")])  # type: ignore[arg-type]
    s1, s2 = states["acc_1"], states["acc_2"]
    assert s1.velocity_ts == (BASE_TS - 5, BASE_TS - 1) and s1.count == 7 and s1.ewma == 42.5
    assert (s1.last_country, s1.last_ts) == ("IN", BASE_TS - 3)
    assert s2 == AccountState()


def test_redis_state_persist_issues_expected_commands() -> None:
    r = FakeRedis()
    ev = make_event()
    state = apply_event(AccountState(), ev, 60)
    RedisState(r, 60).persist([ev], {"acc_1": state})  # type: ignore[arg-type]
    assert r.last is not None
    names = [op[0] for op in r.last.ops]
    assert names.count("zadd") == 1 and names.count("hset") == 2 and names.count("expire") == 3
