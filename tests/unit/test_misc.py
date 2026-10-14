import argparse
import json
import logging
from datetime import UTC, datetime

import pytest
from fastapi import HTTPException

from txnpipe.api.routes.alerts import decode_cursor, encode_cursor
from txnpipe.common.config import Settings
from txnpipe.common.kafka import consumer_config, producer_config
from txnpipe.common.logging import JsonFormatter
from txnpipe.common.schemas import parse_event
from txnpipe.consumer.rules import build_rules
from txnpipe.producer import simulator


def test_cursor_roundtrip_and_rejects_garbage() -> None:
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    assert decode_cursor(encode_cursor(ts, 17)) == (ts, 17)
    for bad in ["!!!", "bm90LWpzb24=", "W10="]:
        with pytest.raises(HTTPException) as info:
            decode_cursor(bad)
        assert info.value.status_code == 400


def test_kafka_configs_match_spec() -> None:
    c = consumer_config(Settings())
    assert c["enable.auto.commit"] is False and c["isolation.level"] == "read_committed"
    assert c["partition.assignment.strategy"] == "cooperative-sticky"
    p = producer_config(Settings())
    assert p["acks"] == "all" and p["enable.idempotence"] is True and p["compression.type"] == "lz4"


def test_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VELOCITY_LIMIT", "9")
    monkeypatch.setenv("BATCH_SIZE", "200")
    s = Settings()
    assert s.velocity_limit == 9 and s.batch_size == 200
    assert build_rules(s)[0].limit == 9  # type: ignore[attr-defined]


def test_json_formatter_includes_extras() -> None:
    record = logging.LogRecord("x", logging.INFO, "f", 1, "hello %s", ("w",), None)
    record.partition = 3
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "hello w" and out["partition"] == 3 and out["level"] == "INFO"


def test_simulator_is_deterministic_and_valid() -> None:
    def first(n: int) -> list[tuple[str, dict[str, object]]]:
        g = simulator.TrafficGenerator(50, 0.2, 0.0, seed=7)
        s = g.stream()
        out = []
        for _ in range(n):
            k, v = next(s)
            d = json.loads(v)
            d.pop("timestamp")
            out.append((k, d))
        return out

    a, b = first(300), first(300)
    assert a == b
    g = simulator.TrafficGenerator(50, 0.2, 0.0, seed=7)
    s = g.stream()
    for _ in range(300):
        key, raw = next(s)
        assert parse_event(raw).account_id == key


def test_simulator_injects_fraud_and_invalid() -> None:
    g = simulator.TrafficGenerator(20, 0.5, 0.3, seed=1)
    s = g.stream()
    bad = good = 0
    per_account: dict[str, int] = {}
    for _ in range(1500):
        _, raw = next(s)
        try:
            e = parse_event(raw)
            good += 1
            per_account[e.account_id] = per_account.get(e.account_id, 0) + 1
        except ValueError:
            bad += 1
    assert bad > 200 and good > 500


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[bytes] = []

    def produce(self, topic: str, value: bytes, key: str, on_delivery: object) -> None:
        self.sent.append(value)

    def poll(self, t: float) -> None:
        pass

    def flush(self, t: float) -> int:
        return 0


def test_simulator_run_sends_exact_count() -> None:
    args = argparse.Namespace(
        rate=0, duration=1, count=25, accounts=10, fraud_ratio=0.1, invalid_ratio=0.0, seed=3
    )
    producer = FakeProducer()
    result = simulator.run(args, producer)  # type: ignore[arg-type]
    assert len(producer.sent) == 25 and result["sent"] == 25
    assert simulator.parse_args(["--count", "5", "--seed", "1"]).count == 5
