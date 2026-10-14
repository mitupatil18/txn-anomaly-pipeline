import json
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tests.integration.conftest import Pipeline, publish, wait_for
from tests.unit.helpers import BASE_TS, payload

from txnpipe.common.config import Settings

pytestmark = pytest.mark.integration


def _count(engine: Engine, table: str) -> int:
    with engine.connect() as c:
        return int(c.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())


def _tid(i: int) -> str:
    return str(uuid.UUID(int=i, version=4))


@pytest.mark.parametrize("batch_size", [1, 50])
def test_end_to_end_creates_transactions_and_alerts(
    engine: Engine, settings: Settings, producer: Producer, batch_size: int
) -> None:
    # 8 transactions in 8 seconds for one account => velocity alerts on the 6th, 7th, 8th.
    for i in range(1, 9):
        publish(producer, settings, "acc_1", payload(tid=_tid(i), ts=BASE_TS + i))
    with Pipeline(settings, batch_size):
        wait_for(lambda: _count(engine, "transactions") == 8)
    with engine.connect() as c:
        rules = c.execute(text("SELECT rule, count(*) FROM alerts GROUP BY rule")).all()
        flagged = c.execute(text("SELECT count(*) FROM transactions WHERE is_flagged")).scalar_one()
    assert dict(rules) == {"VELOCITY": 3}
    assert flagged == 3


def test_duplicate_transaction_id_yields_single_row_and_alert_set(
    engine: Engine, settings: Settings, producer: Producer
) -> None:
    events = [payload(tid=_tid(i), ts=BASE_TS + i) for i in range(1, 8)]
    for raw in events + events:  # every message published twice
        publish(producer, settings, "acc_1", raw)
    with Pipeline(settings, batch_size=10):
        wait_for(lambda: _count(engine, "transactions") == 7)
        # let the duplicates drain too
        wait_for(lambda: _group_lag_zero(settings))
    assert _count(engine, "transactions") == 7
    assert _count(engine, "alerts") == 2  # 6th and 7th exceed the limit, once each


def _group_lag_zero(settings: Settings) -> bool:
    from confluent_kafka import TopicPartition

    c = Consumer(
        {"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": settings.consumer_group}
    )
    try:
        parts = [TopicPartition(settings.topic_raw, p) for p in range(3)]
        committed = c.committed(parts, timeout=10)
        for tp in committed:
            _, high = c.get_watermark_offsets(tp, timeout=10)
            if high > 0 and (tp.offset < 0 or tp.offset < high):
                return False
        return True
    finally:
        c.close()


def test_malformed_message_lands_in_dlq_with_headers(
    engine: Engine, settings: Settings, producer: Producer
) -> None:
    publish(producer, settings, "acc_9", b"{definitely not json")
    publish(producer, settings, "acc_1", payload(tid=_tid(1)))
    with Pipeline(settings):
        dlq = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": f"check.{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        dlq.subscribe([settings.topic_dlq])
        msg = None
        for _ in range(60):
            msg = dlq.poll(1.0)
            if msg is not None and not msg.error():
                break
        dlq.close()
        wait_for(lambda: _count(engine, "transactions") == 1)
    assert msg is not None and msg.value() == b"{definitely not json"
    headers = {k: v.decode() for k, v in msg.headers()}
    assert headers["x-error-type"] == "validation"
    assert headers["x-original-topic"] == settings.topic_raw
    assert headers["x-attempts"] == "0"
    assert {"x-original-partition", "x-original-offset", "x-error-message", "x-failed-at"} <= set(
        headers
    )


def test_crash_before_offset_commit_causes_no_duplicates(
    engine: Engine, settings: Settings, producer: Producer
) -> None:
    for i in range(1, 8):
        publish(producer, settings, "acc_1", payload(tid=_tid(i), ts=BASE_TS + i))
    # First run: DB commits happen, but offsets are never committed (simulated crash).
    with Pipeline(settings, batch_size=3, commit=False):
        wait_for(lambda: _count(engine, "transactions") == 7)
    alerts_before = _count(engine, "alerts")
    # Restart in the same group: everything is redelivered from the last committed offset.
    with Pipeline(settings, batch_size=3):
        wait_for(lambda: _group_lag_zero(settings))
    assert _count(engine, "transactions") == 7
    assert _count(engine, "alerts") == alerts_before == 2


def test_state_survives_consumer_restart_via_redis(
    engine: Engine, settings: Settings, producer: Producer
) -> None:
    for i in range(1, 5):
        publish(producer, settings, "acc_1", payload(tid=_tid(i), ts=BASE_TS + i))
    with Pipeline(settings):
        wait_for(lambda: _count(engine, "transactions") == 4)
    for i in range(5, 8):  # a *new* consumer sees the earlier events through Redis
        publish(producer, settings, "acc_1", payload(tid=_tid(i), ts=BASE_TS + i))
    with Pipeline(settings):
        wait_for(lambda: _count(engine, "transactions") == 7)
    with engine.connect() as c:
        rule_counts = c.execute(text("SELECT count(*) FROM alerts WHERE rule='VELOCITY'"))
        assert rule_counts.scalar_one() == 2


def test_alert_details_are_json(engine: Engine, settings: Settings, producer: Producer) -> None:
    for i in range(1, 8):
        publish(producer, settings, "acc_1", payload(tid=_tid(i), ts=BASE_TS + i))
    with Pipeline(settings):
        wait_for(lambda: _count(engine, "alerts") == 2)
    with engine.connect() as c:
        details = c.execute(
            text("SELECT details FROM alerts ORDER BY alert_id LIMIT 1")
        ).scalar_one()
    assert json.loads(json.dumps(details))["limit"] == 5
