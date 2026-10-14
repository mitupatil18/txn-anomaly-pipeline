import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import Consumer
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.engine import Engine

from tests.unit.helpers import payload
from txnpipe.api.main import create_app
from txnpipe.common.config import Settings

pytestmark = pytest.mark.integration

T0 = datetime(2026, 1, 1, 12, tzinfo=UTC)


def _tid(i: int) -> str:
    return str(uuid.UUID(int=i, version=4))


@pytest.fixture
def seeded(engine: Engine) -> None:
    """3 accounts, 6 transactions, 6 alerts with distinct created_at (one pair identical)."""
    with engine.begin() as c:
        for i in range(1, 7):
            acc = f"acc_{(i - 1) % 3}"
            c.execute(
                text(
                    "INSERT INTO transactions (transaction_id, account_id, merchant_id, amount,"
                    " currency, country, channel, event_time, kafka_partition, kafka_offset,"
                    " is_flagged) VALUES (:id, :acc, 'm', 10.5, 'USD', 'US', 'pos', :t, 0, :o,"
                    " true)"
                ),
                {"id": _tid(i), "acc": acc, "t": T0, "o": i},
            )
            created = T0 + timedelta(seconds=0 if i in (3, 4) else i)  # i=3,4 share a timestamp
            c.execute(
                text(
                    "INSERT INTO alerts (transaction_id, account_id, rule, severity, details,"
                    " created_at) VALUES (:id, :acc, :rule, :sev, '{\"k\": 1}', :c)"
                ),
                {
                    "id": _tid(i),
                    "acc": acc,
                    "rule": "VELOCITY" if i % 2 else "GEO_MISMATCH",
                    "sev": "HIGH" if i > 4 else "MEDIUM",
                    "c": created,
                },
            )


@pytest.fixture
def client(engine: Engine, redis_url: str) -> Iterator[TestClient]:
    app = create_app(Settings(database_url=str(engine.url)), engine)
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def test_health_ready_metrics(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/ready").json() == {"status": "ready"}
    body = client.get("/metrics").text
    assert "http_request_duration_seconds" in body


def test_list_alerts_filters(client: TestClient, seeded: None) -> None:
    assert len(client.get("/alerts").json()["items"]) == 6
    assert len(client.get("/alerts", params={"account_id": "acc_0"}).json()["items"]) == 2
    assert len(client.get("/alerts", params={"rule": "velocity"}).json()["items"]) == 3
    assert len(client.get("/alerts", params={"severity": "HIGH"}).json()["items"]) == 2
    window = {
        "from": (T0 + timedelta(seconds=1)).isoformat(),
        "to": (T0 + timedelta(seconds=5)).isoformat(),
    }
    assert len(client.get("/alerts", params=window).json()["items"]) == 3


def test_keyset_pagination_visits_every_alert_once_in_order(
    client: TestClient, seeded: None
) -> None:
    seen: list[int] = []
    cursor = None
    pages = 0
    while True:
        params = {"limit": 2, **({"cursor": cursor} if cursor else {})}
        body = client.get("/alerts", params=params).json()
        seen += [a["alert_id"] for a in body["items"]]
        pages += 1
        cursor = body["next_cursor"]
        if not cursor:
            break
    assert pages == 3 and len(seen) == len(set(seen)) == 6
    full = [a["alert_id"] for a in client.get("/alerts").json()["items"]]
    assert seen == full


def test_pagination_validation_errors(client: TestClient, seeded: None) -> None:
    assert client.get("/alerts", params={"limit": 0}).status_code == 422
    assert client.get("/alerts", params={"limit": 201}).status_code == 422
    r = client.get("/alerts", params={"cursor": "garbage!"})
    assert r.status_code == 400 and r.json()["error"]["code"] == "bad_request"


def test_get_alert_and_transaction(client: TestClient, seeded: None) -> None:
    first = client.get("/alerts").json()["items"][0]
    detail = client.get(f"/alerts/{first['alert_id']}").json()
    assert detail["transaction"]["transaction_id"] == first["transaction_id"]
    txn = client.get(f"/transactions/{first['transaction_id']}").json()
    assert txn["is_flagged"] is True and len(txn["alerts"]) == 1


def test_not_found_uses_error_envelope(client: TestClient, seeded: None) -> None:
    for url in ["/alerts/999999", f"/transactions/{_tid(999)}", "/accounts/nobody/summary"]:
        r = client.get(url)
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "not_found"
    assert client.get("/alerts/99999999999999999999").status_code == 404
    assert client.get("/transactions/not-a-uuid").status_code == 422


def test_account_summary_and_stats(client: TestClient, seeded: None) -> None:
    s = client.get("/accounts/acc_0/summary").json()
    assert s["transactions"] == 2 and s["flagged_transactions"] == 2
    assert sum(s["alerts_by_rule"].values()) == 2 and s["last_seen"]
    stats = client.get("/stats").json()
    assert stats["transactions"] == 6 and stats["alerts"] == 6
    assert stats["alerts_by_rule"] == {"VELOCITY": 3, "GEO_MISMATCH": 3}


def test_post_transaction_publishes_to_kafka(
    engine: Engine, settings: Settings, kafka_bootstrap: str
) -> None:
    app = create_app(settings, engine)
    with TestClient(app) as client:
        ok = client.post(
            "/transactions",
            content=payload(tid=_tid(1)),
            headers={"content-type": "application/json"},
        )
        assert ok.status_code == 202 and ok.json()["transaction_id"] == _tid(1)
        bad = client.post("/transactions", json={"account_id": "x"})
        assert bad.status_code == 422 and bad.json()["error"]["code"] == "validation_error"
    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"check.{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([settings.topic_raw])
    msg = None
    for _ in range(30):
        msg = c.poll(1.0)
        if msg is not None and not msg.error():
            break
    c.close()
    assert msg is not None and msg.key() == b"acc_1"


def test_ui_lists_latest_alerts_from_database(client: TestClient, seeded: None) -> None:
    page = client.get("/ui")
    assert page.status_code == 200 and page.text.count("<tr>") >= 7  # header + 6 alerts
    only = client.get("/ui/alerts", params={"rule": "GEO_MISMATCH"}).text
    assert "GEO_MISMATCH" in only and "VELOCITY" not in only
    assert "No alerts yet" not in only
