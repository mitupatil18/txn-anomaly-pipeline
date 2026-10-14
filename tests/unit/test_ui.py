from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from txnpipe.api.deps import get_conn
from txnpipe.api.main import create_app
from txnpipe.api.routes import ui
from txnpipe.common.config import Settings

ROW = {
    "alert_id": 1,
    "transaction_id": "7b1f4a52-9a1e-4b6a-8e53-2f2d1c9a0a11",
    "account_id": "acc_<b>1</b>",
    "rule": "VELOCITY",
    "severity": "HIGH",
    "details": {"count": 7, "limit": 5},
    "created_at": datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[TestClient, list[str | None]]]:
    seen: list[str | None] = []

    def fake_latest(conn: Any, rule: str | None) -> list[dict[str, object]]:
        seen.append(rule)
        return [ROW] if rule in ("", None, "VELOCITY") else []

    monkeypatch.setattr(ui, "_latest", fake_latest)
    app = create_app(Settings(), engine=object())  # type: ignore[arg-type]
    app.dependency_overrides[get_conn] = lambda: None
    with TestClient(app) as c:
        yield c, seen


def test_page_renders_alerts_filter_and_htmx(client: tuple[TestClient, list[str | None]]) -> None:
    c, _ = client
    html = c.get("/ui").text
    assert "Latest 50 alerts" in html and "VELOCITY" in html and 'hx-trigger="every 5s"' in html
    assert '<option value="GEO_MISMATCH" >' in html or "GEO_MISMATCH" in html
    assert "sev-high" in html and "count=7" in html


def test_account_is_html_escaped(client: tuple[TestClient, list[str | None]]) -> None:
    c, _ = client
    assert "<b>1</b>" not in c.get("/ui/alerts").text
    assert "&lt;b&gt;" in c.get("/ui/alerts").text


def test_fragment_is_filtered_by_rule_and_has_empty_state(
    client: tuple[TestClient, list[str | None]],
) -> None:
    c, seen = client
    assert "No alerts yet" in c.get("/ui/alerts", params={"rule": "GEO_MISMATCH"}).text
    assert seen[-1] == "GEO_MISMATCH"
    assert "<html" not in c.get("/ui/alerts").text  # fragment, not a page
    assert 'value="VELOCITY" selected' in c.get("/ui", params={"rule": "VELOCITY"}).text


def test_ui_not_in_openapi(client: tuple[TestClient, list[str | None]]) -> None:
    c, _ = client
    assert not [p for p in c.get("/openapi.json").json()["paths"] if p.startswith("/ui")]
