"""Minimal server-rendered UI: latest alerts with a rule filter, refreshed by HTMX every 5s."""

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.engine import Connection

from txnpipe.api.deps import get_conn
from txnpipe.common.db import alerts

router = APIRouter(tags=["ui"], include_in_schema=False)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))

RULES = ["VELOCITY", "AMOUNT_SPIKE", "GEO_MISMATCH"]
LIMIT = 50


def _latest(conn: Connection, rule: str | None) -> list[dict[str, object]]:
    stmt = select(alerts).order_by(alerts.c.created_at.desc(), alerts.c.alert_id.desc())
    if rule in RULES:
        stmt = stmt.where(alerts.c.rule == rule)
    return [dict(r._mapping) for r in conn.execute(stmt.limit(LIMIT))]


@router.get("/ui", response_class=HTMLResponse)
def ui_page(
    request: Request, conn: Annotated[Connection, Depends(get_conn)], rule: str = ""
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "ui.html",
        {"rules": RULES, "rule": rule, "alerts": _latest(conn, rule), "limit": LIMIT},
    )


@router.get("/ui/alerts", response_class=HTMLResponse)
def ui_alerts(
    request: Request, conn: Annotated[Connection, Depends(get_conn)], rule: str = ""
) -> HTMLResponse:
    """HTML fragment (table body) polled by the page."""
    return templates.TemplateResponse(request, "_alerts.html", {"alerts": _latest(conn, rule)})
