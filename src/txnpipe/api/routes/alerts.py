import base64
import binascii
import json
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, tuple_
from sqlalchemy.engine import Connection

from txnpipe.api.deps import get_conn
from txnpipe.api.schemas import AlertDetail, AlertOut, AlertPage, ErrorResponse, TransactionOut
from txnpipe.common.db import alerts, transactions

router = APIRouter(tags=["alerts"])


def encode_cursor(created_at: datetime, alert_id: int) -> str:
    raw = json.dumps([created_at.isoformat(), alert_id]).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        ts, alert_id = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(ts), int(alert_id)
    except (ValueError, TypeError, binascii.Error) as exc:
        raise HTTPException(status_code=400, detail="invalid cursor") from exc


def _alert_out(row: Any) -> AlertOut:
    return AlertOut(**{k: row._mapping[k] for k in AlertOut.model_fields})


@router.get("/alerts", response_model=AlertPage)
def list_alerts(
    conn: Annotated[Connection, Depends(get_conn)],
    account_id: str | None = None,
    rule: str | None = None,
    severity: str | None = None,
    from_: Annotated[datetime | None, Query(alias="from")] = None,
    to: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
) -> AlertPage:
    stmt = select(alerts).order_by(alerts.c.created_at.desc(), alerts.c.alert_id.desc())
    if account_id:
        stmt = stmt.where(alerts.c.account_id == account_id)
    if rule:
        stmt = stmt.where(alerts.c.rule == rule.upper())
    if severity:
        stmt = stmt.where(alerts.c.severity == severity.upper())
    if from_:
        stmt = stmt.where(alerts.c.created_at >= from_)
    if to:
        stmt = stmt.where(alerts.c.created_at <= to)
    if cursor:
        c_ts, c_id = decode_cursor(cursor)
        stmt = stmt.where(tuple_(alerts.c.created_at, alerts.c.alert_id) < (c_ts, c_id))
    rows = conn.execute(stmt.limit(limit + 1)).all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit:
        last = page[-1]
        next_cursor = encode_cursor(last.created_at, last.alert_id)
    return AlertPage(items=[_alert_out(r) for r in page], next_cursor=next_cursor)


@router.get(
    "/alerts/{alert_id}", response_model=AlertDetail, responses={404: {"model": ErrorResponse}}
)
def get_alert(alert_id: int, conn: Annotated[Connection, Depends(get_conn)]) -> AlertDetail:
    # alert_id is BIGINT: reject values that would overflow instead of raising a DB error.
    if alert_id > 2**63 - 1:
        raise HTTPException(status_code=404, detail="alert not found")
    row = conn.execute(select(alerts).where(alerts.c.alert_id == alert_id)).first()
    if row is None:
        raise HTTPException(status_code=404, detail="alert not found")
    txn = conn.execute(
        select(transactions).where(transactions.c.transaction_id == row.transaction_id)
    ).one()
    return AlertDetail(
        **_alert_out(row).model_dump(),
        transaction=TransactionOut(**{k: txn._mapping[k] for k in TransactionOut.model_fields}),
    )
