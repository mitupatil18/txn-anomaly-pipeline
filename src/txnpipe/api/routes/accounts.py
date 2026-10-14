from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.engine import Connection

from txnpipe.api.deps import get_conn
from txnpipe.api.schemas import AccountSummary, ErrorResponse, Stats
from txnpipe.common.db import alerts, transactions

router = APIRouter(tags=["accounts"])


@router.get(
    "/accounts/{account_id}/summary",
    response_model=AccountSummary,
    responses={404: {"model": ErrorResponse}},
)
def account_summary(
    account_id: str, conn: Annotated[Connection, Depends(get_conn)]
) -> AccountSummary:
    t = conn.execute(
        select(
            func.count(),
            func.count().filter(transactions.c.is_flagged),
            func.max(transactions.c.event_time),
        ).where(transactions.c.account_id == account_id)
    ).one()
    if t[0] == 0:
        raise HTTPException(status_code=404, detail="account not found")
    by_rule = conn.execute(
        select(alerts.c.rule, func.count())
        .where(alerts.c.account_id == account_id)
        .group_by(alerts.c.rule)
    ).all()
    return AccountSummary(
        account_id=account_id,
        transactions=t[0],
        flagged_transactions=t[1],
        alerts_by_rule={r: c for r, c in by_rule},
        last_seen=t[2],
    )


@router.get("/stats", response_model=Stats, tags=["stats"])
def stats(conn: Annotated[Connection, Depends(get_conn)]) -> Stats:
    total, flagged = conn.execute(
        select(func.count(), func.count().filter(transactions.c.is_flagged))
    ).one()
    recent = conn.execute(
        select(func.count()).where(
            transactions.c.processed_at > datetime.now(UTC) - timedelta(minutes=5)
        )
    ).scalar_one()
    by_rule = conn.execute(select(alerts.c.rule, func.count()).group_by(alerts.c.rule)).all()
    return Stats(
        transactions=total,
        flagged_transactions=flagged,
        alerts=sum(c for _, c in by_rule),
        alerts_by_rule={r: c for r, c in by_rule},
        rate_last_5m_per_second=round(recent / 300, 3),
    )
