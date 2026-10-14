from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel


class ErrorBody(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorBody


class AlertOut(BaseModel):
    alert_id: int
    transaction_id: str
    account_id: str
    rule: str
    severity: str
    details: dict[str, Any]
    created_at: datetime


class TransactionOut(BaseModel):
    transaction_id: str
    account_id: str
    merchant_id: str
    amount: Decimal
    currency: str
    country: str
    channel: str
    event_time: datetime
    processed_at: datetime
    kafka_partition: int
    kafka_offset: int
    is_flagged: bool


class AlertPage(BaseModel):
    items: list[AlertOut]
    next_cursor: str | None = None


class AlertDetail(AlertOut):
    transaction: TransactionOut


class TransactionDetail(TransactionOut):
    alerts: list[AlertOut]


class AccountSummary(BaseModel):
    account_id: str
    transactions: int
    flagged_transactions: int
    alerts_by_rule: dict[str, int]
    last_seen: datetime | None


class Stats(BaseModel):
    transactions: int
    flagged_transactions: int
    alerts: int
    alerts_by_rule: dict[str, int]
    rate_last_5m_per_second: float


class Accepted(BaseModel):
    transaction_id: str
    status: str = "accepted"
