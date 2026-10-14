"""Event contracts shared by the producer, consumer and API."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Channel(StrEnum):
    ONLINE = "online"
    POS = "pos"
    ATM = "atm"


class TransactionEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    transaction_id: UUID
    account_id: Annotated[str, Field(min_length=1, max_length=64)]
    merchant_id: Annotated[str, Field(min_length=1)]
    amount: Decimal
    currency: str = "USD"
    country: str
    channel: Channel
    timestamp: datetime

    @field_validator("amount", mode="before")
    @classmethod
    def _amount_not_bool(cls, v: object) -> object:
        if isinstance(v, bool):
            raise ValueError("amount must be a number")
        return v

    @field_validator("amount")
    @classmethod
    def _amount_valid(cls, v: Decimal) -> Decimal:
        if not v.is_finite() or v <= 0:
            raise ValueError("amount must be a positive number")
        if v != v.quantize(Decimal("0.01")):
            raise ValueError("amount must have at most 2 decimal places")
        if v >= Decimal("1e12"):
            raise ValueError("amount too large")
        return v

    @field_validator("currency", "country")
    @classmethod
    def _upper_alpha(cls, v: str, info: object) -> str:
        v = v.upper()
        name = getattr(info, "field_name", "")
        size = 3 if name == "currency" else 2
        if len(v) != size or not (v.isascii() and v.isalpha()):
            raise ValueError(f"{name} must be {size} letters")
        return v

    @field_validator("timestamp")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must include a timezone")
        return v.astimezone(UTC)

    @property
    def event_epoch(self) -> float:
        return self.timestamp.timestamp()


def parse_event(payload: bytes | str | None) -> TransactionEvent:
    """Parse a raw Kafka payload. Raises ``ValueError`` (incl. pydantic errors) when invalid."""
    if payload is None:
        raise ValueError("empty payload")
    try:
        data = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("payload must be a JSON object")
    return TransactionEvent.model_validate(data)


def event_to_json(event: TransactionEvent) -> bytes:
    return event.model_dump_json().encode()
