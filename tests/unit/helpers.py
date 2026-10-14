from datetime import UTC, datetime
from typing import Any

from txnpipe.common.schemas import TransactionEvent

BASE_TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC).timestamp()


def make_event(
    *,
    tid: str = "7b1f4a52-9a1e-4b6a-8e53-2f2d1c9a0a11",
    account: str = "acc_1",
    amount: str = "10.00",
    country: str = "US",
    ts: float = BASE_TS,
) -> TransactionEvent:
    return TransactionEvent.model_validate(
        {
            "transaction_id": tid,
            "account_id": account,
            "merchant_id": "m_1",
            "amount": amount,
            "currency": "USD",
            "country": country,
            "channel": "online",
            "timestamp": datetime.fromtimestamp(ts, UTC).isoformat(),
        }
    )


def payload(**kwargs: Any) -> bytes:
    return make_event(**kwargs).model_dump_json().encode()
