import json

import pytest
from tests.unit.helpers import make_event, payload

from txnpipe.common.schemas import parse_event

VALID = json.loads(payload())


def _with(**changes: object) -> bytes:
    data = {**VALID, **changes}
    return json.dumps({k: v for k, v in data.items() if v is not None}).encode()


def test_valid_event_normalizes_fields() -> None:
    e = parse_event(_with(country="in", currency="usd", timestamp="2026-10-15T10:21:33Z"))
    assert e.country == "IN" and e.currency == "USD"
    assert e.timestamp.tzinfo is not None


def test_currency_defaults_to_usd() -> None:
    assert parse_event(_with(currency=None)).currency == "USD"


@pytest.mark.parametrize(
    "bad",
    [
        {"transaction_id": None},
        {"account_id": ""},
        {"account_id": "x" * 65},
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.234"},
        {"amount": "abc"},
        {"amount": True},
        {"amount": "NaN"},
        {"country": "USA"},
        {"country": "1X"},
        {"currency": "US"},
        {"channel": "fax"},
        {"timestamp": "yesterday"},
        {"timestamp": "2026-01-01T00:00:00"},
        {"transaction_id": "not-a-uuid"},
    ],
)
def test_invalid_event_rejected(bad: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        parse_event(_with(**bad))


@pytest.mark.parametrize("raw", [None, b"{oops", b"[1,2]", b"\xff\xfe"])
def test_unparseable_payloads(raw: bytes | None) -> None:
    with pytest.raises(ValueError):
        parse_event(raw)


def test_amount_accepts_number_and_string() -> None:
    assert str(parse_event(_with(amount=12.5)).amount) == "12.5"
    assert make_event(amount="129.99").amount == parse_event(_with(amount=129.99)).amount
