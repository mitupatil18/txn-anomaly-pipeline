"""Synthetic traffic generator with injected fraud patterns.

Example: python -m txnpipe.producer.simulator --rate 500 --duration 60 --seed 42
"""

import argparse
import json
import logging
import math
import random
import time
import uuid
from collections import deque
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

from confluent_kafka import KafkaException, Producer

from txnpipe.common.config import get_settings
from txnpipe.common.kafka import make_producer
from txnpipe.common.logging import setup_logging

log = logging.getLogger("txnpipe.simulator")

COUNTRIES = ["US", "IN", "GB", "DE", "FR", "BR", "JP", "AU", "CA", "SG"]
CHANNELS = ["online", "pos", "atm"]


def _money(value: float) -> str:
    return str(Decimal(str(round(max(value, 0.01), 2))).quantize(Decimal("0.01")))


class TrafficGenerator:
    """Deterministic for a given seed (apart from event timestamps, which use the wall clock)."""

    def __init__(
        self,
        accounts: int = 2000,
        fraud_ratio: float = 0.02,
        invalid_ratio: float = 0.0,
        seed: int | None = None,
    ) -> None:
        self.rng = random.Random(seed)
        self.accounts = [f"acc_{i:04d}" for i in range(accounts)]
        self.home = {a: self.rng.choice(COUNTRIES) for a in self.accounts}
        self.typical = {a: math.exp(self.rng.gauss(3.5, 0.5)) for a in self.accounts}
        n_fraud = max(1, round(accounts * fraud_ratio)) if fraud_ratio > 0 else 0
        self.fraud_accounts = set(self.rng.sample(self.accounts, n_fraud))
        self.invalid_ratio = invalid_ratio
        self.seen: dict[str, int] = {}
        self.queue: deque[dict[str, object]] = deque()

    def _txn(
        self, account: str, *, amount: float | None = None, country: str | None = None
    ) -> dict[str, object]:
        if amount is None:
            amount = self.typical[account] * math.exp(self.rng.gauss(0, 0.4))
        self.seen[account] = self.seen.get(account, 0) + 1
        return {
            "transaction_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "account_id": account,
            "merchant_id": f"m_{self.rng.randint(1, 999)}",
            "amount": _money(amount),
            "currency": "USD",
            "country": country or self.home[account],
            "channel": self.rng.choice(CHANNELS),
            "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    def _start_pattern(self, account: str) -> None:
        kind = self.rng.choice(["burst", "spike", "geo"])
        if kind == "spike" and self.seen.get(account, 0) < 6:
            kind = "burst"
        if kind == "burst":
            self.queue.extend(self._txn(account) for _ in range(8))
        elif kind == "spike":
            self.queue.append(self._txn(account, amount=self.typical[account] * 60 + 1500))
        else:
            foreign = self.rng.choice([c for c in COUNTRIES if c != self.home[account]])
            self.queue.append(self._txn(account))
            self.queue.append(self._txn(account, country=foreign))

    def _invalid(self) -> bytes:
        choice = self.rng.randint(0, 2)
        if choice == 0:
            return b"{not json"
        if choice == 1:
            return json.dumps({"transaction_id": "nope", "account_id": "x"}).encode()
        bad = self._txn(self.rng.choice(self.accounts), amount=1.0)
        bad["amount"] = "-5"
        return json.dumps(bad).encode()

    def stream(self) -> Iterator[tuple[str, bytes]]:
        """Yield ``(key, payload)`` forever."""
        while True:
            if self.invalid_ratio and self.rng.random() < self.invalid_ratio:
                yield self.rng.choice(self.accounts), self._invalid()
                continue
            if not self.queue:
                account = self.rng.choice(self.accounts)
                if account in self.fraud_accounts and self.rng.random() < 0.25:
                    self._start_pattern(account)
                else:
                    self.queue.append(self._txn(account))
            event = self.queue.popleft()
            yield str(event["account_id"]), json.dumps(event).encode()


def run(args: argparse.Namespace, producer: Producer | None = None) -> dict[str, float]:
    settings = get_settings()
    producer = producer or make_producer(settings)
    gen = TrafficGenerator(args.accounts, args.fraud_ratio, args.invalid_ratio, args.seed)
    limit = args.count if args.count else None
    deadline = None if args.count else time.monotonic() + args.duration
    errors = 0

    def on_delivery(err: object, _msg: object) -> None:
        nonlocal errors
        if err is not None:
            errors += 1

    start = time.monotonic()
    sent = 0
    for key, payload in gen.stream():
        if limit is not None and sent >= limit:
            break
        if deadline is not None and time.monotonic() >= deadline:
            break
        if args.rate > 0:
            wait = start + sent / args.rate - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        while True:
            try:
                producer.produce(
                    settings.topic_raw, value=payload, key=key, on_delivery=on_delivery
                )
                break
            except BufferError:
                producer.poll(0.1)
        sent += 1
        producer.poll(0)
    remaining = producer.flush(60)
    elapsed = time.monotonic() - start
    result = {
        "sent": sent,
        "elapsed_seconds": round(elapsed, 3),
        "achieved_rate": round(sent / elapsed, 1) if elapsed else 0.0,
        "delivery_errors": errors,
        "undelivered": remaining,
    }
    log.info("simulator finished", extra=result)
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rate", type=float, default=500, help="messages/second; 0 = unthrottled")
    p.add_argument("--duration", type=float, default=60, help="seconds (ignored with --count)")
    p.add_argument("--count", type=int, default=0, help="send exactly N messages and exit")
    p.add_argument("--accounts", type=int, default=2000)
    p.add_argument("--fraud-ratio", type=float, default=0.02)
    p.add_argument("--invalid-ratio", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    setup_logging(get_settings().log_level)
    try:
        result = run(parse_args(argv))
    except KafkaException:
        log.exception("kafka error")
        return 1
    return 0 if not result["delivery_errors"] and not result["undelivered"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
