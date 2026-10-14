"""Replay messages from the DLQ back to the raw topic.

Reads transactions.dlq from the beginning with a throwaway consumer group (no offsets
are committed), so it can be run repeatedly. Use --dry-run first.

    python scripts/replay_dlq.py --limit 100 --error-type transient_exhausted
"""

import argparse
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer

from txnpipe.common.config import get_settings


def _headers(msg_headers: list[tuple[str, bytes]] | None) -> dict[str, str]:
    return {k: v.decode(errors="replace") for k, v in (msg_headers or [])}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--error-type", choices=["validation", "transient_exhausted", "unexpected"])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--idle-seconds", type=float, default=5.0, help="stop after this much silence")
    args = p.parse_args(argv)

    s = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap_servers,
            "group.id": f"dlq-replay-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": s.kafka_bootstrap_servers, "acks": "all"})
    consumer.subscribe([s.topic_dlq])

    matched = 0
    last_msg = time.monotonic()
    try:
        while matched < args.limit and time.monotonic() - last_msg < args.idle_seconds:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"consumer error: {msg.error()}", file=sys.stderr)
                continue
            last_msg = time.monotonic()
            headers = _headers(msg.headers())
            if args.error_type and headers.get("x-error-type") != args.error_type:
                continue
            matched += 1
            summary = {
                "offset": msg.offset(),
                "error_type": headers.get("x-error-type"),
                "error": headers.get("x-error-message"),
                "key": msg.key().decode() if msg.key() else None,
            }
            print(json.dumps(summary))
            if not args.dry_run:
                producer.produce(s.topic_raw, value=msg.value(), key=msg.key())
                producer.poll(0)
    finally:
        producer.flush(30)
        consumer.close()
    verb = "would replay" if args.dry_run else "replayed"
    print(f"{verb} {matched} message(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
