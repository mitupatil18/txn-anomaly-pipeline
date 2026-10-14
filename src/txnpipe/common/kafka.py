"""Kafka client configuration helpers."""

from typing import Any

from confluent_kafka import Consumer, Producer

from txnpipe.common.config import Settings


def consumer_config(s: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": s.kafka_bootstrap_servers,
        "group.id": s.consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
        "partition.assignment.strategy": "cooperative-sticky",
    }


def producer_config(s: Settings, *, linger_ms: int = 5) -> dict[str, Any]:
    return {
        "bootstrap.servers": s.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": linger_ms,
        "compression.type": "lz4",
    }


def make_consumer(s: Settings) -> Consumer:
    return Consumer(consumer_config(s))


def make_producer(s: Settings, *, linger_ms: int = 5) -> Producer:
    return Producer(producer_config(s, linger_ms=linger_ms))
