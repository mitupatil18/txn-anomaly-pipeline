"""Dead-letter queue publisher."""

from datetime import UTC, datetime
from typing import Protocol

from confluent_kafka import Producer

from txnpipe.common.metrics import TXN_DLQ
from txnpipe.consumer.records import Record


class DlqPublisher(Protocol):
    def publish(
        self, record: Record, error_type: str, error_message: str, attempts: int
    ) -> None: ...


class KafkaDlqPublisher:
    """Publishes the original bytes plus diagnostic headers, and waits for the broker ack."""

    def __init__(self, producer: Producer, topic: str, flush_timeout: float = 30.0) -> None:
        self.producer = producer
        self.topic = topic
        self.flush_timeout = flush_timeout

    def publish(self, record: Record, error_type: str, error_message: str, attempts: int) -> None:
        errors: list[object] = []

        def on_delivery(err: object, _msg: object) -> None:
            if err is not None:
                errors.append(err)

        headers: list[tuple[str, str | bytes | None]] = [
            ("x-original-topic", record.topic.encode()),
            ("x-original-partition", str(record.partition).encode()),
            ("x-original-offset", str(record.offset).encode()),
            ("x-error-type", error_type.encode()),
            ("x-error-message", " ".join(error_message.split())[:1000].encode()),
            ("x-attempts", str(attempts).encode()),
            ("x-failed-at", datetime.now(UTC).isoformat().encode()),
        ]
        self.producer.produce(
            self.topic,
            value=record.value,
            key=record.key,
            headers=headers,
            on_delivery=on_delivery,
        )
        remaining = self.producer.flush(self.flush_timeout)
        if remaining or errors:
            raise RuntimeError(f"DLQ publish failed: remaining={remaining} errors={errors}")
        TXN_DLQ.labels(error_type=error_type).inc()
