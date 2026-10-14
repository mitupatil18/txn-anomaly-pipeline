"""Consumer entry point: poll loop, rebalance callbacks, graceful shutdown."""

import logging
import signal
import sys
from typing import Any

import redis
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from prometheus_client import start_http_server

from txnpipe.common.config import Settings, get_settings
from txnpipe.common.db import make_engine
from txnpipe.common.kafka import consumer_config, make_producer
from txnpipe.common.logging import setup_logging
from txnpipe.consumer.dlq import KafkaDlqPublisher
from txnpipe.consumer.handler import Handler, Record
from txnpipe.consumer.repository import PgRepository
from txnpipe.consumer.retry import RetryPolicy
from txnpipe.consumer.rules import build_rules
from txnpipe.consumer.state import RedisState

log = logging.getLogger("txnpipe.consumer")


class ConsumerLoop:
    def __init__(self, consumer: Any, handler: Handler, batch_size: int, timeout_s: float) -> None:
        self.consumer = consumer
        self.handler = handler
        self.batch_size = max(1, batch_size)
        self.timeout_s = timeout_s
        self.running = True
        self._uncommitted: dict[tuple[str, int], int] = {}

    def stop(self, *_: object) -> None:
        log.info("shutdown requested")
        self.running = False

    # -- rebalance callbacks -------------------------------------------------
    def on_assign(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        log.info("partitions assigned", extra={"partitions": [p.partition for p in partitions]})

    def on_revoke(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        log.info("partitions revoked", extra={"partitions": [p.partition for p in partitions]})
        self.commit_pending()

    def on_lost(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        log.warning("partitions lost", extra={"partitions": [p.partition for p in partitions]})
        self._uncommitted.clear()  # cannot commit what we no longer own; redelivery is idempotent

    # -- loop ------------------------------------------------------------------
    def run(self) -> None:
        while self.running:
            msgs = self.consumer.consume(num_messages=self.batch_size, timeout=self.timeout_s)
            records = self._to_records(msgs)
            if not records:
                continue
            self.handler.process_batch(records)
            self.mark(records)
            self.commit_pending()

    def _to_records(self, msgs: list[Any]) -> list[Record]:
        records: list[Record] = []
        for msg in msgs:
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                if err.fatal():
                    raise KafkaException(err)
                log.warning("consumer error", extra={"error": str(err)})
                continue
            records.append(
                Record(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value())
            )
        return records

    def mark(self, records: list[Record]) -> None:
        for r in records:
            key = (r.topic, r.partition)
            self._uncommitted[key] = max(self._uncommitted.get(key, -1), r.offset + 1)

    def commit_pending(self) -> None:
        if not self._uncommitted:
            return
        offsets = [TopicPartition(t, p, o) for (t, p), o in self._uncommitted.items()]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException:
            # Safe: uncommitted messages are redelivered and deduplicated by transaction_id.
            log.exception("offset commit failed; will retry on next batch")
            return
        self._uncommitted.clear()


def main() -> int:
    settings: Settings = get_settings()
    setup_logging(settings.log_level)
    start_http_server(settings.consumer_metrics_port)

    engine = make_engine(settings.database_url)
    state = RedisState(
        redis.Redis.from_url(settings.redis_url, socket_timeout=2, decode_responses=True),
        settings.velocity_window_seconds,
    )
    handler = Handler(
        repo=PgRepository(engine),
        state=state,
        dlq=KafkaDlqPublisher(make_producer(settings, linger_ms=0), settings.topic_dlq),
        rules=build_rules(settings),
        policy=RetryPolicy(
            settings.retry_base_seconds, settings.retry_max_seconds, settings.retry_max_attempts
        ),
        window_seconds=settings.velocity_window_seconds,
    )
    consumer = Consumer(consumer_config(settings))
    loop = ConsumerLoop(
        consumer, handler, settings.batch_size, max(settings.batch_timeout_ms, 50) / 1000
    )
    signal.signal(signal.SIGTERM, loop.stop)
    signal.signal(signal.SIGINT, loop.stop)
    consumer.subscribe(
        [settings.topic_raw],
        on_assign=loop.on_assign,
        on_revoke=loop.on_revoke,
        on_lost=loop.on_lost,
    )
    log.info(
        "consumer started",
        extra={"group": settings.consumer_group, "batch_size": settings.batch_size},
    )
    try:
        loop.run()
    finally:
        loop.commit_pending()
        consumer.close()  # leaves the group cleanly
        engine.dispose()
        log.info("consumer stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
