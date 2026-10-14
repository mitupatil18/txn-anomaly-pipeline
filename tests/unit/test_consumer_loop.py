from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from tests.unit.fakes import FakeDlq, FakeRepo, FakeState
from tests.unit.helpers import payload

from txnpipe.common.config import Settings
from txnpipe.consumer.dlq import KafkaDlqPublisher
from txnpipe.consumer.handler import Handler
from txnpipe.consumer.main import ConsumerLoop
from txnpipe.consumer.records import Record
from txnpipe.consumer.retry import RetryPolicy
from txnpipe.consumer.rules import build_rules


class FakeMsg:
    def __init__(self, offset: int, value: bytes, partition: int = 0, err: Any = None) -> None:
        self._o, self._v, self._p, self._e = offset, value, partition, err

    def error(self) -> Any:
        return self._e

    def topic(self) -> str:
        return "transactions.raw"

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def key(self) -> bytes:
        return b"k"

    def value(self) -> bytes:
        return self._v


class FakeConsumer:
    def __init__(self, batches: list[list[FakeMsg]], loop_ref: list[ConsumerLoop]) -> None:
        self.batches, self.loop_ref = batches, loop_ref
        self.commits: list[list[tuple[int, int]]] = []
        self.events: list[str] = []
        self.fail_commit = False

    def consume(self, num_messages: int, timeout: float) -> list[FakeMsg]:
        if not self.batches:
            self.loop_ref[0].stop()
            return []
        return self.batches.pop(0)

    def commit(self, offsets: list[Any], asynchronous: bool) -> None:
        assert asynchronous is False
        if self.fail_commit:
            raise KafkaException(KafkaError(KafkaError._FAIL))
        self.events.append("commit")
        self.commits.append([(o.partition, o.offset) for o in offsets])


class RecordingHandler(Handler):
    def __init__(self, consumer: FakeConsumer) -> None:
        super().__init__(
            repo=FakeRepo(),
            state=FakeState(),
            dlq=FakeDlq(),
            rules=build_rules(Settings()),
            policy=RetryPolicy(),
        )
        self.consumer = consumer

    def process_batch(self, records: list[Record]) -> None:
        self.consumer.events.append("process")
        super().process_batch(records)


def _loop(batches: list[list[FakeMsg]]) -> tuple[ConsumerLoop, FakeConsumer]:
    ref: list[ConsumerLoop] = []
    consumer = FakeConsumer(batches, ref)
    loop = ConsumerLoop(consumer, RecordingHandler(consumer), 10, 0.01)
    ref.append(loop)
    return loop, consumer


def test_offsets_committed_after_processing_with_plus_one() -> None:
    msgs = [
        FakeMsg(i, payload(tid=f"00000000-0000-4000-8000-{i:012d}"), partition=i % 2)
        for i in range(4)
    ]
    loop, consumer = _loop([msgs])
    loop.run()
    assert consumer.events == ["process", "commit"]
    assert sorted(consumer.commits[0]) == [(0, 3), (1, 4)]


def test_invalid_message_still_advances_offset() -> None:
    loop, consumer = _loop([[FakeMsg(9, b"garbage")]])
    loop.run()
    assert consumer.commits == [[(0, 10)]]


def test_commit_not_attempted_when_processing_raises() -> None:
    loop, consumer = _loop([[FakeMsg(0, payload())]])

    def boom(records: list[Record]) -> None:
        raise RuntimeError("DLQ unavailable")

    loop.handler.process_batch = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        loop.run()
    assert consumer.commits == []


def test_partition_eof_and_errors_are_skipped() -> None:
    eof = KafkaError(KafkaError._PARTITION_EOF)
    transient = KafkaError(KafkaError._TRANSPORT)
    loop, consumer = _loop([[FakeMsg(0, b"", err=eof), FakeMsg(0, b"", err=transient)]])
    loop.run()
    assert consumer.commits == []


def test_commit_failure_is_kept_pending_and_retried_on_revoke() -> None:
    loop, consumer = _loop([[FakeMsg(4, payload())]])
    consumer.fail_commit = True
    loop.run()
    assert consumer.commits == []
    consumer.fail_commit = False
    loop.on_revoke(consumer, [])
    assert consumer.commits == [[(0, 5)]]


def test_on_lost_drops_pending() -> None:
    loop, consumer = _loop([])
    loop.mark([Record("t", 0, 3, None, None)])
    loop.on_lost(consumer, [])
    loop.on_assign(consumer, [])
    loop.commit_pending()
    assert consumer.commits == []


def test_stop_sets_flag() -> None:
    loop, _ = _loop([])
    loop.stop()
    assert loop.running is False


class FakeProducer:
    def __init__(self, fail: bool = False, remaining: int = 0) -> None:
        self.fail_flag, self.remaining_n = fail, remaining
        self.produced: list[dict[str, Any]] = []

    def produce(self, topic: str, **kw: Any) -> None:
        self.produced.append({"topic": topic, **kw})
        if not self.fail_flag:
            kw["on_delivery"](None, None)
        else:
            kw["on_delivery"]("boom", None)

    def flush(self, timeout: float) -> int:
        return self.remaining_n


def test_kafka_dlq_publisher_sets_headers() -> None:
    producer = FakeProducer()
    KafkaDlqPublisher(producer, "dlq").publish(  # type: ignore[arg-type]
        Record("transactions.raw", 3, 42, b"k", b"bad"), "validation", "oops", 0
    )
    sent = producer.produced[0]
    headers = dict(sent["headers"])
    assert sent["topic"] == "dlq" and sent["value"] == b"bad" and sent["key"] == b"k"
    assert headers["x-original-partition"] == b"3" and headers["x-original-offset"] == b"42"
    assert headers["x-error-type"] == b"validation" and headers["x-attempts"] == b"0"
    assert {"x-original-topic", "x-error-message", "x-failed-at"} <= set(headers)


@pytest.mark.parametrize("kwargs", [{"fail": True}, {"remaining": 1}])
def test_kafka_dlq_publisher_raises_when_not_delivered(kwargs: dict[str, Any]) -> None:
    with pytest.raises(RuntimeError):
        KafkaDlqPublisher(FakeProducer(**kwargs), "dlq").publish(  # type: ignore[arg-type]
            Record("t", 0, 0, None, b"x"), "validation", "m", 0
        )
