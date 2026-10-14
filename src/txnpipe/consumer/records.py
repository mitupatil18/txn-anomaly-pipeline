from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    """A Kafka message, decoupled from the client library so the handler is easy to test."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
