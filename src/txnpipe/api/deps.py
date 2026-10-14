from collections.abc import Iterator

from confluent_kafka import Producer
from fastapi import Request
from sqlalchemy.engine import Connection


def get_conn(request: Request) -> Iterator[Connection]:
    with request.app.state.engine.connect() as conn:
        yield conn


def get_producer(request: Request) -> Producer:
    app = request.app
    if app.state.producer is None:
        from txnpipe.common.kafka import make_producer

        app.state.producer = make_producer(app.state.settings, linger_ms=0)
    producer: Producer = app.state.producer
    return producer
