"""Integration fixtures: real Postgres, Redis and Kafka via testcontainers (needs Docker)."""

import os
import threading
import time
import uuid
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
import redis
from alembic import command
from alembic.config import Config
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import text
from sqlalchemy.engine import Engine

from txnpipe.common.config import Settings
from txnpipe.common.db import make_engine
from txnpipe.consumer.dlq import KafkaDlqPublisher
from txnpipe.consumer.handler import Handler
from txnpipe.consumer.main import ConsumerLoop
from txnpipe.consumer.repository import PgRepository
from txnpipe.consumer.retry import RetryPolicy
from txnpipe.consumer.rules import build_rules
from txnpipe.consumer.state import RedisState

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16", driver="psycopg") as pg:
        url = pg.get_connection_url()
        os.environ["DATABASE_URL"] = url
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("script_location", str(ROOT / "migrations"))
        command.upgrade(cfg, "head")
        yield url


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    from testcontainers.redis import RedisContainer

    with RedisContainer("redis:7") as r:
        yield f"redis://{r.get_container_host_ip()}:{r.get_exposed_port(6379)}/0"


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from testcontainers.kafka import KafkaContainer

        with KafkaContainer().with_kraft() as k:
            yield k.get_bootstrap_server()


@pytest.fixture(scope="session")
def engine(pg_url: str) -> Iterator[Engine]:
    eng = make_engine(pg_url)
    yield eng
    eng.dispose()


@pytest.fixture
def redis_client(redis_url: str) -> redis.Redis:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    client.flushall()
    return client


@pytest.fixture(autouse=True)
def clean_db(request: pytest.FixtureRequest) -> None:
    if "engine" in request.fixturenames:
        eng: Engine = request.getfixturevalue("engine")
        with eng.begin() as conn:
            conn.execute(text("TRUNCATE alerts, transactions RESTART IDENTITY CASCADE"))


@pytest.fixture
def settings(pg_url: str, redis_url: str, kafka_bootstrap: str) -> Settings:
    suffix = uuid.uuid4().hex[:8]
    s = Settings(
        kafka_bootstrap_servers=kafka_bootstrap,
        topic_raw=f"raw.{suffix}",
        topic_dlq=f"dlq.{suffix}",
        consumer_group=f"g.{suffix}",
        database_url=pg_url,
        redis_url=redis_url,
        batch_timeout_ms=100,
        retry_base_seconds=0.01,
        retry_max_seconds=0.05,
    )
    redis.Redis.from_url(redis_url).flushall()  # rule state must not leak between tests
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    futures = admin.create_topics(
        [NewTopic(s.topic_raw, 3, 1), NewTopic(s.topic_dlq, 1, 1)], operation_timeout=30
    )
    for f in futures.values():
        f.result(30)
    return s


@pytest.fixture
def producer(settings: Settings) -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})


def publish(producer: Producer, settings: Settings, key: str, value: bytes) -> None:
    producer.produce(settings.topic_raw, value=value, key=key)
    producer.flush(10)


class Pipeline:
    """Runs the real consumer loop (Postgres + Redis + Kafka) in a background thread."""

    def __init__(self, settings: Settings, batch_size: int = 1, commit: bool = True) -> None:
        self.settings = settings
        self.engine = make_engine(settings.database_url)
        self.redis = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        handler = Handler(
            repo=PgRepository(self.engine),
            state=RedisState(self.redis, settings.velocity_window_seconds),
            dlq=KafkaDlqPublisher(
                Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"}),
                settings.topic_dlq,
            ),
            rules=build_rules(settings),
            policy=RetryPolicy(0.01, 0.05, 3),
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        self.loop = ConsumerLoop(self.consumer, handler, batch_size, 0.2)
        if not commit:  # simulate a crash between DB commit and offset commit
            self.loop.commit_pending = lambda: None  # type: ignore[method-assign]
        self.consumer.subscribe([settings.topic_raw])
        self.thread = threading.Thread(target=self.loop.run, daemon=True)

    def __enter__(self) -> "Pipeline":
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.loop.stop()
        self.thread.join(30)
        self.consumer.close()
        self.engine.dispose()


def wait_for(check: Callable[[], bool], timeout: float = 60.0, interval: float = 0.2) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(interval)
    raise AssertionError("condition not met in time")
