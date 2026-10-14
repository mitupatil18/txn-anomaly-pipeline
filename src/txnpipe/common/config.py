"""Application settings, read from environment variables."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    kafka_bootstrap_servers: str = "kafka:9092"
    topic_raw: str = "transactions.raw"
    topic_dlq: str = "transactions.dlq"
    consumer_group: str = "txn-processors"
    batch_size: int = 1
    batch_timeout_ms: int = 200

    database_url: str = "postgresql+psycopg://txn:txn@postgres:5432/txn"
    redis_url: str = "redis://redis:6379/0"

    velocity_window_seconds: int = 60
    velocity_limit: int = 5
    spike_multiplier: float = 5.0
    spike_min_amount: float = 1000.0
    spike_min_history: int = 5
    geo_min_gap_minutes: int = 10

    retry_base_seconds: float = 0.2
    retry_max_seconds: float = 5.0
    retry_max_attempts: int = 5

    consumer_metrics_port: int = 8001
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
