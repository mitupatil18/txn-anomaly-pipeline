"""Engine factory and table definitions (SQLAlchemy Core)."""

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Engine

metadata = MetaData()

transactions = Table(
    "transactions",
    metadata,
    Column("transaction_id", UUID(as_uuid=False), primary_key=True),
    Column("account_id", Text, nullable=False),
    Column("merchant_id", Text, nullable=False),
    Column("amount", Numeric(14, 2), nullable=False),
    Column("currency", CHAR(3), nullable=False),
    Column("country", CHAR(2), nullable=False),
    Column("channel", Text, nullable=False),
    Column("event_time", DateTime(timezone=True), nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("kafka_partition", Integer, nullable=False),
    Column("kafka_offset", BigInteger, nullable=False),
    Column("is_flagged", Boolean, nullable=False, server_default=text("false")),
    Index("idx_txn_account_time", "account_id", text("event_time DESC")),
    Index("idx_txn_flagged", "is_flagged", postgresql_where=text("is_flagged")),
)

alerts = Table(
    "alerts",
    metadata,
    Column("alert_id", BigInteger, primary_key=True, autoincrement=True),
    Column(
        "transaction_id",
        UUID(as_uuid=False),
        ForeignKey("transactions.transaction_id"),
        nullable=False,
    ),
    Column("account_id", Text, nullable=False),
    Column("rule", Text, nullable=False),
    Column("severity", Text, nullable=False),
    Column("details", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("transaction_id", "rule"),
    Index("idx_alert_account_time", "account_id", text("created_at DESC")),
    Index("idx_alert_rule_time", "rule", text("created_at DESC")),
)


def make_engine(url: str, *, pool_size: int = 5) -> Engine:
    return create_engine(
        url,
        pool_size=pool_size,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )
