"""initial schema: transactions and alerts

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE transactions (
          transaction_id  UUID PRIMARY KEY,
          account_id      TEXT        NOT NULL,
          merchant_id     TEXT        NOT NULL,
          amount          NUMERIC(14,2) NOT NULL,
          currency        CHAR(3)     NOT NULL,
          country         CHAR(2)     NOT NULL,
          channel         TEXT        NOT NULL,
          event_time      TIMESTAMPTZ NOT NULL,
          processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
          kafka_partition INT         NOT NULL,
          kafka_offset    BIGINT      NOT NULL,
          is_flagged      BOOLEAN     NOT NULL DEFAULT FALSE
        )
        """
    )
    op.execute("CREATE INDEX idx_txn_account_time ON transactions (account_id, event_time DESC)")
    op.execute("CREATE INDEX idx_txn_flagged ON transactions (is_flagged) WHERE is_flagged")
    op.execute(
        """
        CREATE TABLE alerts (
          alert_id       BIGSERIAL PRIMARY KEY,
          transaction_id UUID        NOT NULL REFERENCES transactions(transaction_id),
          account_id     TEXT        NOT NULL,
          rule           TEXT        NOT NULL,
          severity       TEXT        NOT NULL,
          details        JSONB       NOT NULL,
          created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (transaction_id, rule)
        )
        """
    )
    op.execute("CREATE INDEX idx_alert_account_time ON alerts (account_id, created_at DESC)")
    op.execute("CREATE INDEX idx_alert_rule_time ON alerts (rule, created_at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE alerts")
    op.execute("DROP TABLE transactions")
