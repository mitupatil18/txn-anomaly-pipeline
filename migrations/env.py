import os

from alembic import context
from sqlalchemy import create_engine

config = context.config
target_metadata = None


def _url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql+psycopg://txn:txn@postgres:5432/txn")


def run_migrations_offline() -> None:
    context.configure(url=_url(), literal_binds=True, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url())
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
