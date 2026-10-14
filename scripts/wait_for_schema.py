"""Block until the Alembic migration has created the schema (used by Kubernetes initContainers)."""

import os
import sys
import time

from sqlalchemy import create_engine, text


def main() -> int:
    engine = create_engine(os.environ["DATABASE_URL"], connect_args={"connect_timeout": 5})
    deadline = time.monotonic() + float(os.environ.get("WAIT_TIMEOUT_SECONDS", "300"))
    while time.monotonic() < deadline:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM alerts LIMIT 1"))
            print("schema ready")
            return 0
        except Exception as exc:
            print(f"waiting for schema: {type(exc).__name__}", flush=True)
            time.sleep(3)
    print("timed out waiting for schema", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
