# Real-Time Transaction Anomaly Pipeline: Implementation Spec

> **How to use this file:** give it to an AI coding agent (Claude Code, Copilot agent, etc.) as the full brief. The agent should build the project in the phases listed in section 14, committing after each phase, and stop to ask if anything below is ambiguous or contradictory.

---

## 0. Instructions for the AI agent (read first)

1. Build in the phase order of section 14. After each phase, run the verification commands listed and fix failures before moving on. Commit after each phase with a clear message.
2. Use the exact stack, names, topic names, table names and endpoints in this spec. If a choice is not specified, pick the simplest option that works and note it in `docs/DESIGN.md`.
3. **Never fabricate benchmark numbers.** `docs/BENCHMARKS.md` must contain only numbers produced by actually running `scripts/benchmark.py`. If the benchmark has not been run, leave the table with `TBD` and say how to run it.
4. Keep everything runnable with `docker compose up`. No cloud account, no paid service, no real secrets. Everything configurable through environment variables with defaults in `.env.example`.
5. Write tests as you go (target at least 80% coverage on `src/`).
6. Python style: type hints everywhere, `ruff` for lint and format, small functions, structured JSON logging, no `print`.
7. This is an original portfolio project. Do not include any code, naming or data from any employer.
8. When a library API is uncertain, check the installed version's documentation instead of guessing.

---

## 1. Goal and scope

**One-line:** an event-driven pipeline that ingests simulated payment transactions from Kafka, evaluates them against fraud-style rules, stores the results in PostgreSQL, and exposes them through a REST API with full observability.

**What this project must demonstrate** (the reason it exists, because it will be shown to interviewers):
- Event-driven design with Kafka: partitioning and keying, consumer groups, offset management, rebalancing.
- Reliability: idempotent processing, retries with exponential backoff, a dead-letter queue (DLQ), graceful shutdown.
- Performance engineering: measured throughput and latency before and after an optimization, using real benchmark runs.
- Observability: metrics, consumer lag, a Grafana dashboard.
- Engineering hygiene: tests, CI, Docker, clear documentation of trade-offs.

**Out of scope:** real payment integrations, authentication and authorization (document as a limitation), ML models, multi-region setups.

---

## 2. Tech stack

| Concern | Choice |
|---|---|
| Language | Python 3.11+ |
| Kafka client | `confluent-kafka` (librdkafka based) |
| Broker | Apache Kafka 3.7 in KRaft mode (no ZooKeeper), image `apache/kafka:3.7.0` |
| API | FastAPI + Uvicorn |
| Validation | Pydantic v2 |
| Database | PostgreSQL 16 |
| DB access | SQLAlchemy 2.0 (Core style) with the `psycopg` (v3) driver, sync |
| Migrations | Alembic |
| Rule state cache | Redis 7 |
| Metrics | `prometheus-client`, Prometheus, Grafana, `kafka-exporter` for lag |
| Tests | pytest, pytest-cov, testcontainers-python (integration) |
| Lint and types | ruff, mypy (non-strict) |
| Packaging | `pyproject.toml`, one Dockerfile shared by all Python services |
| CI | GitHub Actions |
| Orchestration | Docker Compose (required); Kubernetes manifests (optional stretch) |

---

## 3. Architecture

```
                  +------------------+
                  |   simulator      |  (synthetic traffic + injected fraud patterns)
                  +--------+---------+
                           | produce (key = account_id)
                           v
                  +------------------+
                  | Kafka            |
                  |  transactions.raw|  6 partitions
                  |  transactions.dlq|  1 partition
                  +--------+---------+
                           | consumer group: txn-processors
              +------------+-------------+
              v            v             v
         consumer-1    consumer-2    consumer-N      (horizontally scalable)
              |   validate -> dedupe -> evaluate rules -> persist -> commit
              |          |                    |
              |          v                    v
              |        Redis             PostgreSQL
              |   (windowed rule state)  (transactions, alerts)
              |
              +--> on repeated failure or poison message --> transactions.dlq

         +--------+          reads
         |  API   | <-------------------- PostgreSQL
         +--------+
              |  /metrics (every service)
              v
         Prometheus <-- kafka-exporter (consumer lag)
              |
              v
          Grafana (dashboard)
```

### Key design decisions (record and justify each in `docs/DESIGN.md`)

1. **Partition key = `account_id`.** All events of one account go to the same partition, so per-account ordering is preserved and per-account rule state (velocity, last country) is consistent without cross-consumer coordination.
2. **At-least-once delivery + idempotent sink = effectively-once results in the database.** Offsets are committed manually, only after the database transaction commits. A redelivered message is detected through the unique `transaction_id` and skipped.
3. **Rule state lives in Redis, not in consumer memory.** After a rebalance or restart, another consumer can continue with correct state. Trade-off: Redis state is updated after the DB commit, so a crash between the two can drop one event from the window (documented, acceptable for this use case).
4. **Retries are in-process with exponential backoff and jitter** for transient failures only (database or Redis errors). Poison messages (invalid schema) go straight to the DLQ without retrying.
5. **DLQ is a Kafka topic** carrying the original payload plus diagnostic headers, with a replay script.
6. **Batch processing is a configuration option** (`BATCH_SIZE`). The benchmark compares per-message processing against batched DB writes.

---

## 4. Repository layout

```
txn-anomaly-pipeline/
├── README.md
├── PROJECT_SPEC.md                 # this file (keep for reference)
├── Makefile
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── pyproject.toml
├── alembic.ini
├── migrations/
│   └── versions/0001_initial.py
├── src/txnpipe/
│   ├── __init__.py
│   ├── common/
│   │   ├── config.py               # pydantic-settings, all env vars
│   │   ├── logging.py              # JSON logging setup
│   │   ├── schemas.py              # TransactionEvent and other models
│   │   ├── metrics.py              # shared Prometheus metric definitions
│   │   ├── kafka.py                # producer/consumer factory helpers
│   │   └── db.py                   # engine, table definitions
│   ├── producer/
│   │   └── simulator.py            # CLI traffic generator
│   ├── consumer/
│   │   ├── main.py                 # poll loop, rebalance callbacks, shutdown
│   │   ├── handler.py              # process one message / one batch
│   │   ├── repository.py           # DB writes (idempotent)
│   │   ├── state.py                # Redis-backed rule state
│   │   ├── retry.py                # backoff + jitter helper
│   │   ├── dlq.py                  # publish to DLQ with headers
│   │   └── rules/
│   │       ├── base.py             # Rule protocol, RuleResult
│   │       ├── velocity.py
│   │       ├── amount_spike.py
│   │       └── geo_mismatch.py
│   └── api/
│       ├── main.py                 # FastAPI app factory, /metrics, /health
│       ├── deps.py
│       ├── schemas.py              # response models
│       └── routes/
│           ├── alerts.py
│           ├── transactions.py
│           └── accounts.py
├── scripts/
│   ├── create_topics.sh
│   ├── benchmark.py
│   └── replay_dlq.py
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       ├── provisioning/datasources/prometheus.yml
│       ├── provisioning/dashboards/dashboards.yml
│       └── dashboards/txn-pipeline.json
├── tests/
│   ├── unit/
│   └── integration/
├── docs/
│   ├── DESIGN.md
│   ├── BENCHMARKS.md
│   └── images/                     # architecture diagram, dashboard screenshot
├── deploy/k8s/                     # optional stretch
└── .github/workflows/ci.yml
```

---

## 5. Data contracts

### 5.1 Kafka topics

| Topic | Partitions | Replication | Key | Notes |
|---|---|---|---|---|
| `transactions.raw` | 6 | 1 | `account_id` (string) | retention 24h |
| `transactions.dlq` | 1 | 1 | original key | retention 7d |

Created by the `kafka-init` one-shot service in Compose through `scripts/create_topics.sh` (use `kafka-topics.sh --if-not-exists`).

### 5.2 Event schema (`TransactionEvent`, JSON, UTF-8)

| Field | Type | Rules |
|---|---|---|
| `transaction_id` | string (UUID) | required, unique per transaction |
| `account_id` | string | required, 1-64 chars |
| `merchant_id` | string | required |
| `amount` | decimal string or number | required, > 0, max 2 decimal places |
| `currency` | string | ISO 4217, 3 letters, default `USD` |
| `country` | string | ISO 3166-1 alpha-2 |
| `channel` | enum | `online`, `pos`, `atm` |
| `timestamp` | string | ISO 8601 UTC, event time |

Example:
```json
{"transaction_id":"7b1f4a52-9a1e-4b6a-8e53-2f2d1c9a0a11","account_id":"acc_0042","merchant_id":"m_981","amount":"129.99","currency":"USD","country":"IN","channel":"online","timestamp":"2026-10-15T10:21:33Z"}
```

### 5.3 DLQ message

Same payload bytes as the original message, plus Kafka headers:
`x-original-topic`, `x-original-partition`, `x-original-offset`, `x-error-type` (`validation` | `transient_exhausted` | `unexpected`), `x-error-message`, `x-attempts`, `x-failed-at` (ISO 8601).

### 5.4 PostgreSQL schema (create through Alembic migration `0001_initial`)

```sql
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
);
CREATE INDEX idx_txn_account_time ON transactions (account_id, event_time DESC);
CREATE INDEX idx_txn_flagged      ON transactions (is_flagged) WHERE is_flagged;

CREATE TABLE alerts (
  alert_id       BIGSERIAL PRIMARY KEY,
  transaction_id UUID        NOT NULL REFERENCES transactions(transaction_id),
  account_id     TEXT        NOT NULL,
  rule           TEXT        NOT NULL,      -- VELOCITY | AMOUNT_SPIKE | GEO_MISMATCH
  severity       TEXT        NOT NULL,      -- LOW | MEDIUM | HIGH
  details        JSONB       NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (transaction_id, rule)             -- makes alert writes idempotent
);
CREATE INDEX idx_alert_account_time ON alerts (account_id, created_at DESC);
CREATE INDEX idx_alert_rule_time    ON alerts (rule, created_at DESC);
```

### 5.5 Redis keys (rule state)

| Key | Type | Content | TTL |
|---|---|---|---|
| `vel:{account_id}` | ZSET | member = `transaction_id`, score = event epoch seconds | window + 60s |
| `amt:{account_id}` | HASH | `count`, `ewma` (exponentially weighted mean amount) | 7d |
| `geo:{account_id}` | HASH | `country`, `ts` (last seen) | 1d |

---

## 6. Rules engine

Each rule implements `evaluate(event, state_reader) -> RuleResult | None` and is **read-only**. State is written afterwards by `state.update(event)`, only after the DB commit (see decision 3). Rules are registered in a list so new ones can be added without touching the handler. Thresholds come from config (defaults below).

| Rule | Logic | Default thresholds | Severity |
|---|---|---|---|
| `VELOCITY` | Count of the account's transactions in the last `VELOCITY_WINDOW_SECONDS` (including the current one) exceeds the limit. | window 60s, limit 5 | MEDIUM, or HIGH if count > 2x limit |
| `AMOUNT_SPIKE` | Amount > `SPIKE_MULTIPLIER` x the account's EWMA, and amount >= `SPIKE_MIN_AMOUNT`, and at least `SPIKE_MIN_HISTORY` prior transactions exist. | multiplier 5, min amount 1000, min history 5 | MEDIUM, or HIGH if multiplier > 10 |
| `GEO_MISMATCH` | Country differs from the previous transaction's country, and the time gap is less than `GEO_MIN_GAP_MINUTES` ("impossible travel"). | gap 10 minutes | HIGH |

`RuleResult` carries `rule`, `severity`, and a `details` dict (observed values and thresholds) that is stored in `alerts.details`.

Late and out-of-order events: use event time for windows; ignore events older than the window for velocity; document that strict out-of-order handling is a known limitation.

---

## 7. Consumer behavior (the core of the project)

### 7.1 Consumer configuration

```
group.id               = txn-processors
enable.auto.commit     = false
auto.offset.reset      = earliest
isolation.level        = read_committed
max.poll.interval.ms   = 300000
session.timeout.ms     = 45000
partition.assignment.strategy = cooperative-sticky
```

### 7.2 Processing algorithm per message (batch mode applies the same steps to a list, using one DB transaction per batch)

1. **Parse and validate** JSON against `TransactionEvent`. On failure: publish to DLQ (`validation`) with no retry, count as processed, continue.
2. **Open DB transaction.**
3. **Idempotent insert:** `INSERT INTO transactions ... ON CONFLICT (transaction_id) DO NOTHING`. If zero rows were inserted, this is a duplicate redelivery: rollback or skip, do not evaluate rules, go to step 8.
4. **Evaluate all rules** (read-only against Redis).
5. **Insert alerts** with `ON CONFLICT (transaction_id, rule) DO NOTHING` and set `is_flagged` if any alert was produced.
6. **Commit DB transaction.**
7. **Update Redis state** for this event (best effort, log on failure, increment a metric).
8. **Mark offset for commit.** Commit offsets synchronously after the whole batch (or each message when `BATCH_SIZE=1`).

### 7.3 Retry and DLQ

- Retry only transient errors (DB connection errors, Redis connection errors, timeouts): exponential backoff with full jitter, `RETRY_BASE_SECONDS=0.2`, `RETRY_MAX_SECONDS=5`, `RETRY_MAX_ATTEMPTS=5`.
- After attempts are exhausted, or on an unexpected exception: publish to `transactions.dlq` with headers (5.3), then commit the offset so the partition does not block.
- Provide `scripts/replay_dlq.py`: reads `transactions.dlq`, republishes selected messages to `transactions.raw` (flags: `--limit`, `--error-type`, `--dry-run`).

### 7.4 Lifecycle

- Handle `SIGTERM` and `SIGINT`: stop polling, finish the current batch, commit offsets, `consumer.close()` (leaves the group cleanly).
- Rebalance callbacks: on partitions revoked, commit pending offsets; log assignment and revocation with partition numbers.
- Liveness: the consumer exposes `/metrics` on `CONSUMER_METRICS_PORT` (default 8001) via `prometheus_client.start_http_server`. Docker healthcheck confirms the port responds.

### 7.5 Metrics (Prometheus)

| Metric | Type | Labels |
|---|---|---|
| `txn_processed_total` | counter | `result` = `ok`, `duplicate`, `dlq` |
| `txn_alerts_total` | counter | `rule`, `severity` |
| `txn_processing_seconds` | histogram | none (per message or per batch divided by size) |
| `txn_end_to_end_latency_seconds` | histogram | none (processed time minus event timestamp) |
| `txn_retries_total` | counter | `error_type` |
| `txn_dlq_total` | counter | `error_type` |
| `txn_state_update_failures_total` | counter | none |
| `txn_batch_size` | histogram | none |

Consumer lag comes from `kafka-exporter` (`kafka_consumergroup_lag`), scraped by Prometheus.

---

## 8. Traffic simulator (`txnpipe.producer.simulator`)

CLI: `python -m txnpipe.producer.simulator --rate 500 --duration 60 --accounts 2000 --fraud-ratio 0.02 --seed 42`

- Generates realistic transactions: per-account home country, log-normal amounts, random merchants and channels.
- Injects fraud patterns at `--fraud-ratio` of accounts: bursts (velocity), a single very large amount (spike), and a transaction from a distant country seconds after one at home (geo).
- Rate limiting with a token-bucket or sleep-based pacing; reports achieved send rate at the end.
- Producer config: `acks=all`, `enable.idempotence=true`, `linger.ms=5`, `compression.type=lz4`. Key = `account_id`.
- `--invalid-ratio` option injects malformed messages to exercise the DLQ.
- Deterministic with `--seed`. Also expose a mode `--count N` (send exactly N messages and exit) used by the benchmark.

---

## 9. REST API (`txnpipe.api`)

FastAPI, OpenAPI docs at `/docs`. Pagination is keyset-based (`limit` default 50, max 200; `cursor` is an opaque base64 of the last `(created_at, alert_id)`).

| Method | Path | Description |
|---|---|---|
| GET | `/health` | liveness, returns `{"status":"ok"}` |
| GET | `/ready` | checks DB connectivity |
| GET | `/metrics` | Prometheus metrics |
| GET | `/alerts` | filters: `account_id`, `rule`, `severity`, `from`, `to`, `limit`, `cursor` |
| GET | `/alerts/{alert_id}` | single alert with the linked transaction |
| GET | `/transactions/{transaction_id}` | transaction with its alerts |
| GET | `/accounts/{account_id}/summary` | counts of transactions and alerts by rule, last seen time |
| GET | `/stats` | totals: transactions, flagged, alerts by rule, last 5 minutes rate |
| POST | `/transactions` | optional: validates a transaction and publishes to `transactions.raw` (202 Accepted); makes manual demos easy |

Return consistent error bodies (`{"error": {"code": "...", "message": "..."}}`). Use response models. Add request timing middleware and an `http_request_duration_seconds` histogram.

---

## 10. UI

- **Primary UI: the Grafana dashboard** (provisioned automatically, `monitoring/grafana/dashboards/txn-pipeline.json`). Panels:
  1. Messages processed per second (by `result`)
  2. Alerts per minute by rule
  3. Consumer lag per partition (`kafka_consumergroup_lag`)
  4. Processing latency p50/p95/p99 (`txn_processing_seconds`)
  5. End-to-end latency p95/p99
  6. Retries and DLQ rate
  7. API request rate and p95 latency
- **Optional (stretch):** a minimal server-rendered page at `/ui` in the API (Jinja2 + HTMX, no JS build) showing the latest 50 alerts with a filter by rule and auto-refresh every 5s. Only build it after all required phases are done.

---

## 11. Deployment

### 11.1 Docker Compose services

| Service | Image / build | Ports | Notes |
|---|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | 9092 (internal), 29092 (host) | KRaft single node, see config below |
| `kafka-init` | same image | none | runs `create_topics.sh` then exits |
| `postgres` | `postgres:16` | 5432 | volume for data, healthcheck `pg_isready` |
| `redis` | `redis:7` | 6379 | healthcheck `redis-cli ping` |
| `migrate` | build `.` | none | `alembic upgrade head`, runs once after postgres is healthy |
| `consumer` | build `.` | 8001 (metrics) | scalable: `docker compose up --scale consumer=3` (do not publish a fixed host port for scaled replicas) |
| `api` | build `.` | 8000 | depends on `migrate` |
| `simulator` | build `.` | none | profile `load`, run on demand: `docker compose --profile load run --rm simulator ...` |
| `kafka-exporter` | `danielqsj/kafka-exporter` | 9308 | `--kafka.server=kafka:9092` |
| `prometheus` | `prom/prometheus` | 9090 | scrapes api, consumer (service discovery via DNS for scaled replicas, or static targets with documented limits), kafka-exporter |
| `grafana` | `grafana/grafana` | 3000 | provisioned datasource and dashboard, anonymous viewer enabled for demo |

Kafka KRaft environment (verify against the image docs and adjust if the image version differs):
```yaml
KAFKA_NODE_ID: 1
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

Add healthchecks and `depends_on: condition: service_healthy` so `docker compose up` works from a cold start with no manual steps.

### 11.2 Dockerfile

Multi-stage, `python:3.11-slim`, install dependencies from `pyproject.toml`, run as a non-root user, no dev dependencies in the final image. Service selected by command (`python -m txnpipe.consumer.main`, `uvicorn txnpipe.api.main:app --host 0.0.0.0 --port 8000`).

### 11.3 Makefile targets

`make up`, `make down`, `make logs`, `make test`, `make lint`, `make migrate`, `make load` (run simulator with defaults), `make benchmark`, `make scale n=3`, `make replay-dlq`.

### 11.4 Optional stretch: Kubernetes

`deploy/k8s/` with Deployments for api and consumer (probes, resource requests and limits), a Service for api, ConfigMap and Secret for config, and an HPA for the consumer. Verified on a local `kind` cluster, documented in README. Mention that Kafka, Postgres and Redis are assumed to be provided externally or via Helm charts for the cluster demo.

---

## 12. Configuration (all via environment, defaults in `.env.example`)

```
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
TOPIC_RAW=transactions.raw
TOPIC_DLQ=transactions.dlq
CONSUMER_GROUP=txn-processors
BATCH_SIZE=1
BATCH_TIMEOUT_MS=200
DATABASE_URL=postgresql+psycopg://txn:txn@postgres:5432/txn
REDIS_URL=redis://redis:6379/0
VELOCITY_WINDOW_SECONDS=60
VELOCITY_LIMIT=5
SPIKE_MULTIPLIER=5
SPIKE_MIN_AMOUNT=1000
SPIKE_MIN_HISTORY=5
GEO_MIN_GAP_MINUTES=10
RETRY_BASE_SECONDS=0.2
RETRY_MAX_SECONDS=5
RETRY_MAX_ATTEMPTS=5
CONSUMER_METRICS_PORT=8001
LOG_LEVEL=INFO
```

---

## 13. Testing and quality

### 13.1 Unit tests (no external services)
- Each rule: below threshold, at threshold, above threshold, missing history, boundary of the time window.
- Event validation: valid, missing field, bad amount, bad country, bad timestamp.
- Retry helper: backoff growth, jitter bounds, cap, attempts limit (use an injectable clock/sleep).
- Handler logic with fakes for repository, state and DLQ publisher: duplicate skipped, invalid goes to DLQ, transient error retried then DLQ'd, offsets only committed after success.

### 13.2 Integration tests (testcontainers: Kafka, Postgres, Redis)
- End-to-end: publish events, assert transactions and alerts rows.
- Idempotency: publish the same `transaction_id` twice, assert one transaction row and one alert per rule.
- DLQ: publish a malformed message, assert it appears on `transactions.dlq` with the expected headers.
- Crash and redelivery: process a message, kill before offset commit, restart, assert no duplicate rows.
- API: filters, pagination cursors, 404 handling.

### 13.3 CI (`.github/workflows/ci.yml`)
On push and PR: set up Python, install, `ruff check`, `ruff format --check`, `mypy src`, unit tests with coverage (fail under 80%), then integration tests, then `docker build`. Status badge in README.

---

## 14. Phases (build order) with acceptance criteria

| Phase | Deliverable | Verify |
|---|---|---|
| 1. Skeleton | repo layout, `pyproject.toml`, config module, logging, Makefile, Dockerfile, CI with lint only | `make lint` passes, image builds |
| 2. Infra | Compose with kafka, kafka-init, postgres, redis, migrate; Alembic migration | `docker compose up -d` is healthy; topics exist (`kafka-topics.sh --list`); tables exist |
| 3. Core pipeline | schemas, simulator, consumer with validation, idempotent insert, rules, state, offset commit | simulator sends 1000 messages; DB shows 1000 transactions and some alerts; re-sending duplicates does not add rows |
| 4. Reliability | retry with backoff, DLQ, graceful shutdown, rebalance callbacks, replay script | invalid messages land in DLQ with headers; stopping a DB container produces retries then recovery; `docker compose stop consumer` exits cleanly |
| 5. API | all endpoints in section 9 | `/docs` works; filters and cursor pagination verified by tests |
| 6. Observability | metrics in all services, Prometheus, kafka-exporter, Grafana dashboard provisioned | dashboard shows live data during a simulator run; lag panel populated |
| 7. Tests and CI | unit and integration tests, coverage gate | CI green, coverage at least 80% |
| 8. Benchmark | `scripts/benchmark.py`, run experiments, `docs/BENCHMARKS.md` with real results | see section 15 |
| 9. Documentation | README, `docs/DESIGN.md`, architecture diagram, dashboard screenshot | see section 16 |
| 10. Stretch | `/ui` page, Kubernetes manifests, replay tooling polish | optional |

---

## 15. Benchmark plan (must use real measurements)

`scripts/benchmark.py` automates: reset topics and tables, start the consumer configuration under test, produce `N` messages (default 200000) at max rate with the simulator in `--count` mode, wait until consumer lag reaches 0, and report.

**Reported metrics per run:** total messages, wall-clock seconds, throughput (msg/s), end-to-end latency p50/p95/p99, error and DLQ counts, CPU count and machine description.

**Experiments:**

| ID | Configuration | Purpose |
|---|---|---|
| A | 1 consumer, `BATCH_SIZE=1` | baseline |
| B | 1 consumer, `BATCH_SIZE=200` (batched inserts and commits) | effect of batching |
| C | 3 consumers, best batch size | effect of horizontal scaling |
| D | 6 consumers, best batch size | scaling up to the partition count |
| E | 8 consumers (more consumers than partitions) | show idle consumers, explain why |
| F | Optional: 3 partitions vs 6 partitions | effect of partition count |

Each experiment is run at least 3 times; report the median. `docs/BENCHMARKS.md` includes the table, the test machine, the exact commands, and a short analysis of bottlenecks (for example whether Postgres or Redis became the limit). Be honest about local-machine limits; relative improvements are the point.

---

## 16. Documentation requirements

**README.md**
- One-paragraph description and the architecture diagram (Mermaid or image in `docs/images/`).
- Quick start: `cp .env.example .env && docker compose up -d`, then `make load`, then URLs (API docs, Grafana, Prometheus).
- Feature list and tech stack.
- Benchmark summary table (real numbers only) with a link to `docs/BENCHMARKS.md`.
- How to run tests, how to scale consumers, how to replay the DLQ.
- Limitations and future work (auth, schema registry and Avro, exactly-once transactions, state out-of-order handling, multi-broker setup).
- CI badge and a Grafana screenshot.

**docs/DESIGN.md** answers these questions explicitly (they are likely interview questions):
1. Why key by `account_id`, and what breaks if you key by `transaction_id`?
2. What delivery guarantee do you provide and how?
3. What happens if a consumer crashes mid-batch? After the DB commit but before the offset commit?
4. What happens on a rebalance?
5. Why is rule state in Redis and what are its failure modes?
6. How do retries and the DLQ work, and what is retried versus not?
7. How would you handle a hot account (one key producing most traffic)?
8. How would you handle schema evolution?
9. How would you achieve true exactly-once semantics, and what would it cost?
10. What would you change at 100x the traffic?

---

## 17. Definition of done

- [ ] `docker compose up -d` from a clean clone brings everything up healthy with no manual steps.
- [ ] `make load` produces alerts visible through the API and the Grafana dashboard.
- [ ] Duplicate and malformed messages are handled as specified (idempotent, DLQ with headers).
- [ ] Consumer shuts down cleanly on SIGTERM; no duplicate rows after restarts.
- [ ] CI is green: lint, types, tests with at least 80% coverage, image build.
- [ ] `docs/BENCHMARKS.md` contains real measured results from at least experiments A to D.
- [ ] README and `docs/DESIGN.md` complete, with diagram and screenshot.
- [ ] No secrets, no employer code or data, `.env` is git-ignored.

---

## 18. Resume line (fill in real numbers only after the benchmark)

> **Real-Time Transaction Anomaly Pipeline** (Python, Kafka, FastAPI, PostgreSQL, Redis, Docker, Prometheus/Grafana) | github.com/<username>/txn-anomaly-pipeline
> - Built an event-driven fraud-detection pipeline with idempotent consumers, retries with backoff, and a dead-letter queue; processed [X] events/sec on [machine], a [Y]x gain from batching and consumer scaling.
> - Added Prometheus/Grafana monitoring including consumer lag, plus CI with [Z]% test coverage and one-command Docker Compose deployment.
