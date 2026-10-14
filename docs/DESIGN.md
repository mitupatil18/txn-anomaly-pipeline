# Design notes

This document records the key decisions and answers the questions an interviewer is most likely to
ask. It describes what the code in this repository actually does.

## Flow

```
simulator --key=account_id--> transactions.raw (6 partitions)
                                   |  group txn-processors
                                   v
   consumer:  parse/validate -> BEGIN -> INSERT ... ON CONFLICT DO NOTHING (transactions)
              -> read Redis state -> evaluate rules -> INSERT ... ON CONFLICT DO NOTHING (alerts)
              -> UPDATE is_flagged -> COMMIT -> write Redis state -> commit Kafka offsets
                       |
                       +-- invalid / retries exhausted / unexpected --> transactions.dlq
API (FastAPI) reads PostgreSQL;  every service exposes /metrics;  Prometheus + Grafana.
```

## Decisions

| # | Decision | Why | Trade-off |
|---|---|---|---|
| 1 | Key = `account_id` | per-account ordering and consistent per-account rule state | hot accounts serialize on one partition (Q7) |
| 2 | At-least-once + idempotent sink | simple and fast, duplicates are harmless | not exactly-once across Kafka and Postgres (Q9) |
| 3 | Rule state in Redis, written *after* the DB commit | survives restarts and rebalances; DB stays the source of truth | a crash between commit and Redis write loses one batch of state (Q3, Q5) |
| 4 | In-process retries (exponential backoff, full jitter) for transient errors only | transient faults usually clear in seconds | blocks the partition while retrying |
| 5 | DLQ is a Kafka topic with diagnostic headers + replay script | nothing is silently dropped; replay is trivial | needs someone to look at the DLQ |
| 6 | `BATCH_SIZE` option | one DB transaction and one offset commit per batch instead of per message (see BENCHMARKS.md) | a failing batch is retried and then split per message |

Other choices the spec left open:

* **Rules are pure functions** `evaluate(event, AccountState) -> RuleResult | None`. All Redis reads for
  a batch happen in one pipelined round trip (`RedisState.fetch`), so rules never touch Redis directly.
* **Batch overlay.** Redis is only written after the commit, so inside a batch each event is applied to an
  in-memory copy of the account state (`apply_event`). Without it, a burst of 8 events in one batch
  would not trip the velocity rule. The same state is then written to Redis in one pipeline.
* **EWMA** uses alpha = 0.1 (constant in `state.py`).
* **Offsets** are committed synchronously per batch, after the DB commit and the DLQ publish.
* **DLQ publish waits for the broker ack** (`flush`); if the DLQ itself is down the consumer crashes
  *without* committing, so the message is redelivered after restart rather than lost.
* **DB engine** uses `connect_timeout=5` so a hung database surfaces as a retryable error.
* **No authentication** on the API (out of scope, listed in the README limitations).

## Q&A

### 1. Why key by `account_id`, and what breaks if you key by `transaction_id`?

Kafka only guarantees order within a partition. Keying by `account_id` puts all events of an account
on one partition, so one consumer sees them in order and owns that account's rule state.
Keying by `transaction_id` spreads an account's events over all partitions and consumers:
velocity counts and "previous country" would depend on which consumer saw what, events would be
evaluated out of order, and the state would need cross-consumer coordination (locks or a shared
atomic store), which costs more than it saves. Load balancing improves (no hot keys), but correctness
of the stateful rules is lost.

### 2. What delivery guarantee do you provide and how?

**At-least-once delivery, effectively-once results in PostgreSQL.**
Offsets are committed manually, only after the DB transaction committed (and any DLQ publish was
acknowledged), so a message is never lost. Redelivery is made harmless by the primary key on
`transactions.transaction_id` (`ON CONFLICT DO NOTHING`; a duplicate skips rule evaluation) and the
`UNIQUE (transaction_id, rule)` constraint on `alerts`. The producer side uses `acks=all` and
`enable.idempotence=true`, so retries inside the producer do not create duplicates in the topic.
Redis state is *not* covered by this guarantee (Q5).

### 3. What happens if a consumer crashes mid-batch? After the DB commit but before the offset commit?

* **Mid-batch (before COMMIT):** the transaction rolls back, nothing was written and no offset was
  committed, so the whole batch is redelivered and processed normally. Redis was not touched yet.
* **After COMMIT, before the offset commit:** the batch is redelivered; every insert conflicts and is
  skipped (`txn_processed_total{result="duplicate"}` goes up), so no duplicate rows or alerts.
  If the crash also happened *before the Redis write*, that batch's events are missing from Redis state
  and, since redelivered duplicates are skipped, they are never added. Consequence: velocity/EWMA/geo
  for those accounts are slightly under-counted for up to the window (60 s) to a few events of EWMA
  history. This is the documented trade-off of decision 3. `tests/integration/test_pipeline.py::
  test_crash_before_offset_commit_causes_no_duplicates` simulates the second case.

### 4. What happens on a rebalance?

The consumer uses the cooperative-sticky assignor, so only the partitions that move are revoked
(no stop-the-world). The poll loop is single-threaded and processes a batch fully before polling again,
so a revoke callback never runs in the middle of a batch. `on_revoke` commits any offsets still
pending (normally none, because each batch is committed immediately) and logs the partitions.
The new owner resumes from the committed offset and reads rule state from Redis, so it continues
with correct windows. Two edge cases: (a) a consumer that is partitioned away and loses its
partitions (`on_lost`) cannot commit; the new owner redelivers and idempotency absorbs it;
(b) briefly overlapping owners (a "zombie" after a session timeout) could race on the Redis
read-modify-write of EWMA; the database stays correct, rule state can be slightly off.

### 5. Why is rule state in Redis and what are its failure modes?

In-memory state would be lost on every restart or rebalance and would need to be rebuilt by replaying
history. Redis gives fast shared state with TTLs (windows expire on their own).
Failure modes and behaviour:

* **Redis down on read:** a transient error; the batch is rolled back, retried with backoff, and sent to
  the DLQ (`transient_exhausted`) if it does not recover. Fail-closed: we never evaluate rules on
  missing state.
* **Redis down on write (after commit):** logged, `txn_state_update_failures_total` incremented, the
  message is *not* failed. State lags until later events overwrite it.
* **Redis data loss / flush:** rules under-alert until state is rebuilt from new traffic.
* **Non-atomic read-modify-write:** safe only because one consumer owns an account (decision 1).
* **Out-of-order events:** windows use event time; events older than the window are ignored by the velocity
  rule and cannot move the "last country" backwards. Strict out-of-order handling is a known limitation.

### 6. How do retries and the DLQ work, and what is retried versus not?

| Failure | Retried? | Outcome |
|---|---|---|
| Invalid JSON / schema / amount / country ... | no | DLQ `validation`, `x-attempts=0` |
| DB or Redis connection error, timeout | yes: base 0.2 s, doubling, cap 5 s, full jitter, 5 attempts | success, else DLQ `transient_exhausted` |
| Anything else (bug, constraint violation) | no | batch is split and retried one message at a time to isolate the poison message; the culprit goes to the DLQ as `unexpected` |

The DLQ message carries the original key and bytes plus headers `x-original-topic/partition/offset`,
`x-error-type`, `x-error-message`, `x-attempts`, `x-failed-at`. After the DLQ publish is acknowledged the
offset is committed so the partition does not block. `scripts/replay_dlq.py` republishes selected
messages (`--error-type`, `--limit`, `--dry-run`) to the raw topic.
Note: when a *batch* exhausts retries on a transient error, every message of that batch goes to the
DLQ (they are all replayable). With a long database outage that means a lot of DLQ traffic; a
circuit breaker that pauses the consumer instead is a sensible future improvement.

### 7. How would you handle a hot account?

One key = one partition = one consumer, so a hot account caps at one consumer's throughput and
causes lag on that partition. Options, in order of effort: (1) batching, which already raises
per-partition throughput (BENCHMARKS.md); (2) detect hot keys from lag per partition plus a top-N
counter; (3) give hot accounts a sub-key (`account_id#shard`) and compute the velocity count from
per-shard counters that are summed on read, which keeps per-shard order but loses strict per-account
order; (4) rate limit or aggregate upstream; (5) move the rule to a stateful stream processor with
keyed state and local state stores.

### 8. How would you handle schema evolution?

Today the consumer is a tolerant reader (`extra="ignore"`), so adding optional fields is safe for
old consumers, and new required fields would send old events to the DLQ. For production: a schema
registry with Avro or Protobuf and BACKWARD/FULL compatibility, additive changes with defaults only,
an explicit `schema_version`, and for a breaking change a new topic (`transactions.raw.v2`) with
dual-publishing during migration. The DLQ plus replay covers events that failed during a rollout.

### 9. How would you achieve true exactly-once semantics, and what would it cost?

Kafka transactions give exactly-once for Kafka-to-Kafka flows (`read_committed` is already set on
the consumer). For a database sink the usual approach is to make the offset part of the DB transaction:
store `(topic, partition, offset)` in Postgres in the same transaction as the results, and on assignment
`seek` to the stored offset instead of using group offsets. Redis state would have to move into the
same transaction (for example into Postgres) or be rebuilt deterministically from the log.
Costs: custom offset management and rebalance handling, one more write per batch, loss of the simple
operational view of consumer-group lag (the exporter would no longer reflect progress), and
tighter coupling between Kafka and Postgres.

### 10. What would you change at 100x the traffic?

* Kafka: 3+ brokers, replication factor 3, `min.insync.replicas=2`, many more partitions, tune
  `fetch.min.bytes`/`linger.ms`, lag-based autoscaling (HPA/KEDA) of consumers up to the partition count.
* PostgreSQL: time-partitioned `transactions`/`alerts`, `COPY` or larger multi-row inserts, fewer indexes on
  the hot path, a read replica for the API, retention/archival to cheaper storage.
* State: Redis Cluster sharded by account, or a stateful stream processor (Flink, Kafka Streams) with
  local state and changelog topics, removing the per-batch Redis round trip.
* Reliability: circuit breaker on database outages, DLQ alerting, schema registry, per-tenant quotas.
* Observability: exemplars or tracing, alert rules on lag and DLQ rate, SLOs for end-to-end latency.
