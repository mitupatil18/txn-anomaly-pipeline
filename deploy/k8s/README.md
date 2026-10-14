# Kubernetes deployment

Manifests for the **api** and the **consumer** (probes, resource requests and limits, ConfigMap,
Secret, a Service for the API, an HPA for the consumer, and a migration Job). They were verified on a
local `kind` cluster (v0.27, Kubernetes via Docker Desktop).

Kafka, PostgreSQL and Redis are **assumed to be provided externally** (managed services or Helm charts
such as Strimzi, Bitnami PostgreSQL and Redis). For a self-contained demo, `dev-infra/` contains
single-replica, non-persistent versions of the three, which are for demos only.

## Run it on kind

```bash
kind create cluster --name txn
docker build -t txnpipe:latest .
kind load docker-image txnpipe:latest --name txn

kubectl apply -k deploy/k8s/dev-infra          # kafka (+ topics job), postgres, redis
kubectl -n txn-pipeline rollout status deploy/kafka
kubectl -n txn-pipeline wait --for=condition=complete job/kafka-init --timeout=240s

kubectl apply -k deploy/k8s                    # config, secret, migration job, api, consumer, HPA
kubectl -n txn-pipeline rollout status deploy/txn-api deploy/txn-consumer

kubectl -n txn-pipeline port-forward svc/txn-api 8080:80      # http://localhost:8080/docs and /ui
kubectl -n txn-pipeline run sim --image=txnpipe:latest --image-pull-policy=Never --restart=Never --rm -i \
  --env=KAFKA_BOOTSTRAP_SERVERS=kafka:9092 --command -- \
  python -m txnpipe.producer.simulator --count 5000 --rate 0 --seed 42 --invalid-ratio 0.01
```

## What was verified

* All pods become Ready; the API and consumers wait for the schema (`wait-for-schema`
  initContainer, `scripts/wait_for_schema.py`) so they never start against an unmigrated database.
* 5,000 simulated messages gave 4,931 rows in PostgreSQL plus 69 messages in the DLQ (total 5,000).
* The 6 partitions were distributed across the consumer pods (observed assignments such as `[2, 0]` and
  `[3, 1]` after cooperative rebalancing); `/ready`, `/stats` and `/ui` work through the Service.
* Deleting a consumer pod: the process logs `shutdown requested`, `partitions revoked`,
  `consumer stopped` and the pod terminates in about 3 s (grace period 30 s).

## Notes and limits

* **HPA:** the object is valid and accepted, but its scaling behaviour was **not exercised**: kind has no
  metrics-server by default. `maxReplicas` is 6 because more consumers than partitions sit idle.
  CPU is a rough proxy; scaling on consumer lag (KEDA) is better.
* `BATCH_SIZE` is 200 in the ConfigMap (see `docs/BENCHMARKS.md`).
* The Secret holds demo credentials. Replace it with a real secret mechanism outside this repository.
* Prometheus scrape annotations are set on the pods; the monitoring stack itself is not part of these manifests.
* `enableServiceLinks: false` on the Kafka pod is required; otherwise Kubernetes injects `KAFKA_PORT=tcp://...`,
  which the Kafka image misreads.
