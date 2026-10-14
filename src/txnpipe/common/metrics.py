"""Shared Prometheus metric definitions."""

from prometheus_client import Counter, Histogram

_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
_E2E_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)

TXN_PROCESSED = Counter("txn_processed_total", "Messages processed", ["result"])
TXN_ALERTS = Counter("txn_alerts_total", "Alerts raised", ["rule", "severity"])
TXN_PROCESSING_SECONDS = Histogram(
    "txn_processing_seconds",
    "Processing time per message (batch time divided by batch size)",
    buckets=_LATENCY_BUCKETS,
)
TXN_E2E_LATENCY = Histogram(
    "txn_end_to_end_latency_seconds",
    "Processed time minus event timestamp",
    buckets=_E2E_BUCKETS,
)
TXN_RETRIES = Counter("txn_retries_total", "Transient-error retries", ["error_type"])
TXN_DLQ = Counter("txn_dlq_total", "Messages sent to the DLQ", ["error_type"])
TXN_STATE_UPDATE_FAILURES = Counter(
    "txn_state_update_failures_total", "Redis state updates that failed after DB commit"
)
TXN_BATCH_SIZE = Histogram(
    "txn_batch_size", "Messages per processed batch", buckets=(1, 2, 5, 10, 25, 50, 100, 200, 500)
)

HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "API request duration",
    ["method", "path", "status"],
    buckets=_LATENCY_BUCKETS,
)
