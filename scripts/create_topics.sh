#!/bin/sh
# Creates the pipeline topics. Safe to run repeatedly.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
RAW_PARTITIONS="${RAW_PARTITIONS:-6}"

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "${TOPIC_RAW:-transactions.raw}" --partitions "$RAW_PARTITIONS" --replication-factor 1 \
  --config retention.ms=86400000

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "${TOPIC_DLQ:-transactions.dlq}" --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --list
