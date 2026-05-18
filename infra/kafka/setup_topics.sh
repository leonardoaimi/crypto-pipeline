#!/usr/bin/env bash
# Creates all Kafka topics required by the crypto pipeline.
# Runs once via the kafka-init docker-compose service.

set -euo pipefail

BOOTSTRAP=kafka:9092
REPLICATION=1
PARTITIONS=3

wait_for_kafka() {
  echo "Waiting for Kafka at ${BOOTSTRAP}..."
  until kafka-topics --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; do
    sleep 2
  done
  echo "Kafka is ready."
}

create_topic() {
  local topic=$1
  local partitions=${2:-$PARTITIONS}
  kafka-topics \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION}"
  echo "Topic ready: ${topic}"
}

wait_for_kafka

create_topic "crypto.trades.raw"
create_topic "crypto.trades.dlq" 1
create_topic "crypto.ohlcv.streaming"
create_topic "crypto.ohlcv.historical"
create_topic "crypto.tickers.raw"

echo "All topics created successfully."
