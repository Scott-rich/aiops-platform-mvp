#!/bin/sh
set -eu
for topic in telemetry.metrics telemetry.logs telemetry.spans ops.alerts ops.deadletter; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 \
    --replication-factor 1 --config retention.ms=604800000
done
