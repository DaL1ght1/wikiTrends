#!/usr/bin/env bash
set -euo pipefail
KAFKA_BIN="/opt/bitnami/kafka/bin/kafka-topics.sh"
BOOTSTRAP="kafka:9092"


# Wait for Kafka
until /opt/bitnami/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe >/dev/null 2>&1;
  do echo "Waiting for Kafka…"; sleep 2;done
endonecho


echo "Creating topics…"
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic wikimedia.recentchange.raw --partitions 3 --replication-factor 1
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic wikimedia.recentchange.clean --partitions 3 --replication-factor 1
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic trending.windows.1m --partitions 3 --replication-factor 1
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic trending.windows.5m --partitions 3 --replication-factor 1


echo "Topics ready."