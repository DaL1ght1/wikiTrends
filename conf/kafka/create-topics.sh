#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="kafka:9092"

# Wait for broker to answer metadata requests
for i in {1..60}; do
  if kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists --topic wikimedia.recentchange.raw --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists --topic wikimedia.recentchange.clean --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists --topic trending.windows.1m --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists --topic trending.windows.5m --partitions 3 --replication-factor 1
