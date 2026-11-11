#!/usr/bin/env bash
# End-to-end check of a running stack: data must flow from Wikimedia all the
# way to OpenSearch and the UI API. Run after `docker compose up -d --build`.
#   ./scripts/smoke-test.sh            # waits up to 6 minutes
#   TIMEOUT=900 ./scripts/smoke-test.sh
set -uo pipefail

OS_URL="${OS_URL:-http://localhost:9200}"
UI_URL="${UI_URL:-http://localhost:8000}"
TIMEOUT="${TIMEOUT:-360}"
deadline=$(( $(date +%s) + TIMEOUT ))
fail=0

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=1; }

wait_until() {  # wait_until "description" command...
  local what="$1"; shift
  until "$@" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$deadline" ]; then bad "$what (timed out)"; return 1; fi
    sleep 5
  done
  pass "$what"
}

topic_has_data() {
  docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 \
    --topic "$1" --max-messages 1 --timeout-ms 15000 --from-beginning 2>/dev/null | grep -q '{'
}

count_gt_zero() {
  local n
  n=$(curl -fs "$OS_URL/$1/_count" | sed -E 's/.*"count":([0-9]+).*/\1/')
  [ "${n:-0}" -gt 0 ]
}

ui_has_pages() { curl -fs "$UI_URL/api/trending?minutes=30&size=5" | grep -q '"page_key"'; }

echo "Smoke test (timeout ${TIMEOUT}s)"
wait_until "init jobs completed" sh -c \
  "docker compose ps -a --format '{{.Name}} {{.State}} {{.ExitCode}}' | grep -E '^(kafka-init|opensearch-init) exited 0' | wc -l | grep -q 2"
wait_until "OpenSearch template installed" curl -fs "$OS_URL/_index_template/trending-1m"
wait_until "raw events in Kafka"        topic_has_data wikimedia.recentchange.raw
wait_until "clean events in Kafka"      topic_has_data wikimedia.recentchange.clean
wait_until "1m windows in Kafka"        topic_has_data trending.windows.1m
wait_until "documents in trending-1m"   count_gt_zero trending-1m
wait_until "documents in trending-5m"   count_gt_zero trending-5m
wait_until "UI API returns pages"       ui_has_pages
wait_until "Grafana is up"              curl -fs http://localhost:3000/api/health

if [ "$fail" -eq 0 ]; then echo "All checks passed."; else echo "Some checks failed: see 'docker compose logs <service>'."; fi
exit "$fail"
