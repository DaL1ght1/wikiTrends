#!/bin/sh
# Runs the baseline job on a schedule, forever. Set RUN_ONCE=true to run it a
# single time (e.g. `docker compose run --rm -e RUN_ONCE=true baseline-batch`).
set -u

INITIAL_DELAY_MINUTES="${BASELINE_INITIAL_DELAY_MINUTES:-60}"
INTERVAL_HOURS="${BASELINE_INTERVAL_HOURS:-6}"

submit() {
  JARS=$(ls /opt/jars/*.jar | tr '\n' ',' | sed 's/,$//')
  /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --conf spark.cores.max="${SPARK_CORES_MAX:-1}" \
    --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1g}" \
    --conf spark.driver.host="$(hostname -i)" \
    --jars "$JARS" /app/job.py
}

if [ "${RUN_ONCE:-false}" = "true" ]; then
  submit
  exit $?
fi

echo "[baseline] first run in ${INITIAL_DELAY_MINUTES} min, then every ${INTERVAL_HOURS} h"
sleep $((INITIAL_DELAY_MINUTES * 60))
while true; do
  echo "[baseline] run started at $(date -u +%FT%TZ)"
  submit || echo "[baseline] run failed (will retry next interval)"
  sleep $((INTERVAL_HOURS * 3600))
done
