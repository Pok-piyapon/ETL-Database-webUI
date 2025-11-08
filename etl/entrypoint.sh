#!/usr/bin/env bash
set -euo pipefail

# Load .env to container environment if present
if [ -f /app/.env ]; then
  set -a
  . /app/.env
  set +a
fi

# Defaults
: "${SPARK_DRIVER_MEMORY:=1g}"
: "${SPARK_EXECUTOR_MEMORY:=1g}"

# Wait for DBs
wait_for_db() {
  host=$1; port=$2; name=$3;
  echo "Waiting for $name at $host:$port ..."
  for i in {1..60}; do
    if nc -z "$host" "$port" >/dev/null 2>&1; then
      echo "$name is up"; return 0
    fi
    sleep 2
  done
  echo "Timeout waiting for $name"; exit 1
}

wait_for_db "${SRC_DB_HOST}" "${SRC_DB_PORT}" "mariadb-src"
wait_for_db "${DST_DB_HOST}" "${DST_DB_PORT}" "mariadb-dst"

run_once() {
  python3 /app/etl/app/main.py
}

INTERVAL=${ETL_INTERVAL_SECONDS:-0}
if [ "$INTERVAL" -gt 0 ] 2>/dev/null; then
  echo "ETL loop mode enabled. Interval: ${INTERVAL}s"
  while true; do
    echo "[ETL] Run started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if run_once; then
      echo "[ETL] Run completed: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    else
      echo "[ETL] Run failed: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >&2
    fi
    sleep "$INTERVAL"
  done
else
  run_once
fi
