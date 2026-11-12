#!/usr/bin/env bash
# One-command local demo:
#   Pub/Sub emulator -> Beam streaming pipeline -> DuckDB (MERGE) -> dbt Gold models -> report
set -euo pipefail
cd "$(dirname "$0")/.."

SECONDS_TO_PUBLISH="${1:-60}"
RATE="${2:-20}"
export PUBSUB_EMULATOR_HOST=localhost:8085
export GCP_PROJECT=local-fraud-demo
SUB="projects/${GCP_PROJECT}/subscriptions/transactions-sub"

echo "1) starting Pub/Sub emulator"
docker compose up -d pubsub >/dev/null
for i in $(seq 1 20); do uv run python scripts/setup_pubsub.py --reset >/dev/null 2>&1 && break; sleep 2; done

echo "2) starting streaming pipeline (logs in data/pipeline.log)"
rm -f data/warehouse.duckdb data/warehouse.duckdb.wal
uv run python pipeline/fraud_pipeline.py --runner=BundleBasedDirectRunner --subscription "$SUB" > data/pipeline.log 2>&1 &
PIPE_PID=$!
trap 'kill $PIPE_PID 2>/dev/null || true' EXIT
for i in $(seq 1 60); do [ -f data/warehouse.duckdb ] && break; sleep 1; done
sleep 3

echo "3) publishing transactions for ${SECONDS_TO_PUBLISH}s at ~${RATE}/s"
uv run python producer/generate_transactions.py --seconds "$SECONDS_TO_PUBLISH" --rate "$RATE"

echo "4) letting the pipeline drain"
sleep 15
kill $PIPE_PID 2>/dev/null || true; wait $PIPE_PID 2>/dev/null || true

echo "5) building dbt Silver/Gold models and running tests"
(cd dbt && uv run dbt build --profiles-dir . --quiet)

echo "6) results"
uv run python scripts/report.py
