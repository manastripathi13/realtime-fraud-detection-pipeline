#!/usr/bin/env bash
# Run the streaming pipeline for a bounded window (used by the Airflow DAG and the demo):
#   reset the subscription -> start the Beam job -> publish N seconds of payments -> drain -> stop the job.
# In production the Dataflow job runs continuously instead; this makes it testable on a laptop.
set -euo pipefail
cd "$(dirname "$0")/.."
SECONDS_TO_PUBLISH="${1:-60}"
RATE="${2:-20}"
export PUBSUB_EMULATOR_HOST=localhost:8085 GCP_PROJECT=local-fraud-demo
SUB="projects/${GCP_PROJECT}/subscriptions/transactions-sub"

for i in $(seq 1 20); do uv run python scripts/setup_pubsub.py --reset >/dev/null 2>&1 && break; sleep 2; done
rm -f data/warehouse.duckdb data/warehouse.duckdb.wal
uv run python pipeline/fraud_pipeline.py --runner=BundleBasedDirectRunner --subscription "$SUB" > data/pipeline.log 2>&1 &
PIPE_PID=$!
trap 'kill $PIPE_PID 2>/dev/null || true' EXIT
for i in $(seq 1 60); do [ -f data/warehouse.duckdb ] && break; sleep 1; done
sleep 3
echo "publishing transactions for ${SECONDS_TO_PUBLISH}s at ~${RATE}/s"
uv run python producer/generate_transactions.py --seconds "$SECONDS_TO_PUBLISH" --rate "$RATE"
sleep 15   # let the pipeline drain
kill $PIPE_PID 2>/dev/null || true; wait $PIPE_PID 2>/dev/null || true
uv run python -c "import duckdb; c=duckdb.connect('data/warehouse.duckdb', read_only=True); print('rows in bronze:', c.sql('select count(*) from raw.scored_transactions').fetchall()[0][0])"
