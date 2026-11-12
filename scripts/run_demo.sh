#!/usr/bin/env bash
# Same steps as the Airflow DAG `fraud_detection_pipeline`, without Airflow:
#   Pub/Sub emulator -> Beam streaming window -> DuckDB (MERGE) -> dbt Gold -> report
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d pubsub >/dev/null
./scripts/stream_window.sh "${1:-60}" "${2:-20}"
(cd dbt && uv run dbt build --profiles-dir . --quiet)
uv run python scripts/report.py
