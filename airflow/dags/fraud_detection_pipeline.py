"""Airflow DAG: run the fraud-detection pipeline end to end.

    start_pubsub ─► stream_window ─► dbt_build_gold ─► check_latency_sla ─► report

stream_window starts the Beam streaming job, publishes payments for `seconds`, lets the
job drain, and stops it. In production the Dataflow job runs continuously and only the
Gold refresh is scheduled (see fraud_gold_refresh).

Runs locally with the Pub/Sub emulator and DuckDB. Trigger it from the Airflow UI
("Trigger DAG" with optional seconds/rate).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

REPO = Path(__file__).resolve().parents[2]


def sh(cmd: str) -> str:
    # each project has its own uv environment; Airflow's own venv must not leak into it
    return f"set -euo pipefail; cd {REPO}; unset VIRTUAL_ENV; {cmd} "  # trailing space: stops Airflow treating "*.sh" as a template file


with DAG(
    dag_id="fraud_detection_pipeline",
    description="Pub/Sub -> Beam streaming window -> BigQuery-style MERGE -> dbt Gold -> SLA check",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "seconds": Param(60, type="integer", minimum=10, maximum=600, description="How long to publish payments"),
        "rate": Param(20, type="integer", minimum=1, maximum=200, description="Payments per second"),
    },
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["project-1", "fraud", "streaming", "beam", "dbt"],
    doc_md=__doc__,
) as dag:
    start_pubsub = BashOperator(
        task_id="start_pubsub",
        bash_command=sh("docker compose up -d pubsub && sleep 3 && docker compose ps pubsub"),
    )
    stream_window = BashOperator(
        task_id="stream_window",
        bash_command=sh("./scripts/stream_window.sh {{ params.seconds }} {{ params.rate }}"),
        execution_timeout=None,
    )
    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=sh("cd dbt && uv run dbt build --profiles-dir . --no-use-colors"),
    )
    check_latency_sla = BashOperator(
        task_id="check_latency_sla",
        bash_command=sh(
            "cd dbt && uv run dbt --no-use-colors --quiet show --profiles-dir . --inline "
            "\"select * from {{ '{{' }} ref('rpt_pipeline_health') {{ '}}' }} where p95_latency_seconds > 120\" --output json "
            "| uv run python -c \"import sys,json; rows=json.load(sys.stdin)['show']; "
            "print('p95 latency within 120s SLA' if not rows else 'BREACH'); sys.exit(1 if rows else 0)\""
        ),
    )
    report = BashOperator(task_id="report", bash_command=sh("uv run python scripts/report.py"))

    start_pubsub >> stream_window >> dbt_build_gold >> check_latency_sla >> report
