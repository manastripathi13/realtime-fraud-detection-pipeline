"""Airflow / Cloud Composer DAG: refresh the fraud Gold layer every 15 minutes.

The streaming job keeps the Bronze table fresh. This DAG:
  1. checks Bronze freshness (fails fast if the stream has stalled),
  2. rebuilds the dbt Silver/Gold models and runs their data tests,
  3. fails if p95 latency (event -> warehouse) exceeds the 120 s SLA.

Locally it runs dbt through the project's uv environment against DuckDB.
On Cloud Composer set DBT_CMD="dbt" and DBT_TARGET=bq in the environment.
Paused by default: unpause it in the UI while a stream is running.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = Path(__file__).resolve().parents[2]
DBT_DIR = os.getenv("FRAUD_DBT_DIR", str(REPO / "dbt"))
DBT = f"cd {DBT_DIR} && unset VIRTUAL_ENV && " + os.getenv("DBT_CMD", "uv run dbt") + " --no-use-colors --quiet"

with DAG(
    dag_id="fraud_gold_refresh",
    description="Refresh fraud Gold models from the streaming Bronze table",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["project-1", "fraud", "dbt", "gold"],
    doc_md=__doc__,
) as dag:
    freshness = BashOperator(task_id="check_bronze_freshness", bash_command=f"{DBT} source freshness --profiles-dir .")
    build = BashOperator(task_id="dbt_build_silver_gold", bash_command=f"{DBT} build --profiles-dir .")
    sla_check = BashOperator(
        task_id="check_latency_sla",
        bash_command=(
            f"{DBT} show --profiles-dir . --inline "
            "\"select * from {{ '{{' }} ref('rpt_pipeline_health') {{ '}}' }} where p95_latency_seconds > 120\" "
            "--output json | python3 -c \"import sys,json; rows=json.load(sys.stdin)['show']; "
            "sys.exit('latency SLA breached' if rows else 0)\""
        ),
    )
    freshness >> build >> sla_check
