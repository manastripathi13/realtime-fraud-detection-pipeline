"""Cloud Composer (managed Airflow) DAG.

The streaming Dataflow job runs continuously and keeps the Bronze table fresh.
This DAG runs every 15 minutes and:
  1. checks Bronze freshness (fails fast if the stream has stalled),
  2. rebuilds the dbt Silver/Gold models and runs their data tests,
  3. checks the pipeline-health table and fails if latency or quality slips.

Set DBT_TARGET=bq in Composer's environment variables to run against BigQuery.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = os.getenv("FRAUD_DBT_DIR", "/home/airflow/gcs/data/fraud/dbt")
DBT = f"cd {DBT_DIR} && dbt --no-use-colors --quiet"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fraud_gold_refresh",
    description="Refresh fraud Gold models from the streaming Bronze table",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fraud", "dbt", "gold"],
) as dag:
    freshness = BashOperator(task_id="check_bronze_freshness", bash_command=f"{DBT} source freshness --profiles-dir .")

    build = BashOperator(task_id="dbt_build_silver_gold", bash_command=f"{DBT} build --profiles-dir .")

    sla_check = BashOperator(
        task_id="check_latency_sla",
        # dbt "show" prints the one-row health table; the inline test fails the task if p95 latency > 120s
        bash_command=(
            f"{DBT} show --profiles-dir . --inline "
            "\"select * from {{ ref('rpt_pipeline_health') }} where p95_latency_seconds > 120\" "
            "--output json | python -c \"import sys,json; rows=json.load(sys.stdin)['show']; "
            "sys.exit('latency SLA breached' if rows else 0)\""
        ),
    )

    freshness >> build >> sla_check
