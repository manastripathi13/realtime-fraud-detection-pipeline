# Real-Time Event-Driven Fraud Detection Pipeline

Streaming pipeline that scores card payments for travel bookings (hotels, airlines, car rentals, OTAs, cruises) as they happen, flags likely fraud within a second, and publishes BI-ready "revenue at risk" tables.

**Stack:** GCP Pub/Sub · Dataflow (Apache Beam) · BigQuery · Cloud Composer (Airflow) · dbt · Python · Docker

**System design:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the architecture diagrams and a step-by-step walkthrough of the design, decisions and failure handling.

```
 producer ──► Pub/Sub ──► Beam / Dataflow (streaming) ──► BigQuery Bronze ──► dbt Silver/Gold ──► dashboards
 (payments)   topic       parse → enrich → velocity      MERGE on            (Composer runs     revenue at risk,
                          → score with rules.yaml         transaction_id      every 15 min)      alerts, latency
                          bad messages → dead_letter
```

## Results from a local run

| Load | Transactions | p50 latency | p95 latency | Max latency | Duplicates absorbed | Alert precision |
|---|---|---|---|---|---|---|
| ~20 events/s | 628 | 0.19 s | n/a | n/a | 4 | 100% |
| ~60 events/s (3x) | 1,917 | 0.36 s | 0.61 s | 0.90 s | 21 | 100% |

Latency is measured from when the payment happened to when the scored row landed in the warehouse. Tripling the load needed no code or design change. Recall is about 55 to 65%, because the first few payments of a fast fraud burst look normal until the velocity rule has enough history. That trade-off is discussed below.

## Run it with Airflow (the main way)

All pipeline runs go through Airflow. The DAGs live in [`airflow/dags/`](airflow/dags/):

| DAG | Schedule | Tasks |
|---|---|---|
| `fraud_detection_pipeline` | manual (params `seconds`, `rate`) | `start_pubsub` → `stream_window` → `dbt_build_gold` → `check_latency_sla` → `report` |
| `fraud_gold_refresh` | every 15 min (paused by default) | `check_bronze_freshness` → `dbt_build_silver_gold` → `check_latency_sla` |

`stream_window` starts the Beam streaming job, publishes payments for `seconds`, lets it drain and stops it, so a run has a clear start and end on a laptop. In production the Dataflow job runs continuously and `fraud_gold_refresh` is what Composer schedules.

**Start a local Airflow 3 pointed at this repo's DAGs** (web UI on http://localhost:8081, no login):

```bash
uv venv --python 3.12 ~/airflow-venv
curl -sO https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt
uv pip install --python ~/airflow-venv/bin/python -c constraints-3.12.txt apache-airflow==3.2.2 apache-airflow-providers-standard

export AIRFLOW_HOME=~/airflow AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags" AIRFLOW__CORE__LOAD_EXAMPLES=False \
       AIRFLOW__API__PORT=8081 AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
       AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8081/execution/ PATH="$HOME/airflow-venv/bin:$PATH"
airflow standalone
```

Tasks call this repo's own `uv` environment (`uv run ...`), so Airflow and the project keep separate dependencies. The constraints file pins Airflow's dependencies to the versions it was tested with; without it a newer Pydantic breaks Airflow 3.2's API server.

Then open the UI, unpause `fraud_detection_pipeline` and click **Trigger**. A 30-second run takes about 2 minutes, and all five tasks go green.

## Run it without Airflow (no cloud account needed)

Requirements: Docker Desktop, [uv](https://docs.astral.sh/uv/), Python 3.12 (uv installs it).

```bash
uv sync            # install dependencies
make demo          # emulator → pipeline → DuckDB → dbt → printed report
make test          # unit tests for the rule engine
```

Locally, the **Pub/Sub emulator** (official Google image) stands in for Pub/Sub, **DuckDB** stands in for BigQuery, and Beam's **DirectRunner** stands in for Dataflow. The pipeline code is the same; only flags change.

## Run it on Google Cloud

```bash
export GCP_PROJECT=<your-project>
gcloud pubsub topics create transactions
gcloud pubsub subscriptions create transactions-sub --topic transactions
bq mk --dataset $GCP_PROJECT:fraud_raw
gsutil cp data/customer_profiles.csv gs://<bucket>/fraud/customer_profiles.csv

uv run python pipeline/fraud_pipeline.py \
  --runner=DataflowRunner --project=$GCP_PROJECT --region=us-central1 \
  --temp_location=gs://<bucket>/tmp \
  --subscription=projects/$GCP_PROJECT/subscriptions/transactions-sub \
  --sink=bigquery --bq-project=$GCP_PROJECT --bq-dataset=fraud_raw \
  --profiles=gs://<bucket>/fraud/customer_profiles.csv

DBT_TARGET=bq dbt build --project-dir dbt --profiles-dir dbt
```

Copy `airflow/dags/fraud_gold_refresh.py` to the Composer bucket's `dags/` folder and the `dbt/` folder to `data/fraud/dbt`.

> The cloud path uses the same code as the local run, but this repo has only been executed end to end locally. The BigQuery sink and Dataflow flags follow Google's documented APIs.

---

## Explained from zero

### The business problem
A travel company takes thousands of card payments a minute. Some are made with stolen cards. The old setup collected payments in **batches** and checked them every few hours. By the time fraud was spotted (4 to 6 hours later), the hotel night or flight was already used, and the money was lost as a chargeback. The goal was to check every payment **as it happens**.

### The pieces, one by one

| Piece | What it is, in plain words | What it does here |
|---|---|---|
| **Pub/Sub** | A message queue, like a post box. Apps drop messages in, other apps pick them up. | Every payment becomes one message on the `transactions` topic. |
| **Producer** | A script pretending to be the payment system. | Sends realistic payments, plus a few fraud patterns, duplicates and one broken message. |
| **Apache Beam / Dataflow** | Beam is a library for writing data pipelines. Dataflow is Google's service that runs Beam pipelines on many machines. | Reads each message, cleans it, adds customer info, scores it and writes it out. Runs forever ("streaming"). |
| **Rules (`rules.yaml`)** | A plain settings file. | Says what counts as suspicious and how many points each signal is worth. |
| **BigQuery** | Google's data warehouse: a database built for analysing huge tables with SQL. | Stores every scored payment (DuckDB plays this role locally). |
| **dbt** | A tool where you write SQL `select` statements and it builds them into tables in the right order, with tests. | Turns raw rows into clean (Silver) and business-ready (Gold) tables. |
| **Cloud Composer / Airflow** | A scheduler for data jobs. You describe tasks and their order, and it runs them on a timetable. | Every 15 minutes: check the stream is alive, rebuild Gold tables, check latency. |
| **Docker** | Packs an app and everything it needs into one portable box. | Runs the Pub/Sub emulator, and packages the pipeline. |

### What happens to one payment
1. **Parse and validate.** Is it valid JSON? Are the ID, card, amount and time present? If not, it goes to a **dead-letter** table (a "lost and found" for bad messages) instead of crashing the job.
2. **Enrich.** Look up the customer's home country, normal spend and known devices.
3. **Velocity.** How many payments has this card made in the last 60 seconds? Beam keeps a small memory per card (**state**) so it can answer this without querying a database.
4. **Score.** Each rule that matches adds points. 50 or more means alert.
   - amount far above normal: +50
   - more than 5 payments in a minute: +40
   - IP country differs from home country: +30
   - tiny "test" amount (≤ $2): +25
   - unknown device and ≥ $800: +25
5. **Write with MERGE.** Pub/Sub can deliver the same message twice ("at-least-once delivery"). A plain INSERT would double-count it. **MERGE** says "if this `transaction_id` already exists, update it; otherwise insert it". Replays never create duplicates. This is called being **idempotent**.

### The medallion layers
- **Bronze** `raw.scored_transactions`: exactly what the pipeline wrote.
- **Silver** `silver.stg_transactions`: clean names and types, plus helper columns such as latency.
- **Gold**: tables a dashboard reads directly.
  - `fct_fraud_alerts`: one row per alert.
  - `agg_revenue_at_risk_daily`: booking value vs value flagged as fraud, per category per day.
  - `rpt_rule_effectiveness`: how often each rule fires and how often it is right.
  - `rpt_pipeline_health`: latency, duplicates absorbed, precision and recall.

### Zero-downtime rule updates
The scoring step re-reads `rules.yaml` every 15 seconds if the file changed. Fraud analysts can change a threshold or switch off a noisy rule, and the running pipeline picks it up without a restart. `tests/test_rules.py::test_rules_hot_reload` proves it.

### Key terms
- **Latency**: time from the payment happening to the result being stored.
- **Precision**: of the payments we flagged, how many were really fraud. High precision means few false alarms.
- **Recall**: of all real fraud, how much we caught. High recall means little fraud slips through.
- **Partitioning / clustering (BigQuery)**: the table is split by day and sorted by card and category, so queries scan less data and cost less.

---

## Talking about it in an interview

**30-second version.** "Fraud checks ran in batch, so detection lagged 4 to 6 hours. I built an event-driven pipeline on GCP. Pub/Sub ingests every payment, a Beam job on Dataflow enriches it and scores it with rules, and results land in BigQuery through an idempotent MERGE, so duplicates from at-least-once delivery don't double-count. dbt builds Gold tables like revenue at risk by category, and Composer refreshes them and checks a latency SLA. Alerts arrive in seconds instead of hours, rules can be changed without downtime, and tripling the volume needed no design change."

**Likely follow-up questions**
- *Why MERGE and not INSERT?* Pub/Sub is at-least-once, and retries happen. MERGE on a business key makes the write idempotent.
- *Why is recall not 100%?* The first payments in a burst look normal until velocity history builds up. Options: lower the velocity threshold (more false alarms), add a card-level hold after the first alert, or train an ML model on the labelled history in BigQuery.
- *How would you handle late or out-of-order events?* Use event-time windows with watermarks and allowed lateness in Beam. Velocity here uses event time, not arrival time.
- *How does it scale?* Dataflow autoscales workers. The work is keyed by `card_id`, so it spreads evenly across workers.
- *Cost control in BigQuery?* Partition by date, cluster by card and category, use incremental dbt models for big Gold tables, and avoid `select *`.
- *What if the stream stops?* `dbt source freshness` fails the Composer DAG if Bronze is older than 60 minutes, which alerts the on-call engineer.

## Repo layout
```
producer/generate_transactions.py   synthetic payments + fraud patterns
pipeline/fraud_pipeline.py          Beam streaming pipeline
pipeline/rules.py                   rule engine with hot reload
pipeline/sinks.py                   DuckDB / BigQuery MERGE sinks
config/rules.yaml                   fraud rules (editable at runtime)
dbt/                                Silver + Gold models, tests, BigQuery/DuckDB profiles
airflow/dags/                       Airflow DAGs: fraud_detection_pipeline (end to end), fraud_gold_refresh (Composer, every 15 min)
scripts/stream_window.sh            bounded streaming run used by the DAG
scripts/run_demo.sh                 one-command local demo
tests/test_rules.py                 unit tests
```
