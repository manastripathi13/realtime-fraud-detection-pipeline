"""Warehouse sinks that write with an idempotent MERGE.

Pub/Sub delivers messages *at least once*, so the same transaction can arrive
twice. Instead of a blind INSERT we MERGE on `transaction_id`: if the row is
already there it is updated, otherwise inserted. Re-running or replaying the
stream therefore never creates duplicates.

- DuckDBMergeSink: local stand-in for BigQuery (runs on a laptop, no cloud).
- BigQueryMergeSink: same logic against real BigQuery (needs a GCP project).
"""

from __future__ import annotations

import json
import time
import uuid

SCORED_COLUMNS = [
    "transaction_id", "card_id", "customer_id", "merchant_id", "merchant_category",
    "amount", "currency", "ip_country", "home_country", "device_id", "is_known_device",
    "channel", "avg_amount", "card_txn_count_window", "fraud_score", "is_alert",
    "alert_reasons", "rules_version", "label", "event_time", "publish_time", "processed_at",
]

DUCKDB_DDL = """
create schema if not exists raw;
create table if not exists raw.scored_transactions (
    transaction_id varchar primary key,
    card_id varchar, customer_id varchar, merchant_id varchar, merchant_category varchar,
    amount double, currency varchar, ip_country varchar, home_country varchar,
    device_id varchar, is_known_device boolean, channel varchar, avg_amount double,
    card_txn_count_window integer, fraud_score integer, is_alert boolean,
    alert_reasons varchar, rules_version integer, label varchar,
    event_time timestamptz, publish_time timestamptz, processed_at timestamptz,
    loaded_at timestamptz, load_count integer
);
create table if not exists raw.dead_letter (
    received_at timestamptz, error varchar, payload varchar
);
"""


def _merge_sql(target: str, source: str, now_fn: str) -> str:
    cols = ", ".join(SCORED_COLUMNS)
    sets = ", ".join(f"{c} = s.{c}" for c in SCORED_COLUMNS if c != "transaction_id")
    vals = ", ".join(f"s.{c}" for c in SCORED_COLUMNS)
    return f"""
merge into {target} t
using {source} s
on t.transaction_id = s.transaction_id
when matched then update set {sets}, loaded_at = {now_fn}, load_count = t.load_count + 1
when not matched then insert ({cols}, loaded_at, load_count) values ({vals}, {now_fn}, 1)
"""


class DuckDBMergeSink:
    def __init__(self, path: str):
        self.path = path

    def _connect(self):
        import duckdb

        # DuckDB allows one writer at a time; retry briefly if another process holds the lock.
        for attempt in range(20):
            try:
                return duckdb.connect(self.path)
            except duckdb.IOException:
                time.sleep(0.25 * (attempt + 1))
        raise RuntimeError(f"could not open {self.path}")

    def setup(self) -> None:
        con = self._connect()
        con.execute(DUCKDB_DDL)
        con.close()

    def write(self, rows: list[dict], dead: list[dict]) -> None:
        if not rows and not dead:
            return
        import pandas as pd

        con = self._connect()
        try:
            if rows:
                df = pd.DataFrame(rows, columns=SCORED_COLUMNS)
                # de-duplicate inside the batch too (MERGE needs one source row per key)
                df = df.drop_duplicates("transaction_id", keep="last")
                con.register("batch_df", df)
                con.execute(_merge_sql("raw.scored_transactions", "batch_df", "now()"))
                con.unregister("batch_df")
            if dead:
                con.executemany(
                    "insert into raw.dead_letter values (now(), ?, ?)",
                    [(d["error"], d["payload"]) for d in dead],
                )
        finally:
            con.close()


class BigQueryMergeSink:
    """Load a micro-batch into a short-lived staging table, then MERGE it into the target.

    For very high volume you would switch to the Storage Write API and run the
    MERGE on a schedule, but the idempotency idea is the same.
    """

    def __init__(self, project: str, dataset: str, location: str = "US"):
        self.project, self.dataset, self.location = project, dataset, location
        self.target = f"`{project}.{dataset}.scored_transactions`"

    def setup(self) -> None:
        from google.cloud import bigquery

        self.client = bigquery.Client(project=self.project, location=self.location)
        self.client.query(
            f"""
            create table if not exists {self.target} (
              transaction_id string, card_id string, customer_id string, merchant_id string,
              merchant_category string, amount float64, currency string, ip_country string,
              home_country string, device_id string, is_known_device bool, channel string,
              avg_amount float64, card_txn_count_window int64, fraud_score int64, is_alert bool,
              alert_reasons string, rules_version int64, label string, event_time timestamp,
              publish_time timestamp, processed_at timestamp, loaded_at timestamp, load_count int64
            )
            partition by date(event_time)
            cluster by card_id, merchant_category
            """
        ).result()
        self.client.query(
            f"create table if not exists `{self.project}.{self.dataset}.dead_letter` "
            "(received_at timestamp, error string, payload string)"
        ).result()

    def write(self, rows: list[dict], dead: list[dict]) -> None:
        from google.cloud import bigquery

        if rows:
            staging = f"{self.project}.{self.dataset}._stg_scored_{uuid.uuid4().hex[:12]}"
            dedup = {r["transaction_id"]: r for r in rows}
            job = self.client.load_table_from_json(
                list(dedup.values()),
                staging,
                job_config=bigquery.LoadJobConfig(
                    schema=self.client.get_table(self.target.strip("`")).schema[: len(SCORED_COLUMNS)],
                    write_disposition="WRITE_TRUNCATE",
                ),
            )
            job.result()
            try:
                self.client.query(_merge_sql(self.target, f"`{staging}`", "current_timestamp()")).result()
            finally:
                self.client.delete_table(staging, not_found_ok=True)
        if dead:
            errors = self.client.insert_rows_json(
                f"{self.project}.{self.dataset}.dead_letter",
                [{"received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **d} for d in dead],
            )
            if errors:
                raise RuntimeError(errors)


def to_reasons_json(reasons: list[str]) -> str:
    return json.dumps(reasons)
