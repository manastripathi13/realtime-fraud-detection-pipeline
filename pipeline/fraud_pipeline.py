"""Streaming fraud-detection pipeline (Apache Beam).

    Pub/Sub  ->  parse & validate  ->  enrich with customer profile
             ->  per-card velocity (stateful)  ->  rule scoring  ->  MERGE into warehouse

Runs locally on Beam's DirectRunner against the Pub/Sub emulator and DuckDB,
or on Google Cloud Dataflow against real Pub/Sub and BigQuery (same code,
different flags). See README for both commands.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.coders import PickleCoder
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rules import RuleEngine  # noqa: E402
from sinks import BigQueryMergeSink, DuckDBMergeSink, to_reasons_json  # noqa: E402

REQUIRED = {"transaction_id": str, "card_id": str, "customer_id": str, "amount": (int, float), "event_time": str}
DEAD = "dead_letter"


class ParseAndValidate(beam.DoFn):
    """Turn raw bytes into a dict. Anything malformed goes to the dead-letter output instead of crashing the job."""

    def process(self, msg):
        payload = msg.data if hasattr(msg, "data") else msg
        try:
            txn = json.loads(payload)
            for key, typ in REQUIRED.items():
                if not isinstance(txn.get(key), typ):
                    raise ValueError(f"missing or bad field: {key}")
            if txn["amount"] < 0:
                raise ValueError("negative amount")
            pt = getattr(msg, "publish_time", None)
            txn["publish_time"] = pt.isoformat() if hasattr(pt, "isoformat") else None
            yield txn
        except Exception as e:  # noqa: BLE001 - we want every bad record captured
            yield beam.pvalue.TaggedOutput(DEAD, {"error": str(e)[:500], "payload": payload.decode("utf-8", "replace")[:2000]})


class EnrichWithProfile(beam.DoFn):
    """Add the customer's normal behaviour (home country, average spend, known devices)."""

    def __init__(self, profiles_path: str):
        self.profiles_path = profiles_path

    def setup(self):
        self.profiles = {}
        with FileSystems.open(self.profiles_path) as f:
            for row in csv.DictReader(line.decode() for line in f):
                self.profiles[row["customer_id"]] = {
                    "home_country": row["home_country"],
                    "avg_amount": float(row["avg_amount"]),
                    "known_devices": set(row["known_devices"].split("|")),
                }

    def process(self, txn):
        p = self.profiles.get(txn["customer_id"], {})
        txn["home_country"] = p.get("home_country")
        txn["avg_amount"] = p.get("avg_amount")
        txn["is_known_device"] = txn.get("device_id") in p.get("known_devices", set())
        yield txn["card_id"], txn  # key by card for the stateful velocity step


class CardVelocity(beam.DoFn):
    """Count how many transactions this card made in the last N seconds.

    Uses Beam *state*: for each card we remember recent event timestamps.
    This is how a streaming job "remembers" history without a database lookup.
    """

    RECENT = ReadModifyWriteStateSpec("recent_ts", PickleCoder())

    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds

    def process(self, element, recent=beam.DoFn.StateParam(RECENT)):
        _card, txn = element
        ts = datetime.fromisoformat(txn["event_time"]).timestamp()
        history = [t for t in (recent.read() or []) if ts - t <= self.window_seconds]
        history.append(ts)
        recent.write(history[-200:])
        txn["card_txn_count_window"] = len(history)
        yield txn


class Score(beam.DoFn):
    def __init__(self, rules_path: str):
        self.rules_path = rules_path

    def setup(self):
        self.engine = RuleEngine(self.rules_path)

    def process(self, txn):
        if self.engine.maybe_reload():
            logging.info("rules reloaded, version=%s", self.engine.cfg.get("version"))
        res = self.engine.score(txn)
        txn.update(
            fraud_score=res.score,
            is_alert=res.is_alert,
            alert_reasons=to_reasons_json(res.reasons),
            rules_version=res.rules_version,
            label=txn.pop("_label", None),
            processed_at=datetime.now(timezone.utc).isoformat(),
        )
        yield txn


class WriteToWarehouse(beam.DoFn):
    """Buffer rows within a bundle and MERGE them in one go (cheaper than row-by-row)."""

    def __init__(self, sink_kind: str, sink_args: dict, max_buffer: int = 500):
        self.sink_kind, self.sink_args, self.max_buffer = sink_kind, sink_args, max_buffer

    def setup(self):
        cls = DuckDBMergeSink if self.sink_kind == "duckdb" else BigQueryMergeSink
        self.sink = cls(**self.sink_args)
        self.sink.setup()

    def start_bundle(self):
        self.rows, self.dead = [], []

    def process(self, element, is_dead=False):
        (self.dead if is_dead else self.rows).append(element)
        if len(self.rows) + len(self.dead) >= self.max_buffer:
            self._flush()

    def finish_bundle(self):
        self._flush()

    def _flush(self):
        self.sink.write(self.rows, self.dead)
        self.rows, self.dead = [], []


def run(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--subscription", required=True, help="projects/<p>/subscriptions/<s>")
    ap.add_argument("--sink", choices=["duckdb", "bigquery"], default="duckdb")
    ap.add_argument("--duckdb-path", default="data/warehouse.duckdb")
    ap.add_argument("--bq-project")
    ap.add_argument("--bq-dataset", default="fraud_raw")
    ap.add_argument("--profiles", default="data/customer_profiles.csv")
    ap.add_argument("--rules", default="config/rules.yaml")
    args, beam_args = ap.parse_known_args(argv)

    opts = PipelineOptions(beam_args)
    opts.view_as(StandardOptions).streaming = True
    opts.view_as(SetupOptions).save_main_session = False

    import yaml

    with open(args.rules) as f:
        window_seconds = yaml.safe_load(f)["rules"]["velocity"]["window_seconds"]

    sink_args = (
        {"path": os.path.abspath(args.duckdb_path)}
        if args.sink == "duckdb"
        else {"project": args.bq_project, "dataset": args.bq_dataset}
    )

    with beam.Pipeline(options=opts) as p:
        parsed = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.subscription, with_attributes=True)
            | "Parse" >> beam.ParDo(ParseAndValidate()).with_outputs(DEAD, main="ok")
        )
        scored = (
            parsed.ok
            | "Enrich" >> beam.ParDo(EnrichWithProfile(os.path.abspath(args.profiles)))
            | "Velocity" >> beam.ParDo(CardVelocity(window_seconds))
            | "Score" >> beam.ParDo(Score(os.path.abspath(args.rules)))
        )
        writer = WriteToWarehouse(args.sink, sink_args)
        scored | "WriteScored" >> beam.ParDo(writer)
        parsed[DEAD] | "WriteDeadLetter" >> beam.ParDo(writer, is_dead=True)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
