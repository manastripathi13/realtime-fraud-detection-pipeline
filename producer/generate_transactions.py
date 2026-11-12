"""Synthetic payment-transaction producer.

Simulates card payments for travel bookings (hotels, airlines, car rentals,
OTAs, cruises) and publishes each one as a JSON message to Pub/Sub.

A small share of the traffic is deliberately fraudulent so the pipeline has
something to catch. Each message carries a hidden `_label` field with the
"true" answer, which we only use later to measure how well the rules work.

Usage:
    python producer/generate_transactions.py --seconds 60 --rate 25
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import pubsub_v1

ROOT = Path(__file__).resolve().parents[1]
PROFILES_CSV = ROOT / "data" / "customer_profiles.csv"

COUNTRIES = ["IN", "US", "GB", "AE", "SG", "DE", "FR", "AU"]
CATEGORIES = {
    # category: (min, max) typical booking amount in USD
    "hotel": (60, 450),
    "airline": (80, 900),
    "car_rental": (30, 250),
    "ota": (40, 600),
    "cruise": (300, 2500),
}
CHANNELS = ["web", "mobile_app", "call_centre"]


def build_profiles(n_customers: int, seed: int) -> list[dict]:
    """Create (or load) a fixed set of customers with their normal behaviour."""
    if PROFILES_CSV.exists():
        with PROFILES_CSV.open() as f:
            rows = list(csv.DictReader(f))
        for r in rows:
            r["avg_amount"] = float(r["avg_amount"])
            r["known_devices"] = r["known_devices"].split("|")
        return rows

    rng = random.Random(seed)
    profiles = []
    for i in range(n_customers):
        profiles.append(
            {
                "customer_id": f"C{i:05d}",
                "card_id": f"CARD-{rng.randint(10**11, 10**12 - 1)}",
                "home_country": rng.choice(COUNTRIES),
                "avg_amount": round(rng.uniform(80, 500), 2),
                "known_devices": [f"DEV-{uuid.UUID(int=rng.getrandbits(128)).hex[:10]}" for _ in range(rng.randint(1, 3))],
            }
        )
    PROFILES_CSV.parent.mkdir(parents=True, exist_ok=True)
    with PROFILES_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["customer_id", "card_id", "home_country", "avg_amount", "known_devices"])
        w.writeheader()
        for p in profiles:
            w.writerow({**p, "known_devices": "|".join(p["known_devices"])})
    return profiles


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def normal_txn(p: dict, rng: random.Random) -> dict:
    cat = rng.choice(list(CATEGORIES))
    lo, hi = CATEGORIES[cat]
    amount = min(max(rng.gauss(p["avg_amount"], p["avg_amount"] * 0.3), lo), hi)
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": p["card_id"],
        "customer_id": p["customer_id"],
        "merchant_id": f"M{rng.randint(1, 300):04d}",
        "merchant_category": cat,
        "amount": round(amount, 2),
        "currency": "USD",
        "ip_country": p["home_country"],
        "device_id": rng.choice(p["known_devices"]),
        "channel": rng.choice(CHANNELS),
        "event_time": now_iso(),
        "_label": "legit",
    }


def fraud_burst(p: dict, rng: random.Random) -> list[dict]:
    """Return one of four fraud patterns as a list of transactions."""
    pattern = rng.choice(["card_testing_then_big", "amount_spike", "geo_new_device", "velocity"])
    foreign = rng.choice([c for c in COUNTRIES if c != p["home_country"]])
    stolen_device = f"DEV-{uuid.uuid4().hex[:10]}"
    out: list[dict] = []

    def make(amount: float, cat: str, ip: str, device: str) -> dict:
        t = normal_txn(p, rng)
        t.update(amount=round(amount, 2), merchant_category=cat, ip_country=ip, device_id=device, _label=f"fraud:{pattern}")
        return t

    if pattern == "card_testing_then_big":
        out += [make(rng.uniform(0.5, 1.99), "ota", foreign, stolen_device) for _ in range(3)]
        out.append(make(rng.uniform(1500, 2500), "cruise", foreign, stolen_device))
    elif pattern == "amount_spike":
        out.append(make(p["avg_amount"] * rng.uniform(5, 9), "airline", p["home_country"], rng.choice(p["known_devices"])))
    elif pattern == "geo_new_device":
        out.append(make(rng.uniform(900, 2000), "hotel", foreign, stolen_device))
    else:  # velocity
        out += [make(rng.uniform(100, 300), "car_rental", foreign, stolen_device) for _ in range(7)]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.getenv("GCP_PROJECT", "local-fraud-demo"))
    ap.add_argument("--topic", default=os.getenv("PUBSUB_TOPIC", "transactions"))
    ap.add_argument("--seconds", type=int, default=60, help="how long to keep publishing")
    ap.add_argument("--rate", type=float, default=20, help="normal transactions per second")
    ap.add_argument("--fraud-share", type=float, default=0.02, help="chance each tick starts a fraud burst")
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--duplicate-share", type=float, default=0.01, help="re-send some messages to prove MERGE is idempotent")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    profiles = build_profiles(args.customers, args.seed)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    # one deliberately broken message so the dead-letter path is always exercised
    publisher.publish(topic_path, b'{"transaction_id": "broken", "amount": "not-a-number"').result()

    sent = frauds = dups = 0
    end = time.time() + args.seconds
    futures = []
    while time.time() < end:
        tick_start = time.time()
        batch = [normal_txn(rng.choice(profiles), rng) for _ in range(int(args.rate))]
        if rng.random() < args.fraud_share * args.rate:
            burst = fraud_burst(rng.choice(profiles), rng)
            frauds += len(burst)
            batch += burst
        if batch and rng.random() < args.duplicate_share * args.rate:
            batch.append(dict(batch[0]))  # exact duplicate: same transaction_id
            dups += 1
        for txn in batch:
            futures.append(publisher.publish(topic_path, json.dumps(txn).encode()))
            sent += 1
        # also send the occasional broken message to exercise the dead-letter path
        if rng.random() < 0.01:
            futures.append(publisher.publish(topic_path, b'{"transaction_id": "broken", "amount": "not-a-number"'))
        time.sleep(max(0.0, 1.0 - (time.time() - tick_start)))

    for f in futures:
        f.result(timeout=30)
    print(f"published {sent} transactions ({frauds} fraudulent, {dups} duplicates) to {topic_path}")


if __name__ == "__main__":
    main()
