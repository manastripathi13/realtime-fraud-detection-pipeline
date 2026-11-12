"""Rule engine for fraud scoring.

Kept separate from the Beam code so it is plain Python and easy to unit test.
The engine reloads `config/rules.yaml` when the file changes, which lets you
update rules while the streaming pipeline keeps running.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

import yaml


@dataclass
class ScoreResult:
    score: int
    is_alert: bool
    reasons: list[str] = field(default_factory=list)
    rules_version: int = 0


class RuleEngine:
    def __init__(self, path: str):
        self.path = path
        self._mtime = 0.0
        self._last_check = 0.0
        self.cfg: dict = {}
        self._load()

    def _load(self) -> None:
        with open(self.path) as f:
            self.cfg = yaml.safe_load(f)
        self._mtime = os.path.getmtime(self.path)

    def maybe_reload(self) -> bool:
        """Reload rules if the file changed. Checked at most every `reload_seconds`."""
        now = time.time()
        if now - self._last_check < self.cfg.get("reload_seconds", 15):
            return False
        self._last_check = now
        if os.path.getmtime(self.path) != self._mtime:
            self._load()
            return True
        return False

    def score(self, txn: dict) -> ScoreResult:
        """Score one enriched transaction.

        Expected keys: amount, avg_amount, home_country, ip_country,
        is_known_device, card_txn_count_window.
        """
        rules = self.cfg["rules"]
        total, reasons = 0, []

        def hit(name: str) -> None:
            nonlocal total
            total += rules[name]["score"]
            reasons.append(name)

        r = rules.get("velocity", {})
        if r.get("enabled") and txn["card_txn_count_window"] > r["max_txns"]:
            hit("velocity")

        r = rules.get("amount_spike", {})
        if r.get("enabled") and txn.get("avg_amount") and txn["amount"] > r["multiplier"] * txn["avg_amount"]:
            hit("amount_spike")

        r = rules.get("geo_mismatch", {})
        if r.get("enabled") and txn.get("home_country") and txn["ip_country"] != txn["home_country"]:
            hit("geo_mismatch")

        r = rules.get("card_testing", {})
        if r.get("enabled") and txn["amount"] <= r["max_amount"]:
            hit("card_testing")

        r = rules.get("new_device_high_value", {})
        if r.get("enabled") and not txn["is_known_device"] and txn["amount"] >= r["min_amount"]:
            hit("new_device_high_value")

        return ScoreResult(
            score=total,
            is_alert=total >= self.cfg["alert_threshold"],
            reasons=reasons,
            rules_version=self.cfg.get("version", 0),
        )
