import os
import sys
import time

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
from rules import RuleEngine  # noqa: E402

RULES = os.path.join(os.path.dirname(__file__), "..", "config", "rules.yaml")


def txn(**over):
    base = dict(amount=150.0, avg_amount=200.0, home_country="IN", ip_country="IN",
                is_known_device=True, card_txn_count_window=1)
    base.update(over)
    return base


def test_normal_transaction_is_not_an_alert():
    res = RuleEngine(RULES).score(txn())
    assert res.score == 0 and not res.is_alert


def test_amount_spike_alone_raises_alert():
    res = RuleEngine(RULES).score(txn(amount=1200))
    assert "amount_spike" in res.reasons and res.is_alert


def test_card_testing_from_abroad_raises_alert():
    res = RuleEngine(RULES).score(txn(amount=1.5, ip_country="US"))
    assert set(res.reasons) == {"card_testing", "geo_mismatch"} and res.is_alert


def test_velocity_needs_more_than_max_txns():
    eng = RuleEngine(RULES)
    assert "velocity" not in eng.score(txn(card_txn_count_window=5)).reasons
    assert "velocity" in eng.score(txn(card_txn_count_window=6)).reasons


def test_rules_hot_reload(tmp_path):
    cfg = yaml.safe_load(open(RULES))
    cfg["reload_seconds"] = 0
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(cfg))
    eng = RuleEngine(str(path))
    assert eng.score(txn(amount=1200)).is_alert

    cfg["rules"]["amount_spike"]["enabled"] = False      # "deploy" a rule change
    cfg["version"] = 2
    time.sleep(0.01)
    path.write_text(yaml.safe_dump(cfg))
    os.utime(path, (time.time() + 1, time.time() + 1))   # make sure mtime changes

    assert eng.maybe_reload() is True
    res = eng.score(txn(amount=1200))
    assert not res.is_alert and res.rules_version == 2
