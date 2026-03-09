from __future__ import annotations

from pathlib import Path

import yaml


SOURCE_FILES = ["user", "recharge", "bet", "withdraw", "bonus"]


def test_source_configs_exist_and_have_headers():
    root = Path(__file__).resolve().parents[1]
    for name in SOURCE_FILES:
        path = root / "configs" / "sources" / f"{name}.yaml"
        assert path.exists()
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert cfg.get("name") == name
        assert isinstance(cfg.get("headers"), list)
        assert len(cfg["headers"]) > 0
        assert isinstance(cfg.get("primary_key"), list)
        assert cfg.get("business_date_field") in cfg["headers"]


def test_thresholds_exist():
    root = Path(__file__).resolve().parents[1]
    path = root / "configs" / "ops_thresholds.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    keys = cfg["thresholds"].keys()
    for k in [
        "stable_b_gap_threshold",
        "stable_r_gap_threshold",
        "lost_b_gap_threshold",
        "lost_r_gap_threshold",
        "bet_drop_threshold",
        "rech_drop_threshold",
        "wd_rate_long_threshold",
        "pay_friction_fail_rate_threshold",
    ]:
        assert k in keys
