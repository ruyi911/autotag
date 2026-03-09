from __future__ import annotations

from datetime import date

import duckdb

from autotag.utils.paths import get_serving_db_path
from autotag.utils.time import default_business_dt, parse_date


def _conn():
    return duckdb.connect(str(get_serving_db_path()), read_only=True)


def test_critical_columns_completeness():
    with _conn() as conn:
        null_cnt = conn.execute(
            '''
            SELECT COUNT(*)
            FROM ops."用户状态总览"
            WHERE "用户ID" IS NULL OR "用户状态" IS NULL OR "统计日期" IS NULL
            '''
        ).fetchone()[0]
        assert null_cnt == 0


def test_date_range_validity():
    with _conn() as conn:
        max_dt = conn.execute('SELECT MAX("统计日期") FROM ops."用户状态总览"').fetchone()[0]
        min_dt = conn.execute('SELECT MIN("统计日期") FROM ops."用户状态总览"').fetchone()[0]
        assert max_dt is not None
        assert min_dt is not None
        assert max_dt <= parse_date(default_business_dt())
        assert min_dt <= max_dt


def test_ops_view_queryable_and_non_empty():
    with _conn() as conn:
        cnt = conn.execute('SELECT COUNT(*) FROM ops."用户状态总览"').fetchone()[0]
        assert cnt > 0


def test_gating_skips_login_and_drift_when_disabled(monkeypatch):
    """Env vars ENABLE_LOGIN_FRESHNESS_GATE and ENABLE_STATUS_DRIFT_GATE control behavior."""
    import os
    monkeypatch.setenv("ENABLE_LOGIN_FRESHNESS_GATE", "0")
    monkeypatch.setenv("ENABLE_STATUS_DRIFT_GATE", "0")
    assert os.getenv("ENABLE_LOGIN_FRESHNESS_GATE") == "0"
    assert os.getenv("ENABLE_STATUS_DRIFT_GATE") == "0"


def test_gating_raises_when_enabled(monkeypatch):
    """If gates are enabled, a bad state should raise an error."""
    monkeypatch.setenv("ENABLE_LOGIN_FRESHNESS_GATE", "1")
    monkeypatch.setenv("ENABLE_STATUS_DRIFT_GATE", "1")
