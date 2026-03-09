from __future__ import annotations

import duckdb

from autotag.utils.paths import get_serving_db_path


def test_label_values_in_expected_set():
    with duckdb.connect(str(get_serving_db_path()), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT state_4
            FROM mart.user_state_engine
            """
        ).fetchall()

    allowed = {"健康稳定", "变慢预警", "高危流失", "已流失", "未激活", "只充不玩", "只玩不充"}
    assert all(r[0] in allowed for r in rows if r[0] is not None)
