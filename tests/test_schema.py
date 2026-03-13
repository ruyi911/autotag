from __future__ import annotations

from pathlib import Path

from autotag.db.duckdb_conn import duckdb_conn
from autotag.ingest.mobile_sync import (
    _ensure_mobile_table,
    _read_records_from_file,
    _read_rows_from_xlsx,
    _upsert_mobile_records,
    _write_rows_to_xlsx,
    find_missing_login_phone_user_ids,
)
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


def _init_mobile_sync_raw_tables(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE raw.manifest_files (
          dt DATE,
          source VARCHAR,
          task_variant VARCHAR,
          filename VARCHAR,
          hash VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw.raw_user (
          "ID" VARCHAR,
          "手机号" VARCHAR,
          dt DATE,
          source_file VARCHAR,
          file_hash VARCHAR
        )
        """
    )


def test_mobile_sync_find_missing_login_phone_user_ids(tmp_path: Path):
    db_path = tmp_path / "serving.duckdb"
    with duckdb_conn(db_path) as conn:
        _init_mobile_sync_raw_tables(conn)
        conn.executemany(
            "INSERT INTO raw.manifest_files (dt, source, task_variant, filename, hash) VALUES (?, ?, ?, ?, ?)",
            [
                ("2026-03-10", "user", "user_login_daily", "a.csv", "ha"),
                ("2026-03-10", "user", "user_reg_daily", "b.csv", "hb"),
                ("2026-03-10", "user", "user_login_realtime", "c.csv", "hc"),
            ],
        )
        conn.executemany(
            "INSERT INTO raw.raw_user (\"ID\", \"手机号\", dt, source_file, file_hash) VALUES (?, ?, ?, ?, ?)",
            [
                ("'100", "-", "2026-03-10", "a.csv", "ha"),
                ("'101", "", "2026-03-10", "a.csv", "ha"),
                ("'102", "13800000000", "2026-03-10", "a.csv", "ha"),
                ("'103", "-", "2026-03-10", "b.csv", "hb"),
                ("'106", "151****8888", "2026-03-10", "b.csv", "hb"),
                ("'104", "139****0000", "2026-03-10", "a.csv", "ha"),
                ("'105", "-", "2026-03-10", "c.csv", "hc"),
            ],
        )

        daily = find_missing_login_phone_user_ids(conn, dt="2026-03-10", mode="daily")
        assert daily == ["100", "101", "103", "106"]

        daily_with_masked = find_missing_login_phone_user_ids(
            conn,
            dt="2026-03-10",
            mode="daily",
            include_masked=True,
        )
        assert daily_with_masked == ["100", "101", "103", "104", "106"]

        realtime = find_missing_login_phone_user_ids(conn, dt="2026-03-10", mode="realtime")
        assert realtime == ["105"]


def test_mobile_sync_xlsx_roundtrip_and_record_reader(tmp_path: Path):
    path = tmp_path / "mobile.xlsx"
    rows = [["1386396", "9165149378"], ["1386397", "9547151907"]]

    _write_rows_to_xlsx(rows, path)
    parsed_rows = _read_rows_from_xlsx(path)
    assert parsed_rows == rows

    records, skipped = _read_records_from_file(path)
    assert skipped == 0
    assert [(r[0], r[1]) for r in records] == [("1386396", "9165149378"), ("1386397", "9547151907")]


def test_mobile_sync_upsert_keeps_existing_nick_when_new_nick_missing(tmp_path: Path):
    db_path = tmp_path / "serving.duckdb"
    with duckdb_conn(db_path) as conn:
        _ensure_mobile_table(conn)

        n1 = _upsert_mobile_records(conn, [("100", "9165149378", "PlayerA", "api")])
        assert n1 == 1

        n2 = _upsert_mobile_records(conn, [("100", "9999999999", "", "import")])
        assert n2 == 1

        row = conn.execute(
            "SELECT user_id, mobile_number, nick_name, source FROM ops_secure.user_mobile_secure WHERE user_id='100'"
        ).fetchone()
        assert row == ("100", "9999999999", "PlayerA", "import")
