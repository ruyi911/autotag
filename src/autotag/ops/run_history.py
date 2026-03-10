from __future__ import annotations

import argparse
import json
import uuid

from autotag.db.duckdb_conn import duckdb_conn
from autotag.utils.paths import get_serving_db_path


def _ensure_table(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops_config")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_config.run_history (
          run_id VARCHAR PRIMARY KEY,
          dt DATE,
          mode VARCHAR,
          started_at TIMESTAMP,
          ended_at TIMESTAMP,
          status VARCHAR,
          failed_step VARCHAR,
          message VARCHAR,
          source_success_json VARCHAR,
          source_fail_json VARCHAR,
          task_variant_success_json VARCHAR,
          task_variant_fail_json VARCHAR,
          runtime_config_json VARCHAR,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          created_at TIMESTAMP DEFAULT now(),
          updated_at TIMESTAMP DEFAULT now()
        )
        """
    )
    for sql in [
        "ALTER TABLE ops_config.run_history ADD COLUMN IF NOT EXISTS task_variant_success_json VARCHAR",
        "ALTER TABLE ops_config.run_history ADD COLUMN IF NOT EXISTS task_variant_fail_json VARCHAR",
        "ALTER TABLE ops_config.run_history ADD COLUMN IF NOT EXISTS runtime_config_json VARCHAR",
        "ALTER TABLE ops_config.run_history ADD COLUMN IF NOT EXISTS window_start TIMESTAMP",
        "ALTER TABLE ops_config.run_history ADD COLUMN IF NOT EXISTS window_end TIMESTAMP",
        "ALTER TABLE ops_config.run_history ADD COLUMN IF NOT EXISTS mode VARCHAR",
    ]:
        conn.execute(sql)


def start_run(dt: str, mode: str = "daily", run_id: str | None = None) -> str:
    rid = run_id or uuid.uuid4().hex
    with duckdb_conn(get_serving_db_path()) as conn:
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO ops_config.run_history (run_id, dt, mode, started_at, status, updated_at)
            VALUES (?, ?::DATE, ?, now(), 'RUNNING', now())
            ON CONFLICT(run_id) DO UPDATE
              SET dt = excluded.dt,
                  mode = excluded.mode,
                  started_at = excluded.started_at,
                  status = excluded.status,
                  updated_at = excluded.updated_at
            """,
            [rid, dt, mode],
        )
    return rid


def finish_run(
    run_id: str,
    status: str,
    failed_step: str | None,
    message: str | None,
    source_success_json: str | None,
    source_fail_json: str | None,
    task_variant_success_json: str | None = None,
    task_variant_fail_json: str | None = None,
    runtime_config_json: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> None:
    with duckdb_conn(get_serving_db_path()) as conn:
        _ensure_table(conn)
        conn.execute(
            """
            UPDATE ops_config.run_history
            SET ended_at = now(),
                status = ?,
                failed_step = ?,
                message = ?,
                source_success_json = ?,
                source_fail_json = ?,
                task_variant_success_json = ?,
                task_variant_fail_json = ?,
                runtime_config_json = ?,
                window_start = NULLIF(?, '')::TIMESTAMP,
                window_end = NULLIF(?, '')::TIMESTAMP,
                updated_at = now()
            WHERE run_id = ?
            """,
            [
                status,
                failed_step,
                message,
                source_success_json,
                source_fail_json,
                task_variant_success_json,
                task_variant_fail_json,
                runtime_config_json,
                window_start or "",
                window_end or "",
                run_id,
            ],
        )


def get_last_success_dt() -> str:
    with duckdb_conn(get_serving_db_path()) as conn:
        _ensure_table(conn)
        row = conn.execute(
            """
            SELECT CAST(MAX(dt) AS VARCHAR)
            FROM ops_config.run_history
            WHERE status = 'SUCCESS'
            """
        ).fetchone()
    return row[0] if row and row[0] else ""


def _read_status_json(path: str | None) -> str | None:
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return None


def _extract_field(path: str | None, key: str) -> str | None:
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        val = obj.get(key)
        if val is None:
            return None
        if isinstance(val, (dict, list)):
            return json.dumps(val, ensure_ascii=False)
        return str(val)
    except Exception:
        return None


def _cli() -> None:
    parser = argparse.ArgumentParser(description="run history ctl")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start")
    p_start.add_argument("--dt", required=True)
    p_start.add_argument("--mode", default="daily")
    p_start.add_argument("--run-id")

    p_finish = sub.add_parser("finish")
    p_finish.add_argument("--run-id", required=True)
    p_finish.add_argument("--status", required=True)
    p_finish.add_argument("--failed-step")
    p_finish.add_argument("--message")
    p_finish.add_argument("--source-success-file")
    p_finish.add_argument("--source-fail-file")
    p_finish.add_argument("--status-file")
    p_finish.add_argument("--runtime-config-file")

    sub.add_parser("last-success")

    args = parser.parse_args()
    if args.cmd == "start":
        print(start_run(dt=args.dt, mode=args.mode, run_id=args.run_id))
        return
    if args.cmd == "finish":
        finish_run(
            run_id=args.run_id,
            status=args.status,
            failed_step=args.failed_step,
            message=args.message,
            source_success_json=_read_status_json(args.source_success_file),
            source_fail_json=_read_status_json(args.source_fail_file),
            task_variant_success_json=_extract_field(args.status_file, "task_variant_success"),
            task_variant_fail_json=_extract_field(args.status_file, "task_variant_fail"),
            runtime_config_json=_read_status_json(args.runtime_config_file),
            window_start=_extract_field(args.status_file, "window_start"),
            window_end=_extract_field(args.status_file, "window_end"),
        )
        return
    if args.cmd == "last-success":
        print(get_last_success_dt())


if __name__ == "__main__":
    _cli()
