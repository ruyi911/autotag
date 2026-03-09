from __future__ import annotations

import argparse

from autotag.db.duckdb_conn import duckdb_conn
from autotag.utils.paths import PROJECT_ROOT, get_serving_db_path
from autotag.utils.time import default_business_dt


def _run_sql_file(conn, rel_path: str, dt: str) -> None:
    sql_path = PROJECT_ROOT / "src" / "autotag" / "db" / "sql" / rel_path
    sql = sql_path.read_text(encoding="utf-8")
    if "?" in sql:
        safe_dt = dt.replace("'", "")
        sql = sql.replace("?", f"DATE '{safe_dt}'")
    conn.execute(sql)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build labels")
    parser.add_argument("--dt", default=default_business_dt())
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with duckdb_conn(get_serving_db_path()) as conn:
        _run_sql_file(conn, "mart/build_labels.sql", args.dt)


if __name__ == "__main__":
    main()
