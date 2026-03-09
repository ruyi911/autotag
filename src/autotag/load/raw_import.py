from __future__ import annotations

import argparse
import csv
import tempfile
from pathlib import Path

import duckdb
import yaml

from autotag.db.duckdb_conn import duckdb_conn
from autotag.ingest.manifest import read_manifest
from autotag.utils.paths import get_config_path, get_serving_db_path, get_source_config_path
from autotag.utils.time import default_business_dt, iter_dates


def _load_sources() -> list[str]:
    with open(get_config_path("pipelines.yaml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["pipelines"]["daily"]["sources"]


def _headers(source: str) -> list[str]:
    with open(get_source_config_path(source), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["headers"]


def _sanitize_to_temp_csv(src: Path, headers: list[str]) -> tuple[Path, int]:
    rows = 0
    fd, temp_path = tempfile.mkstemp(prefix="autotag_", suffix=".csv")
    Path(temp_path).unlink(missing_ok=True)
    out = Path(temp_path)
    with open(src, "r", encoding="utf-8-sig", newline="") as fin, open(
        out, "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        try:
            next(reader)
        except StopIteration:
            writer.writerow(headers)
            return out, 0

        writer.writerow(headers)
        n = len(headers)
        for row in reader:
            if not row:
                continue
            if len(row) < n:
                row = row + [""] * (n - len(row))
            elif len(row) > n:
                row = row[:n]
            writer.writerow(row)
            rows += 1

    return out, rows


def _create_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS stg")
    conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops_config")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")


def _ensure_manifest_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.manifest_files (
          dt DATE,
          source VARCHAR,
          task_variant VARCHAR,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          filename VARCHAR,
          original_filename VARCHAR,
          hash VARCHAR,
          rows BIGINT,
          source_path VARCHAR,
          archived_path VARCHAR,
          created_at TIMESTAMP,
          inserted_at TIMESTAMP DEFAULT now()
        )
        """
    )


def _ensure_raw_table(conn: duckdb.DuckDBPyConnection, source: str, headers: list[str]) -> None:
    columns = ",\n".join([f'"{h}" VARCHAR' for h in headers])
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS raw.raw_{source} (
          {columns},
          dt DATE,
          source_file VARCHAR,
          file_hash VARCHAR,
          ingested_at TIMESTAMP DEFAULT now()
        )
        """
    )


def _file_loaded(conn: duckdb.DuckDBPyConnection, dt: str, source: str, file_hash: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM raw.manifest_files
        WHERE dt = ?::DATE AND source = ? AND hash = ?
        LIMIT 1
        """,
        [dt, source, file_hash],
    ).fetchone()
    return row is not None


def _ensure_manifest_columns(conn: duckdb.DuckDBPyConnection) -> None:
    for sql in [
        "ALTER TABLE raw.manifest_files ADD COLUMN IF NOT EXISTS task_variant VARCHAR",
        "ALTER TABLE raw.manifest_files ADD COLUMN IF NOT EXISTS window_start TIMESTAMP",
        "ALTER TABLE raw.manifest_files ADD COLUMN IF NOT EXISTS window_end TIMESTAMP",
        "ALTER TABLE raw.manifest_files ADD COLUMN IF NOT EXISTS original_filename VARCHAR",
        "ALTER TABLE raw.manifest_files ADD COLUMN IF NOT EXISTS source_path VARCHAR",
    ]:
        conn.execute(sql)


def _import_manifest_item(conn: duckdb.DuckDBPyConnection, item: dict) -> None:
    source = item["source"]
    dt = item["dt"]
    file_hash = item["hash"]
    archived_path = Path(item["archived_path"])

    if _file_loaded(conn, dt=dt, source=source, file_hash=file_hash):
        return

    headers = _headers(source)
    _ensure_raw_table(conn, source=source, headers=headers)

    temp_csv, parsed_rows = _sanitize_to_temp_csv(archived_path, headers=headers)
    try:
        temp_path_sql = str(temp_csv).replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE TEMP VIEW temp_csv AS "
            f"SELECT * FROM read_csv('{temp_path_sql}', "
            f"header=true, all_varchar=true, delim=',', quote='\"', escape='\"', "
            f"strict_mode=false, null_padding=true, parallel=false)"
        )

        select_cols = ", ".join([f'"{h}"' for h in headers])
        conn.execute(
            f"""
            INSERT INTO raw.raw_{source} ({select_cols}, dt, source_file, file_hash)
            SELECT {select_cols}, ?::DATE, ?, ?
            FROM temp_csv
            """,
            [dt, item["filename"], file_hash],
        )

        conn.execute(
            """
            INSERT INTO raw.manifest_files (
              dt, source, task_variant, window_start, window_end, filename, original_filename,
              hash, rows, source_path, archived_path, created_at
            )
            VALUES (?::DATE, ?, ?, NULLIF(?, '')::TIMESTAMP, NULLIF(?, '')::TIMESTAMP, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP)
            """,
            [
                dt,
                source,
                item.get("task_variant", "unknown"),
                item.get("window_start", ""),
                item.get("window_end", ""),
                item["filename"],
                item.get("original_filename", item["filename"]),
                file_hash,
                parsed_rows,
                item.get("source_path", ""),
                str(archived_path),
                item["created_at"],
            ],
        )
    finally:
        temp_csv.unlink(missing_ok=True)


def _upsert_thresholds(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_config.thresholds (
          key VARCHAR PRIMARY KEY,
          value DOUBLE,
          updated_at TIMESTAMP DEFAULT now()
        )
        """
    )
    with open(get_config_path("ops_thresholds.yaml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    thresholds = cfg.get("thresholds", {})
    for key, value in thresholds.items():
        conn.execute(
            """
            INSERT INTO ops_config.thresholds(key, value, updated_at)
            VALUES (?, ?, now())
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            [key, float(value)],
        )


def _run_for_dt(conn: duckdb.DuckDBPyConnection, dt: str) -> None:
    items = read_manifest(dt)
    for item in items:
        _import_manifest_item(conn, item=item)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import raw csv to serving duckdb")
    parser.add_argument("--dt", default=default_business_dt())
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = get_serving_db_path()
    with duckdb_conn(db_path) as conn:
        _create_schemas(conn)
        _ensure_manifest_table(conn)
        _ensure_manifest_columns(conn)
        _upsert_thresholds(conn)

        if args.start_date and args.end_date:
            for dt in iter_dates(args.start_date, args.end_date):
                _run_for_dt(conn, dt=dt)
        else:
            _run_for_dt(conn, dt=args.dt)


if __name__ == "__main__":
    main()
