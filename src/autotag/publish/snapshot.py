from __future__ import annotations

import argparse
import os
import shutil
from datetime import datetime
from pathlib import Path

from autotag.db.duckdb_conn import duckdb_conn
from autotag.publish.validate import run_gating
from autotag.utils.paths import get_metabase_db_path, get_serving_db_path
from autotag.utils.time import default_business_dt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Atomic publish serving -> metabase")
    parser.add_argument("--dt", default=default_business_dt())
    return parser.parse_args()


def atomic_publish(dt: str) -> None:
    run_gating(dt)

    serving = get_serving_db_path()
    target = get_metabase_db_path()
    tmp = Path(str(target) + ".tmp")
    tmp.unlink(missing_ok=True)

    with duckdb_conn(tmp, read_only=False) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
        conn.execute(f"ATTACH '{serving.as_posix()}' AS serving (READ_ONLY)")

        objects = conn.execute(
            """
            SELECT table_name AS object_name
            FROM duckdb_tables()
            WHERE database_name = 'serving' AND schema_name = 'ops'
            UNION
            SELECT view_name AS object_name
            FROM duckdb_views()
            WHERE database_name = 'serving' AND schema_name = 'ops'
            ORDER BY object_name
            """
        ).fetchall()

        if not objects:
            raise RuntimeError("发布失败: serving.ops 下无可发布对象")

        for (name,) in objects:
            conn.execute(f'DROP TABLE IF EXISTS ops."{name}"')
            conn.execute(f'CREATE TABLE ops."{name}" AS SELECT * FROM serving.ops."{name}"')

        published_rows = conn.execute('SELECT COUNT(*) FROM ops."用户状态总览"').fetchone()[0]
        if published_rows <= 0:
            raise RuntimeError("发布失败: 临时 metabase 库用户状态总览为空")

        conn.execute("DETACH serving")

    os.replace(tmp, target)
    _backup_metabase(target)


def _backup_metabase(target: Path) -> None:
    keep = int(os.getenv("METABASE_SNAPSHOT_KEEP", "90"))
    snapshots_dir = target.parent / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    snap = snapshots_dir / f"metabase_{ts}.duckdb"
    shutil.copy2(target, snap)

    snaps = sorted(snapshots_dir.glob("metabase_*.duckdb"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in snaps[keep:]:
        old.unlink(missing_ok=True)


def main() -> None:
    args = parse_args()
    atomic_publish(args.dt)


if __name__ == "__main__":
    main()
