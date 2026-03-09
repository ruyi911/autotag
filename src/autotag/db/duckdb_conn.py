from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import duckdb


@contextmanager
def duckdb_conn(db_path: Path, read_only: bool = False):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path), read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()
