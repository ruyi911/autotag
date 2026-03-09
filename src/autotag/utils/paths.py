from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "configs"
SQL_DIR = PROJECT_ROOT / "src" / "autotag" / "db" / "sql"


def get_dropbox_dir() -> Path:
    return Path(os.getenv("DROPBOX_PATH", DATA_DIR / "dropbox")).resolve()


def get_initial_csv_dir() -> Path:
    return Path(os.getenv("INITIAL_CSV_PATH", DATA_DIR / "initial_csv")).resolve()


def get_raw_files_dir(dt: str | None = None) -> Path:
    path = Path(os.getenv("RAW_FILES_PATH", DATA_DIR / "raw_files")).resolve()
    return path / f"dt={dt}" if dt else path


def get_manifests_dir(dt: str | None = None) -> Path:
    path = Path(os.getenv("MANIFESTS_PATH", DATA_DIR / "manifests")).resolve()
    return path / f"dt={dt}" if dt else path


def get_db_path(name: str) -> Path:
    base = DATA_DIR / "db"
    if name.endswith(".duckdb"):
        return (base / name).resolve()
    return (base / f"{name}.duckdb").resolve()


def get_serving_db_path() -> Path:
    return Path(os.getenv("DB_PATH", get_db_path("serving"))).resolve()


def get_metabase_db_path() -> Path:
    return Path(os.getenv("METABASE_DB_PATH", get_db_path("metabase"))).resolve()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def get_source_config_path(source: str) -> Path:
    return (CONFIG_DIR / "sources" / f"{source}.yaml").resolve()


def get_config_path(filename: str) -> Path:
    return (CONFIG_DIR / filename).resolve()


def get_log_dir() -> Path:
    return Path(os.getenv("LOGS_PATH", PROJECT_ROOT / "logs" / "daily")).resolve()
