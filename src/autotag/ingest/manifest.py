from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from pathlib import Path

from autotag.utils.paths import get_manifests_dir


@dataclass
class ManifestItem:
    dt: str
    source: str
    task_variant: str
    window_start: str
    window_end: str
    filename: str
    original_filename: str
    source_path: str
    archived_path: str
    hash: str
    rows: int
    created_at: str


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def count_csv_rows(path: Path) -> int:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def write_manifest(dt: str, items: list[ManifestItem]) -> Path:
    out_dir = get_manifests_dir(dt)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "manifest.json"
    payload = {
        "dt": dt,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "items": [asdict(item) for item in items],
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path


def read_manifest(dt: str) -> list[dict]:
    path = get_manifests_dir(dt) / "manifest.json"
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("items", [])
