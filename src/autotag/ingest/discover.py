from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

import yaml

from autotag.utils.paths import get_initial_csv_dir, get_dropbox_dir, get_source_config_path


def _load_source_cfg(source: str) -> dict:
    with open(get_source_config_path(source), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _iter_csv_files(base: Path) -> list[Path]:
    if not base.exists():
        return []
    return sorted([p for p in base.rglob("*.csv") if p.is_file()])


def _normalize_cols(cols: list[str]) -> list[str]:
    return [c.replace("\ufeff", "").strip() for c in cols]


def _read_header(path: Path) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            return _normalize_cols(header)
    except Exception:
        return []


def _header_match(path: Path, expected_headers: list[str]) -> bool:
    header = _read_header(path)
    if not header:
        return False
    expect = _normalize_cols(expected_headers)
    return header[: len(expect)] == expect


def discover_source_files(
    source: str,
    include_initial: bool = True,
    include_dropbox: bool = True,
    require_header_match: bool = False,
) -> list[Path]:
    cfg = _load_source_cfg(source)
    folder_hints = [h.lower() for h in cfg.get("folder_hints", [])]
    file_hints = [h.lower() for h in cfg.get("file_hints", [])]
    headers = cfg.get("headers", [])

    candidates: list[Path] = []
    bases = []
    if include_dropbox:
        bases.append(get_dropbox_dir())
    if include_initial:
        bases.append(get_initial_csv_dir())

    for base in bases:
        if base is None or not base.exists():
            continue
        for p in _iter_csv_files(base):
            key = f"{str(p.parent).lower()} {p.name.lower()}"
            hinted = any(h in key for h in folder_hints + file_hints)
            matched = _header_match(p, headers) if headers else False
            if require_header_match:
                if matched:
                    candidates.append(p)
            elif hinted or matched:
                candidates.append(p)

    uniq = sorted(set(candidates))
    return uniq


def discover_files(
    sources: Iterable[str],
    include_initial: bool = True,
    include_dropbox: bool = True,
    require_header_match: bool = False,
) -> dict[str, list[Path]]:
    result: dict[str, list[Path]] = {}
    for source in sources:
        result[source] = discover_source_files(
            source=source,
            include_initial=include_initial,
            include_dropbox=include_dropbox,
            require_header_match=require_header_match,
        )
    return result
