#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

KEEP_DAYS="${1:-${LOG_KEEP_DAYS:-90}}"
TARGET_DIR="logs/daily"
SNAP_DIR="data/db/snapshots"
SNAP_KEEP="${METABASE_SNAPSHOT_KEEP:-90}"

if [[ -d "$TARGET_DIR" ]]; then
  find "$TARGET_DIR" -type f -name 'dt=*.log' -mtime +"$KEEP_DAYS" -print -delete
  find "$TARGET_DIR" -type f -name 'source_*.json' -mtime +"$KEEP_DAYS" -print -delete
fi

if [[ -d "$SNAP_DIR" ]]; then
  find "$SNAP_DIR" -type f -name 'metabase_*.duckdb' -mtime +"$KEEP_DAYS" -print -delete
  SNAP_DIR_ENV="$SNAP_DIR" SNAP_KEEP_ENV="$SNAP_KEEP" python3 - <<'PY'
import os
from pathlib import Path

snap_dir = Path(os.environ["SNAP_DIR_ENV"])
keep = int(os.environ["SNAP_KEEP_ENV"])
snaps = sorted(snap_dir.glob("metabase_*.duckdb"), key=lambda p: p.stat().st_mtime, reverse=True)
for old in snaps[keep:]:
    print(str(old))
    old.unlink(missing_ok=True)
PY
fi
