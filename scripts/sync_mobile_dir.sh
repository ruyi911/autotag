#!/usr/bin/env bash
export TZ=Asia/Kolkata
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/sync_mobile_dir.sh <directory> [--no-recursive]"
  exit 1
fi

DIR="$1"
shift

exec env PYTHONPATH=src .venv/bin/python -m autotag.ingest.mobile_sync import-dir --dir "$DIR" "$@"
