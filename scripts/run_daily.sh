#!/usr/bin/env bash
export TZ=Asia/Kolkata
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

exec env PYTHONPATH=src .venv/bin/python -m autotag.ops.pipeline_runner "$@"
