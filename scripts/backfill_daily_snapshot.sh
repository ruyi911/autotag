#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

START_DATE="${1:-2026-02-02}"
END_DATE="${2:-$(PYTHONPATH=src .venv/bin/python - <<'PY'
from autotag.utils.time import default_business_dt
print(default_business_dt())
PY
)}"

echo "daily snapshot backfill: start=${START_DATE} end=${END_DATE}"
echo "mode: reuse local manifests (--skip-download)"

bash scripts/backfill.sh \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --skip-download
