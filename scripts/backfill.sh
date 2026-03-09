#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

START_DT=""
END_DT=""
ONLY_SOURCES=""
SKIP_DOWNLOAD=0
FROM_LAST_SUCCESS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-date)
      START_DT="$2"; shift 2 ;;
    --end-date)
      END_DT="$2"; shift 2 ;;
    --only-sources)
      ONLY_SOURCES="$2"; shift 2 ;;
    --skip-download)
      SKIP_DOWNLOAD=1; shift ;;
    --from-last-success)
      FROM_LAST_SUCCESS=1; shift ;;
    *)
      echo "Unknown arg: $1"
      echo "Usage: scripts/backfill.sh --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--only-sources a,b] [--skip-download] [--from-last-success]"
      exit 1 ;;
  esac
done

if [[ "$FROM_LAST_SUCCESS" -eq 1 ]]; then
  LAST_SUCCESS=$(PYTHONPATH=src .venv/bin/python -m autotag.ops.run_history last-success || true)
  if [[ -n "$LAST_SUCCESS" ]]; then
    START_DT=$(PYTHONPATH=src .venv/bin/python - <<PY
from datetime import datetime, timedelta
print((datetime.strptime('${LAST_SUCCESS}','%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'))
PY
)
    echo "from-last-success enabled, start_date=${START_DT}"
  fi
fi

if [[ -z "$START_DT" || -z "$END_DT" ]]; then
  echo "Usage: scripts/backfill.sh --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--only-sources a,b] [--skip-download] [--from-last-success]"
  exit 1
fi

DTS=$(PYTHONPATH=src .venv/bin/python - <<PY
from autotag.utils.time import iter_dates
for d in iter_dates('${START_DT}', '${END_DT}'):
    print(d)
PY
)

for dt in $DTS; do
  echo "running dt=${dt}"
  ARGS=("$dt")
  if [[ "$SKIP_DOWNLOAD" -eq 1 ]]; then
    ARGS+=("--skip-download")
  fi
  if [[ -n "$ONLY_SOURCES" ]]; then
    ARGS+=("--sources" "$ONLY_SOURCES")
  fi
  scripts/run_daily.sh "${ARGS[@]}"
done
