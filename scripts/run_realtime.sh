#!/usr/bin/env bash
export TZ=Asia/Kolkata
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

DT=""
SOURCES="${REALTIME_SOURCES:-user,recharge,withdraw,bet,bonus}"
PUBLISH_EVERY_2H="${REALTIME_PUBLISH_EVERY_2H:-1}"
FORCE_PUBLISH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dt)
      DT="$2"; shift 2 ;;
    --sources)
      SOURCES="$2"; shift 2 ;;
    --force-publish)
      FORCE_PUBLISH=1; shift ;;
    *)
      echo "Unknown arg: $1"
      exit 1 ;;
  esac
done

if [[ -z "$DT" ]]; then
  DT=$(PYTHONPATH=src .venv/bin/python - <<'PY'
from datetime import datetime
from autotag.utils.time import INDIA_TZ
print(datetime.now(INDIA_TZ).strftime("%Y-%m-%d"))
PY
)
fi

echo "run_realtime dt=${DT} sources=${SOURCES}"
bash scripts/run_daily.sh "$DT" --mode realtime --no-publish --sources "$SOURCES"

if [[ "$FORCE_PUBLISH" -eq 1 ]]; then
  echo "run_realtime force publish"
  bash scripts/run_daily.sh "$DT" --mode realtime --skip-download
  exit 0
fi

if [[ "$PUBLISH_EVERY_2H" == "1" ]]; then
  HOUR_NOW=$(PYTHONPATH=src .venv/bin/python - <<'PY'
from datetime import datetime
from autotag.utils.time import INDIA_TZ
print(datetime.now(INDIA_TZ).hour)
PY
)
  if (( HOUR_NOW % 2 == 0 )); then
    echo "run_realtime scheduled publish at even hour=${HOUR_NOW}"
    bash scripts/run_daily.sh "$DT" --mode realtime --skip-download
  else
    echo "run_realtime skip publish at hour=${HOUR_NOW}"
  fi
fi
