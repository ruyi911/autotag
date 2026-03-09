#!/usr/bin/env bash
export TZ=Asia/Kolkata
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

ROLLING_DAYS="${ROLLING_DAYS:-35}"
SOURCES="${REPLAY_SOURCES:-user,recharge,withdraw}"
START_DATE=""
END_DATE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days)
      ROLLING_DAYS="$2"; shift 2 ;;
    --sources)
      SOURCES="$2"; shift 2 ;;
    --start-date)
      START_DATE="$2"; shift 2 ;;
    --end-date)
      END_DATE="$2"; shift 2 ;;
    *)
      echo "Unknown arg: $1"
      exit 1 ;;
  esac
done

if [[ -z "$END_DATE" ]]; then
  END_DATE=$(PYTHONPATH=src .venv/bin/python - <<'PY'
from datetime import datetime, timedelta
from autotag.utils.time import INDIA_TZ
print((datetime.now(INDIA_TZ).date() - timedelta(days=1)).strftime("%Y-%m-%d"))
PY
)
fi

if [[ -z "$START_DATE" ]]; then
  START_DATE=$(END_DATE="$END_DATE" ROLLING_DAYS="$ROLLING_DAYS" PYTHONPATH=src .venv/bin/python - <<'PY'
import os
from datetime import timedelta
from autotag.utils.time import parse_date
end_date = parse_date(os.environ["END_DATE"])
days = int(os.environ["ROLLING_DAYS"])
print((end_date - timedelta(days=days-1)).strftime("%Y-%m-%d"))
PY
)
fi

echo "weekly_replay start=${START_DATE} end=${END_DATE} sources=${SOURCES}"

for dt in $(START_DATE="$START_DATE" END_DATE="$END_DATE" PYTHONPATH=src .venv/bin/python - <<'PY'
import os
from autotag.utils.time import iter_dates
for d in iter_dates(os.environ["START_DATE"], os.environ["END_DATE"]):
    print(d)
PY
); do
  echo "weekly_replay download+manifest dt=${dt}"
  PYTHONPATH=src .venv/bin/python -m autotag.ingest.downloader --dt "$dt" --mode replay --fetch --sources "$SOURCES"
done

echo "weekly_replay batch load start=${START_DATE} end=${END_DATE}"
PYTHONPATH=src .venv/bin/python -m autotag.load.raw_import --start-date "$START_DATE" --end-date "$END_DATE"
PYTHONPATH=src .venv/bin/python -m autotag.load.normalize --dt "$END_DATE"
PYTHONPATH=src .venv/bin/python -m autotag.load.build_mart --dt "$END_DATE"
PYTHONPATH=src .venv/bin/python -m autotag.model.features --dt "$END_DATE"
PYTHONPATH=src .venv/bin/python -m autotag.model.labeling --dt "$END_DATE"
PYTHONPATH=src .venv/bin/python -m autotag.model.views_ops --dt "$END_DATE"
PYTHONPATH=src .venv/bin/python -m autotag.publish.validate --dt "$END_DATE"
PYTHONPATH=src .venv/bin/python -m pytest tests/test_publish_gating.py -q
PYTHONPATH=src .venv/bin/python -m autotag.publish.snapshot --dt "$END_DATE"
