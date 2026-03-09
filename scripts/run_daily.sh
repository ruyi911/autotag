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
SKIP_DOWNLOAD=0
SOURCES=""
RUN_MODE="daily"
NO_PUBLISH=0
NON_FATAL_STEPS_CSV="${NON_FATAL_STEPS:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-download)
      SKIP_DOWNLOAD=1; shift ;;
    --sources)
      SOURCES="$2"; shift 2 ;;
    --mode)
      RUN_MODE="$2"; shift 2 ;;
    --no-publish)
      NO_PUBLISH=1; shift ;;
    *)
      if [[ -z "$DT" ]]; then
        DT="$1"
      else
        echo "Unknown arg: $1"
        exit 1
      fi
      shift ;;
  esac
done

if [[ -z "$DT" ]]; then
  DT=$(PYTHONPATH=src .venv/bin/python -c 'from autotag.utils.time import default_business_dt; print(default_business_dt())')
fi

mkdir -p logs/daily .locks
LOG_FILE="logs/daily/dt=${DT}.log"
RUN_ID=$(PYTHONPATH=src .venv/bin/python - <<'PY'
import uuid
print(uuid.uuid4().hex)
PY
)
SOURCE_STATUS_FILE="logs/daily/source_status_${DT}_${RUN_ID}.json"
SOURCE_SUCCESS_FILE="logs/daily/source_success_${DT}_${RUN_ID}.json"
SOURCE_FAIL_FILE="logs/daily/source_fail_${DT}_${RUN_ID}.json"
LOCK_FILE=".locks/run_daily.lock"
LOCK_PID_FILE=".locks/run_daily.pid"
LOCK_BACKEND=""
LAST_STEP="init"
START_TS=$(date +%s)

is_non_fatal_step() {
  local step="$1"
  if [[ -z "$NON_FATAL_STEPS_CSV" ]]; then
    return 1
  fi
  IFS=',' read -r -a arr <<< "$NON_FATAL_STEPS_CSV"
  for s in "${arr[@]}"; do
    if [[ "$(echo "$s" | xargs)" == "$step" ]]; then
      return 0
    fi
  done
  return 1
}

send_alert() {
  local subject="$1"
  local body="$2"
  SUBJECT="$subject" BODY="$body" PYTHONPATH=src .venv/bin/python - <<'PY'
import os
from autotag.utils.alert import send_alert
send_alert(os.environ["SUBJECT"], os.environ["BODY"])
PY
}

acquire_lock() {
  if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] another run is active (flock), exit" | tee -a "$LOG_FILE"
      exit 2
    fi
    LOCK_BACKEND="flock"
    return
  fi

  if [[ -f "$LOCK_PID_FILE" ]]; then
    old_pid=$(cat "$LOCK_PID_FILE" 2>/dev/null || true)
    if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] another run is active (pid=$old_pid), exit" | tee -a "$LOG_FILE"
      exit 2
    fi
  fi
  echo $$ > "$LOCK_PID_FILE"
  LOCK_BACKEND="pid"
}

cleanup_lock() {
  if [[ "$LOCK_BACKEND" == "pid" ]]; then
    rm -f "$LOCK_PID_FILE"
  fi
}

write_source_split_files() {
  if [[ -f "$SOURCE_STATUS_FILE" ]]; then
    SRC_FILE="$SOURCE_STATUS_FILE" SRC_OK="$SOURCE_SUCCESS_FILE" SRC_FAIL="$SOURCE_FAIL_FILE" PYTHONPATH=src .venv/bin/python - <<'PY'
import json
import os
src = json.load(open(os.environ["SRC_FILE"], 'r', encoding='utf-8'))
json.dump(src.get('source_success', []), open(os.environ["SRC_OK"], 'w', encoding='utf-8'), ensure_ascii=False)
json.dump(src.get('source_fail', {}), open(os.environ["SRC_FAIL"], 'w', encoding='utf-8'), ensure_ascii=False)
PY
  fi
}

on_error() {
  local code=$?
  local now
  now=$(date '+%Y-%m-%d %H:%M:%S')
  write_source_split_files
  echo "[$now] FAILED dt=${DT} run_id=${RUN_ID} step=${LAST_STEP} code=${code}" | tee -a "$LOG_FILE"

  PYTHONPATH=src .venv/bin/python -m autotag.ops.run_history finish \
    --run-id "$RUN_ID" \
    --status FAILED \
    --failed-step "$LAST_STEP" \
    --message "exit_code=$code" \
    --source-success-file "$SOURCE_SUCCESS_FILE" \
    --source-fail-file "$SOURCE_FAIL_FILE" \
    --status-file "$SOURCE_STATUS_FILE" >/dev/null 2>&1 || true

  send_alert "[AutoTag] FAILED dt=${DT} step=${LAST_STEP}" "run_id=${RUN_ID}\nstep=${LAST_STEP}\ncode=${code}\nlog=${LOG_FILE}\ndt=${DT}"
  cleanup_lock
  exit "$code"
}

trap on_error ERR
trap cleanup_lock EXIT

acquire_lock

echo "[$(date '+%Y-%m-%d %H:%M:%S')] run_daily dt=${DT} run_id=${RUN_ID} mode=${RUN_MODE} skip_download=${SKIP_DOWNLOAD} no_publish=${NO_PUBLISH} sources=${SOURCES:-all}" | tee -a "$LOG_FILE"
PYTHONPATH=src .venv/bin/python -m autotag.ops.run_history start --dt "$DT" --mode "$RUN_MODE" --run-id "$RUN_ID" >/dev/null

run_module_step() {
  local step="$1"
  local module="$2"
  shift 2
  LAST_STEP="$step"
  local start end dur
  start=$(date +%s)
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] start ${step}" | tee -a "$LOG_FILE"

  set +e
  PYTHONPATH=src .venv/bin/python -m "$module" --dt "$DT" "$@" 2>&1 | tee -a "$LOG_FILE"
  local rc=${PIPESTATUS[0]}
  set -e

  end=$(date +%s)
  dur=$((end-start))
  if [[ "$rc" -ne 0 ]]; then
    if is_non_fatal_step "$step"; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] warn ${step} failed rc=${rc}, tolerated by NON_FATAL_STEPS" | tee -a "$LOG_FILE"
      return 0
    fi
    return "$rc"
  fi

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] done ${step} duration_s=${dur}" | tee -a "$LOG_FILE"
}

if [[ "$SKIP_DOWNLOAD" -eq 0 ]]; then
  DOWNLOADER_ARGS=("--fetch" "--status-out" "$SOURCE_STATUS_FILE" "--mode" "$RUN_MODE")
  if [[ -n "$SOURCES" ]]; then
    DOWNLOADER_ARGS+=("--sources" "$SOURCES")
  fi
  run_module_step "ingest.downloader" "autotag.ingest.downloader" "${DOWNLOADER_ARGS[@]}"
  write_source_split_files
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip ingest.downloader by --skip-download" | tee -a "$LOG_FILE"
  echo "[]" > "$SOURCE_SUCCESS_FILE"
  echo "{}" > "$SOURCE_FAIL_FILE"
  echo "{\"mode\":\"${RUN_MODE}\",\"task_variant_success\":[],\"task_variant_fail\":[],\"window_start\":\"\",\"window_end\":\"\",\"source_success\":[],\"source_fail\":{}}" > "$SOURCE_STATUS_FILE"
fi

run_module_step "load.raw_import" "autotag.load.raw_import"
run_module_step "load.normalize" "autotag.load.normalize"
run_module_step "load.build_mart" "autotag.load.build_mart"
run_module_step "model.features" "autotag.model.features"
run_module_step "model.labeling" "autotag.model.labeling"
run_module_step "model.views_ops" "autotag.model.views_ops"
if [[ "$NO_PUBLISH" -eq 0 ]]; then
  run_module_step "publish.validate" "autotag.publish.validate"
fi

if [[ "$NO_PUBLISH" -eq 0 ]]; then
  LAST_STEP="pytest.publish_gating"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] start pytest publish gating" | tee -a "$LOG_FILE"
  set +e
  PYTHONPATH=src .venv/bin/python -m pytest tests/test_publish_gating.py -q 2>&1 | tee -a "$LOG_FILE"
  PYTEST_RC=${PIPESTATUS[0]}
  set -e
  if [[ "$PYTEST_RC" -ne 0 ]]; then
    if is_non_fatal_step "$LAST_STEP"; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] warn pytest failed rc=${PYTEST_RC}, tolerated" | tee -a "$LOG_FILE"
    else
      exit "$PYTEST_RC"
    fi
  fi
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] done pytest publish gating" | tee -a "$LOG_FILE"
fi

if [[ "$NO_PUBLISH" -eq 0 ]]; then
  run_module_step "publish.snapshot" "autotag.publish.snapshot"
fi

PYTHONPATH=src .venv/bin/python -m autotag.ops.run_history finish \
  --run-id "$RUN_ID" \
  --status SUCCESS \
  --message "ok mode=${RUN_MODE} no_publish=${NO_PUBLISH}" \
  --source-success-file "$SOURCE_SUCCESS_FILE" \
  --source-fail-file "$SOURCE_FAIL_FILE" \
  --status-file "$SOURCE_STATUS_FILE" >/dev/null

END_TS=$(date +%s)
TOTAL_DUR=$((END_TS-START_TS))
SUMMARY=$(RUN_DT="$DT" STATUS_FILE="$SOURCE_STATUS_FILE" PYTHONPATH=src .venv/bin/python - <<'PY'
import json
import os
from pathlib import Path

import duckdb
from autotag.utils.paths import get_metabase_db_path, get_serving_db_path

dt = os.environ["RUN_DT"]
status = json.load(open(os.environ["STATUS_FILE"], 'r', encoding='utf-8'))
ops_rows = -1
max_dt = None
user_login_updates = 0
order_state_updates = 0
try:
    con = duckdb.connect(str(get_serving_db_path()), read_only=True)
    ops_rows = con.execute('select count(*) from ops."用户状态总览"').fetchone()[0]
    max_dt = con.execute('select max("统计日期") from ops."用户状态总览"').fetchone()[0]
    user_login_updates = con.execute("""
      select count(*)
      from stg.stg_user
      where dt = ?::date and last_login_time is not null
    """, [dt]).fetchone()[0]
    order_state_updates = con.execute("""
      select (
        select count(*) from (
          select order_id from stg.stg_recharge
          where dt = ?::date and order_id is not null and order_id <> ''
          group by 1 having count(distinct status_raw) > 1
        ) t
      ) + (
        select count(*) from (
          select withdraw_id from stg.stg_withdraw
          where dt = ?::date and withdraw_id is not null and withdraw_id <> ''
          group by 1 having count(distinct status_raw) > 1
        ) t
      )
    """, [dt, dt]).fetchone()[0]
    con.close()
except Exception:
    pass

mb = get_metabase_db_path()
mb_size = Path(mb).stat().st_size if Path(mb).exists() else 0
variant_ok = len(status.get("task_variant_success", []))
variant_fail = len(status.get("task_variant_fail", []))
print(
    f"dt={dt};ops_rows={ops_rows};max_ops_dt={max_dt};metabase_size_bytes={mb_size};"
    f"variant_ok={variant_ok};variant_fail={variant_fail};"
    f"user_login_updates={user_login_updates};order_state_updates={order_state_updates}"
)
PY
)

echo "[$(date '+%Y-%m-%d %H:%M:%S')] completed dt=${DT} run_id=${RUN_ID} total_duration_s=${TOTAL_DUR} ${SUMMARY}" | tee -a "$LOG_FILE"

if [[ "${ALERT_ON_SUCCESS:-1}" == "1" ]]; then
  send_alert "[AutoTag] SUCCESS dt=${DT}" "run_id=${RUN_ID}\nmode=${RUN_MODE}\nno_publish=${NO_PUBLISH}\nduration_s=${TOTAL_DUR}\n${SUMMARY}\nlog=${LOG_FILE}"
fi
