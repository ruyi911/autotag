#!/usr/bin/env bash
# 用户全量回溯脚本 - 拉取指定日期范围内的所有新注册用户
export TZ=Asia/Kolkata
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

# 循环回溯用户数据时，忽略登录时间的新鲜度或订单状态漂移，因此禁用相关门控
export ENABLE_LOGIN_FRESHNESS_GATE=1
export ENABLE_STATUS_DRIFT_GATE=1

# 默认参数
USER_RANGE_START="2026-02-02"
USER_RANGE_END=""
FETCH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-date)
      USER_RANGE_START="$2"; shift 2 ;;
    --end-date)
      USER_RANGE_END="$2"; shift 2 ;;
    --fetch)
      FETCH=1; shift ;;
    *)
      echo "Unknown arg: $1"
      echo "Usage: $0 [--start-date DATE] [--end-date DATE] [--fetch]"
      exit 1 ;;
  esac
done

# 如果没有指定结束日期，默认为昨天
if [[ -z "$USER_RANGE_END" ]]; then
  USER_RANGE_END=$(PYTHONPATH=src .venv/bin/python - <<'PY'
from datetime import datetime, timedelta
from autotag.utils.time import INDIA_TZ
print((datetime.now(INDIA_TZ).date() - timedelta(days=1)).strftime("%Y-%m-%d"))
PY
)
fi

# 使用 end_date 作为 dt 参数（API 的时间点）
END_DATE="$USER_RANGE_END"

mkdir -p logs/daily .locks
LOG_FILE="logs/daily/user_full_backfill_${USER_RANGE_START}_to_${USER_RANGE_END}.log"
RUN_ID=$(PYTHONPATH=src .venv/bin/python - <<'PY'
import uuid
print(uuid.uuid4().hex)
PY
)
STATUS_FILE="logs/daily/user_backfill_status_${RUN_ID}.json"

echo "========================================" | tee -a "$LOG_FILE"
echo "User Full Backfill" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "Start Date: $USER_RANGE_START" | tee -a "$LOG_FILE"
echo "End Date: $USER_RANGE_END" | tee -a "$LOG_FILE"
echo "Remote Fetch: $([[ $FETCH -eq 1 ]] && echo 'enabled' || echo 'disabled')" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# 执行用户全量导出
FETCH_ARGS=""
REMOTE_FETCH_FLAG="0"
if [[ $FETCH -eq 1 ]]; then
  FETCH_ARGS="--fetch"
  REMOTE_FETCH_FLAG="1"
fi

echo "[user_backfill] downloading user data..." | tee -a "$LOG_FILE"
env PYTHONPATH=src ENABLE_REMOTE_FETCH="$REMOTE_FETCH_FLAG" .venv/bin/python -m autotag.ingest.downloader \
  --dt "$END_DATE" \
  --sources user \
  --mode daily \
  $FETCH_ARGS \
  --user-range-start "$USER_RANGE_START" \
  --user-range-end "$USER_RANGE_END" \
  --status-out "$STATUS_FILE" \
  2>&1 | tee -a "$LOG_FILE"

echo "[user_backfill] completed" | tee -a "$LOG_FILE"
echo "Status file: $STATUS_FILE" | tee -a "$LOG_FILE"

# 如果指定了 --fetch，还需要执行 ingest 和后续处理
if [[ $FETCH -eq 1 ]]; then
  echo "[user_backfill] running ingest and downstream tasks..." | tee -a "$LOG_FILE"
  
  PYTHONPATH=src .venv/bin/python -m autotag.load.raw_import --dt "$END_DATE" 2>&1 | tee -a "$LOG_FILE"
  PYTHONPATH=src .venv/bin/python -m autotag.load.normalize --dt "$END_DATE" 2>&1 | tee -a "$LOG_FILE"
  
  # 如果需要运行门控（上述环境变量已禁用新鲜度/漂移）   
  PYTHONPATH=src .venv/bin/python -m autotag.publish.validate --dt "$END_DATE" 2>&1 | tee -a "$LOG_FILE"
  
  echo "[user_backfill] all tasks completed" | tee -a "$LOG_FILE"
fi

echo "Log file: $LOG_FILE"
