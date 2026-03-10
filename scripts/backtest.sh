#!/usr/bin/env bash
# 回测脚本：通过指定日期重新构建标签和用户状态总览
# 用法: bash scripts/backtest.sh --dt 2026-03-05
# 或:  bash scripts/backtest.sh --dt 2026-03-05 --publish (发布到 Metabase)

export TZ=Asia/Kolkata
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

DT=""
PUBLISH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dt)
      DT="$2"; shift 2 ;;
    --publish)
      PUBLISH=1; shift ;;
    *)
      echo "Unknown arg: $1"
      echo "Usage: $0 --dt DATE [--publish]"
      echo "Example: $0 --dt 2026-03-05 --publish"
      exit 1 ;;
  esac
done

if [[ -z "$DT" ]]; then
  echo "Error: --dt is required"
  echo "Usage: $0 --dt DATE [--publish]"
  exit 1
fi

echo "=========================================="
echo "Backtest Analysis"
echo "=========================================="
echo "Date: $DT"
echo "Publish to Metabase: $([[ $PUBLISH -eq 1 ]] && echo 'yes' || echo 'no')"
echo "=========================================="

# 第 1 步：重新构建特征表（使用指定日期）
echo "Step 1/4: Building features for $DT..."
PYTHONPATH=src .venv/bin/python -m autotag.model.features --dt "$DT"

# 第 2 步：重新构建标签和用户状态引擎
echo "Step 2/4: Building labels for $DT..."
PYTHONPATH=src .venv/bin/python -m autotag.model.labeling --dt "$DT"

# 第 3 步：重新构建 ops 视图（用户状态总览表）
echo "Step 3/4: Rebuilding ops views..."
PYTHONPATH=src .venv/bin/python -m autotag.model.views_ops --dt "$DT"

echo "Step 4/4: Backtest completed."
echo "用户状态总览表 已更新至 $DT"

# 可选：发布到 Metabase
if [[ $PUBLISH -eq 1 ]]; then
  echo ""
  echo "Publishing to Metabase..."
  PYTHONPATH=src .venv/bin/python -m autotag.publish.snapshot --dt "$DT"
  echo "✓ Published to Metabase"
fi

echo ""
echo "Query example to verify:"
echo "  SELECT MAX(\"统计日期\") FROM ops.\"用户状态总览\";"
echo ""
