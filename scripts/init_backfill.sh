#!/usr/bin/env bash
set -euo pipefail

# 初始数据搬运脚本：从 initial_csv 移动文件到 raw_files
# 使用方式：bash scripts/init_backfill.sh <date:YYYY-MM-DD>
# 例如：bash scripts/init_backfill.sh 2026-02-28

cd "$(dirname "$0")/.."

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/init_backfill.sh <date:YYYY-MM-DD>"
  echo "Example: bash scripts/init_backfill.sh 2026-02-28"
  exit 1
fi

DT="$1"

# 验证日期格式
if ! [[ "$DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: Invalid date format. Expected YYYY-MM-DD, got: $DT"
  exit 1
fi

# 配置路径
INITIAL_CSV_PATH="./data/initial_csv"
RAW_FILES_DIR="./data/raw_files/dt=${DT}"

# 检查初始数据目录
if [[ ! -d "$INITIAL_CSV_PATH" ]] || [[ -z "$(ls -A "$INITIAL_CSV_PATH" 2>/dev/null)" ]]; then
  echo "ERROR: No data found in $INITIAL_CSV_PATH"
  exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Moving initial CSV files to raw_files/dt=$DT"
echo "Source: $INITIAL_CSV_PATH"
echo "Target: $RAW_FILES_DIR"
echo ""

# 扫描 initial_csv 目录，找出所有的数据源子目录
# 根据目录名推断源名（简化：目录名直接对应源名或按规则转换）
# 例如："用户数据" -> "user", "投注数据" -> "bet" 等

# 构建源映射（用纯 find 来避免中文编码问题）
INITIAL_CSV_PATH_EXPANDED=$(cd "$INITIAL_CSV_PATH" && pwd)

# 用 find 遍历子目录
for source_dir in $(find "$INITIAL_CSV_PATH_EXPANDED" -maxdepth 1 -mindepth 1 -type d -print0 | xargs -0 -I{} basename {}); do
  # 根据目录名判断源
  case "$source_dir" in
    *用户数据*|*user*)
      source_name="user"
      ;;
    *投注数据*|*bet*)
      source_name="bet"
      ;;
    *充值订单*|*recharge*)
      source_name="recharge"
      ;;
    *提现订单*|*withdraw*)
      source_name="withdraw"
      ;;
    *彩金数据*|*bonus*)
      source_name="bonus"
      ;;
    *)
      echo "[SKIP] Unknown source directory: $source_dir"
      continue
      ;;
  esac

  full_source_dir="$INITIAL_CSV_PATH_EXPANDED/$source_dir"
  target_dir="$RAW_FILES_DIR/$source_name"

  if [[ ! -d "$full_source_dir" ]]; then
    echo "[SKIP] Source directory not found: $full_source_dir"
    continue
  fi

  # 创建目标目录
  mkdir -p "$target_dir"

  # 复制所有 CSV 文件
  csv_count=0
  while IFS= read -r csv_file; do
    filename=$(basename "$csv_file")
    cp "$csv_file" "$target_dir/$filename"
    ((csv_count++))
    echo "[OK] Copied: $source_name/$filename"
  done < <(find "$full_source_dir" -maxdepth 1 -name "*.csv" -type f)

  if [[ $csv_count -gt 0 ]]; then
    echo "[INFO] Copied $csv_count CSV files from $source_dir to $source_name"
  else
    echo "[WARN] No CSV files found in $full_source_dir"
  fi

  echo ""
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Move Complete ==="
echo "Files are now in: $RAW_FILES_DIR"
echo ""
echo "Next steps:"
echo "  1. Run load pipeline:"
echo "     PYTHONPATH=src .venv/bin/python -m autotag.load.raw_import --dt $DT"
echo "     PYTHONPATH=src .venv/bin/python -m autotag.load.normalize --dt $DT"
echo "     PYTHONPATH=src .venv/bin/python -m autotag.load.build_mart --dt $DT"
echo "  2. Or run full pipeline:"
echo "     bash scripts/run_daily.sh $DT"
