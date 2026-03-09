#!/usr/bin/env bash
# 验证数据窗口模式切换功能

cd "$(dirname "$0")/.."

echo "测试：验证不同模式下的数据窗口"
echo "=================================="

echo -e "\n【测试1】日常模式 - 应该拉取3天数据"
PYTHONPATH=src python3 << 'PYTHON'
from autotag.ingest.downloader import _task_variants_for_dt
variants = _task_variants_for_dt('2026-03-09', ['recharge', 'withdraw'], 'daily', use_day_window=False)
for v in variants:
    if v.variant in ['recharge_window_3d', 'withdraw_window_3d']:
        print(f"  {v.variant}:")
        print(f"    开始: {v.window_start}")
        print(f"    结束: {v.window_end}")
        # 验证时间跨度
        from autotag.utils.time import parse_date
        from datetime import datetime
        start = datetime.fromisoformat(v.window_start)
        end = datetime.fromisoformat(v.window_end)
        days = (end.date() - start.date()).days + 1
        print(f"    天数: {days} 天 {'✓' if days == 3 else '✗ 错误'}")
PYTHON

echo -e "\n【测试2】回放模式 - 应该拉取当天数据"
PYTHONPATH=src python3 << 'PYTHON'
from autotag.ingest.downloader import _task_variants_for_dt
variants = _task_variants_for_dt('2026-03-09', ['recharge', 'withdraw'], 'replay', use_day_window=True)
for v in variants:
    if v.variant in ['recharge_daily', 'withdraw_daily']:
        print(f"  {v.variant}:")
        print(f"    开始: {v.window_start}")
        print(f"    结束: {v.window_end}")
        # 验证时间跨度
        from autotag.utils.time import parse_date
        from datetime import datetime
        start = datetime.fromisoformat(v.window_start)
        end = datetime.fromisoformat(v.window_end)
        days = (end.date() - start.date()).days + 1
        print(f"    天数: {days} 天 {'✓' if days == 1 else '✗ 错误'}")
PYTHON

echo -e "\n【测试3】实时模式 - 应该拉取30分钟数据"
PYTHONPATH=src python3 << 'PYTHON'
from autotag.ingest.downloader import _task_variants_for_dt
variants = _task_variants_for_dt('2026-03-09', ['recharge', 'withdraw'], 'realtime', use_day_window=False)
for v in variants:
    if v.variant in ['recharge_realtime', 'withdraw_realtime']:
        print(f"  {v.variant}:")
        print(f"    开始: {v.window_start}")
        print(f"    结束: {v.window_end}")
PYTHON

echo -e "\n【测试4】变体名称验证"
PYTHONPATH=src python3 << 'PYTHON'
from autotag.ingest.downloader import _task_variants_for_dt

# 日常模式
daily_variants = _task_variants_for_dt('2026-03-09', ['recharge', 'withdraw', 'user', 'bet', 'bonus'], 'daily', use_day_window=False)
daily_names = [v.variant for v in daily_variants]
print(f"  日常模式变体: {daily_names}")

# 回放模式
replay_variants = _task_variants_for_dt('2026-03-09', ['recharge', 'withdraw', 'user', 'bet', 'bonus'], 'replay', use_day_window=True)
replay_names = [v.variant for v in replay_variants]
print(f"  回放模式变体: {replay_names}")

# 验证充值和提现的名称变化
print(f"\n  验证变体名称变化:")
print(f"    充值日常: recharge_window_3d {'✓' if 'recharge_window_3d' in daily_names else '✗'}")
print(f"    充值回放: recharge_daily {'✓' if 'recharge_daily' in replay_names else '✗'}")
print(f"    提现日常: withdraw_window_3d {'✓' if 'withdraw_window_3d' in daily_names else '✗'}")
print(f"    提现回放: withdraw_daily {'✓' if 'withdraw_daily' in replay_names else '✗'}")
PYTHON

echo -e "\n=================================="
echo "验证完成！"
