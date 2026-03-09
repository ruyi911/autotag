# 数据窗口优化 - 回放模式仅当天数据

## 问题
在循环回放（`weekly_replay`）时，每天都有完整的数据，但当前仍然拉取前两天的数据（3天窗口），造成数据冗余。

## 解决方案
根据运行模式动态调整数据窗口：

| 模式 | 充值/提现 | 说明 |
|------|---------|------|
| **daily** (日常) | 3天窗口 | 保持原有逻辑，拉取前两天+今天 |
| **replay** (回放) | 当天仅 | 新增优化，循环回放时只拉当天数据 |
| **realtime** (实时) | 30分钟 | 保持原有逻辑 |

## 代码改动

### 1. 修改 `_task_variants_for_dt()` 函数签名

```python
# 新增参数 use_day_window
def _task_variants_for_dt(
    dt: str, 
    sources: list[str], 
    mode: str, 
    use_day_window: bool = False  # 新增
) -> list[TaskVariant]:
```

### 2. 充值数据逻辑

```python
if "recharge" in sources:
    if mode == "realtime":
        # 实时模式：30分钟窗口
        win_start, win_end = rt_start_s, rt_end_s
        variant_name = "recharge_realtime"
    elif use_day_window:
        # ✨ 新增：回放模式 - 仅当天数据
        win_start, win_end = _day_window(d)
        variant_name = "recharge_daily"
    else:
        # 日常模式 - 3天窗口
        win_start, win_end = _range_window(d - timedelta(days=2), d)
        variant_name = "recharge_window_3d"
```

### 3. 提现数据逻辑

```python
if "withdraw" in sources:
    if mode == "realtime":
        # 实时模式：30分钟窗口
        win_start, win_end = rt_start_s, rt_end_s
        variant_name = "withdraw_realtime"
    elif use_day_window:
        # ✨ 新增：回放模式 - 仅当天数据
        win_start, win_end = _day_window(d)
        variant_name = "withdraw_daily"
    else:
        # 日常模式 - 3天窗口
        win_start, win_end = _range_window(d - timedelta(days=2), d)
        variant_name = "withdraw_window_3d"
```

### 4. 修改 `_remote_fetch()` 函数

```python
def _remote_fetch(
    dt: str, 
    sources: list[str], 
    mode: str, 
    use_day_window: bool = False  # 新增参数
) -> tuple[dict[str, list[Path]], dict[str, Any]]:
    
    # 传递参数到 _task_variants_for_dt
    variants = _task_variants_for_dt(
        dt=dt, 
        sources=sources, 
        mode=mode, 
        use_day_window=use_day_window
    )
```

### 5. 修改 `_run_single_dt()` 函数

```python
variant_success_map: dict[str, list[dict[str, str]]] = {}
if _remote_enabled(args):
    # ✨ 回放模式时使用当天数据窗口
    use_day_window = args.mode == "replay"
    preferred_files, status_payload = _remote_fetch(
        dt=dt,
        sources=sources,
        mode=args.mode,
        use_day_window=use_day_window,
    )
```

## 使用示例

### 日常运行（保持现有行为）
```bash
bash scripts/run_daily.sh
# 模式: daily
# 充值/提现: 拉取 3 天数据 (前两天 + 今天)
```

### 回放运行（新优化）
```bash
bash scripts/weekly_replay.sh --days 35
# 模式: replay
# 充值/提现: 仅拉取当天数据 ✨
```

### 原始命令行调用
```bash
# 日常模式 - 3天窗口
PYTHONPATH=src python -m autotag.ingest.downloader --dt 2026-03-09 --mode daily --fetch

# 回放模式 - 当天窗口
PYTHONPATH=src python -m autotag.ingest.downloader --dt 2026-03-09 --mode replay --fetch
```

## 性能改进

### 回放模式数据量减少

**原逻辑**（3天窗口）：
- Day 1 (3/9): 拉取 3/7, 3/8, 3/9 = 3 天数据
- Day 2 (3/10): 拉取 3/8, 3/9, 3/10 = 3 天数据（重复 3/8, 3/9）
- Day 3 (3/11): 拉取 3/9, 3/10, 3/11 = 3 天数据（重复 3/9, 3/10）
- ...35 天共拉取: 105 天数据（重复度 66%）

**新逻辑**（当天窗口）：
- Day 1 (3/9): 拉取 3/9 = 1 天数据
- Day 2 (3/10): 拉取 3/10 = 1 天数据
- Day 3 (3/11): 拉取 3/11 = 1 天数据
- ...35 天共拉取: 35 天数据（无重复）

**改进**：减少 66% 的数据重复，API 请求次数不变，但单次请求数据量减少 3 倍 ✓

## 向后兼容性

- ✅ 日常模式（daily）行为完全不变
- ✅ 实时模式（realtime）行为完全不变
- ✅ 默认参数 `use_day_window=False`，不影响现有调用
- ✅ `weekly_replay.sh` 已使用 `--mode replay`，自动享受优化

## 验证

```bash
# 验证日常模式（3天窗口）
PYTHONPATH=src python -c "
from autotag.ingest.downloader import _task_variants_for_dt
variants = _task_variants_for_dt('2026-03-09', ['recharge'], 'daily', use_day_window=False)
for v in variants:
    print(f'{v.variant}: {v.window_start} ~ {v.window_end}')
"
# 输出: recharge_window_3d: 2026-03-07 00:00:00 ~ 2026-03-09 23:59:59 ✓

# 验证回放模式（当天窗口）
PYTHONPATH=src python -c "
from autotag.ingest.downloader import _task_variants_for_dt
variants = _task_variants_for_dt('2026-03-09', ['recharge'], 'replay', use_day_window=True)
for v in variants:
    print(f'{v.variant}: {v.window_start} ~ {v.window_end}')
"
# 输出: recharge_daily: 2026-03-09 00:00:00 ~ 2026-03-09 23:59:59 ✓
```

## 相关文件

- [src/autotag/ingest/downloader.py](../src/autotag/ingest/downloader.py) - 核心逻辑修改
- [scripts/weekly_replay.sh](../scripts/weekly_replay.sh) - 已使用 `--mode replay`

## 后续优化

- [ ] 添加环境变量控制 `REPLAY_USE_DAY_WINDOW`
- [ ] 添加更多模式，如 `batch` 模式
- [ ] 优化 API 服务端，支持按日期范围批量导出
