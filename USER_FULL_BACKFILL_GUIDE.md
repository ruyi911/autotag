# 用户全量回溯 - 使用指南

## 功能说明

一次性拉取指定日期范围内的所有新注册用户数据，支持分批导出大数据量。

## 使用方式

### 1. 基本用法（推荐）

```bash
# 拉取 2-2 到昨天的所有新注册用户（带并发 3 个）
bash scripts/user_full_backfill.sh --fetch

# 结果：
# - 从 API 拉取用户数据
# - 自动分片处理（如果超过 100 万条）
# - 使用 3 个并发加快速度
# - 存储到 data/raw_files/{END_DATE}/user/
```

### 2. 自定义日期范围

```bash
# 拉取 2026-01-01 到 2026-03-08 的用户
bash scripts/user_full_backfill.sh \
  --start-date 2026-01-01 \
  --end-date 2026-03-08 \
  --fetch
```

### 3. 仅导出不拉取（用于测试）

```bash
# 生成用户导出任务，但不从 API 拉取（用于测试参数）
bash scripts/user_full_backfill.sh --start-date 2026-02-02 --end-date 2026-03-08
```

### 4. 命令行直接调用

```bash
# 最灵活的方式
PYTHONPATH=src python -m autotag.ingest.downloader \
  --dt 2026-03-08 \
  --sources user \
  --mode daily \
  --fetch \
  --user-range-start 2026-02-02 \
  --user-range-end 2026-03-08
```

## 参数说明

### `user_full_backfill.sh` 脚本参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--start-date` | `2026-02-02` | 用户注册开始日期（YYYY-MM-DD） |
| `--end-date` | 昨天 | 用户注册结束日期（YYYY-MM-DD） |
| `--fetch` | - | 启用从 API 拉取（不指定则只生成任务） |

### 环境变量

```bash
# 自定义并发数（默认 3）
API_MAX_CONCURRENT=5 bash scripts/user_full_backfill.sh --fetch

# 配置到 .env
echo "API_MAX_CONCURRENT=5" >> .env
```

## 工作原理

### 单次拉取流程

```
user_full_backfill.sh --fetch
    ↓
python downloader.py --dt 2026-03-08 \
  --sources user \
  --user-range-start 2026-02-02 \
  --user-range-end 2026-03-08 \
  --fetch
    ↓
_remote_fetch(user_range_start='2026-02-02', user_range_end='2026-03-08')
    ↓
_task_variants_for_dt(..., user_range_start='2026-02-02', user_range_end='2026-03-08')
    ↓
生成任务：user_reg_backfill (2026-02-02 00:00:00 ~ 2026-03-08 23:59:59)
    ↓
执行 API 请求
    ↓
返回用户数据
    ↓
如果数据 > 100 万条，自动分片重试
    ↓
保存到 data/raw_files/2026-03-08/user/
```

## 数据量处理

### 单次请求限制

API 限制：最多 100 万条记录

**场景**：2026-02-02 到 2026-03-08（35 天）可能有 300 万条新用户

**自动处理**：

```
第 1 次请求：2026-02-02 ~ 2026-03-08 (失败：300 万 > 100 万)
    ↓ 自动分片（depth=1）
第 2 次请求：2026-02-02 ~ 2026-02-18 (成功：150 万 > 100 万)
    ↓ 继续分片（depth=2）
第 3 次请求：2026-02-02 ~ 2026-02-10 (成功：75 万)
第 4 次请求：2026-02-11 ~ 2026-02-18 (成功：75 万)
第 5 次请求：2026-02-19 ~ 2026-03-08 (成功：150 万 > 100 万)
    ↓ 继续分片（depth=3）
...最终完成
```

### 并发优化

```bash
# 使用 3 个并发加快处理
user_reg_backfill 任务 1 (2-2 ~ 2-18)  → 线程 1
user_reg_backfill 任务 2 (2-19 ~ 3-8)  → 线程 2
[等待分片结果]                          → 线程 3
...
```

## 输出文件

### 日志文件
```
logs/daily/user_full_backfill_2026-02-02_to_2026-03-08.log
```

### 数据文件
```
data/raw_files/2026-03-08/user/
├── 用户数据文件_xxxxx.csv
└── manifest_2026-03-08.json
```

### 状态文件
```
logs/daily/user_backfill_status_{RUN_ID}.json

格式：
{
  "dt": "2026-03-08",
  "task_variant_success": [
    {
      "variant": "user_reg_backfill",
      "source": "user",
      "window_start": "2026-02-02 00:00:00",
      "window_end": "2026-03-08 23:59:59",
      "filename": "用户数据文件_xxxxx.csv"
    }
  ],
  "task_variant_fail": [],
  "source_success": ["user"],
  "source_fail": {}
}
```

## 完整工作流示例

### 场景：首次完整导入用户数据

```bash
# 步骤 1：拉取全量用户（注册时间 2-2 ~ 昨天）
bash scripts/user_full_backfill.sh --fetch

# 步骤 2：（可选）导入到数据库
PYTHONPATH=src python -m autotag.load.raw_import --dt $(date -d yesterday +%Y-%m-%d)

# 步骤 3：（可选）标准化
PYTHONPATH=src python -m autotag.load.normalize --dt $(date -d yesterday +%Y-%m-%d)
```

### 场景：定期获取新用户增量

```bash
# 每天定时任务（cron）
0 2 * * * bash /path/to/user_full_backfill.sh --fetch

# 或手动运行最新一天
bash scripts/user_full_backfill.sh --end-date $(date +%Y-%m-%d) --fetch
```

### 场景：导出特定历史期间的用户

```bash
# 导出 2026-01-01 到 2026-02-01 的用户
bash scripts/user_full_backfill.sh \
  --start-date 2026-01-01 \
  --end-date 2026-02-01 \
  --fetch
```

## 常见问题

### Q1：如何检查导出是否成功？

```bash
# 查看日志
tail -f logs/daily/user_full_backfill_*.log

# 查看状态
cat logs/daily/user_backfill_status_*.json | python -m json.tool

# 检查数据文件
ls -lh data/raw_files/*/user/
```

### Q2：如果超过 100 万条数据会怎样？

自动分片处理：
- 会递归分片直到每片 < 100 万条
- 最多 4 层分片（深度由 `EXPORT_SPLIT_MAX_DEPTH=4` 控制）
- 所有数据最终合并

### Q3：如何处理中途失败的情况？

**自动重试**：
- 登录失败：自动重试 3 次（2s, 4s, 8s 延迟）
- API 请求失败：自动分片重试
- 可选源失败：记录后继续

**手动重新运行**：
```bash
# 重新拉取相同日期范围（会覆盖之前的数据）
bash scripts/user_full_backfill.sh \
  --start-date 2026-02-02 \
  --end-date 2026-03-08 \
  --fetch
```

### Q4：并发数如何设置最优？

```bash
# 测试不同并发数
for i in 1 3 5 10; do
  echo "Testing with $i concurrent..."
  time API_MAX_CONCURRENT=$i bash scripts/user_full_backfill.sh --fetch
done
```

建议：
- 网络好：3-5 并发
- 网络一般：3 并发（默认）
- API 限制：1-2 并发

### Q5：导出的数据包括什么？

```
user_reg_backfill 任务导出：
├─ 用户 ID
├─ 注册时间
├─ 所属渠道
├─ VIP 等级
├─ 注册设备信息
└─ ...其他用户属性
```

## 性能参考

| 日期范围 | 用户数 | 所需时间* |
|---------|--------|---------|
| 1 周 | ~50 万 | 3-5 分钟 |
| 1 个月 | ~200 万 | 8-15 分钟 |
| 2 个月 | ~400 万 | 15-30 分钟 |
| 3+ 个月 | >100 万条/次 | 分片处理 |

*基于 3 个并发、网络延迟 50-100ms

---

**快速开始**：`bash scripts/user_full_backfill.sh --fetch`
