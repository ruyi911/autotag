# 快速命令参考

## 首次部署（一次性）

```bash
# 1. 环境准备
cd /Users/momo/Desktop/autotag
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# 2. 初始化加载历史数据
# 假设历史数据覆盖 2026-02-01 到 2026-02-28
bash scripts/init_backfill.sh 2026-02-01 2026-02-28

# 3. 运行完整流程（建模 + 发布）
bash scripts/run_daily.sh 2026-02-28

# 4. 验证成功
duckdb data/db/serving.duckdb
# 在 DuckDB 中查询：SELECT COUNT(*) FROM ops.user_profile;
```

---

## 日常运行

```bash
# 激活虚拟环境
source .venv/bin/activate

# 进入项目目录
cd /Users/momo/Desktop/autotag

# 运行前一天的数据（自动计算）
bash scripts/run_daily.sh

# 或指定日期
bash scripts/run_daily.sh 2026-03-04
```

---

## 补数运行（某个时间范围）

```bash
# 补数 2026-02-01 到 2026-02-07（共 7 天）
bash scripts/backfill.sh 2026-02-01 2026-02-07

# 补数单个月份
bash scripts/backfill.sh 2026-02-01 2026-02-28
```

---

## 调试与查询

```bash
# 查看运行日志
tail -50 logs/daily/dt=2026-03-04.log

# 查询数据库
duckdb data/db/serving.duckdb

# 在 DuckDB 中查询示例：
# SELECT COUNT(*) FROM raw.user;
# SELECT COUNT(*) FROM stg.user;
# SELECT COUNT(*) FROM mart.user;
# SELECT COUNT(*) FROM ops.user_profile;
```

---

## 关键特别提醒

| 场景 | 使用脚本 | 用途 |
|------|---------|------|
| **首次部署 + 历史数据** | `init_backfill.sh <start> <end>` | 仅 ingest/load，不建模 |
| **首次部署后的建模发布** | `run_daily.sh <date>` | 完整流程：load + model + publish |
| **日常运行** | `run_daily.sh` 或 `run_daily.sh <date>` | 新增数据的完整处理 |
| **补历史数据** | `backfill.sh <start> <end>` | 某个时间范围的完整处理 |

---

**记住**：`init_backfill.sh` 只做数据加载，最后一定要用 `run_daily.sh` 做完整流程！
