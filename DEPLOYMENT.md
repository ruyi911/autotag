# 首次部署指南

## 整体流程

AutoTag 首次部署分为两个阶段：

1. **初始化加载**（`init_backfill.sh`）- 加载历史数据到数据库 raw/stg/mart 层
2. **完整流程运行**（`run_daily.sh`）- 包含建模、标签、OPS 视图、发布

---

## 第一步：环境准备

```bash
cd /Users/momo/Desktop/autotag

# 创建虚拟环境
python3 -m venv .venv

# 激活虚拟环境
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 根据需要编辑 .env（主要是 API 凭证等）
```

---

## 第二步：准备历史数据

确保历史 CSV 数据放在正确的位置：

```
data/initial_csv/
├── 用户数据/
│   └── 用户数据2.01-2.28.csv
├── 投注数据/
│   ├── 投注2.01.csv
│   ├── 投注2.02.csv
│   └── ...
├── 充值订单/
│   └── 充值2.01-2.28.csv
├── 提现订单/
│   └── 提现订单数据2.01-2.28.csv
└── 彩金数据/
    ├── 彩金02-01.csv
    ├── 彩金02-02.csv
    └── ...
```

---

## 第三步：初始化加载历史数据

运行初始化脚本，将历史 CSV 加载到 `serving.duckdb` 的 raw/stg/mart 层：

```bash
# 假设历史数据覆盖 2026-02-01 到 2026-02-28
bash scripts/init_backfill.sh 2026-02-01 2026-02-28
```

**这一步会做什么**：
- ✅ 逐日扫描 `data/initial_csv/` 下的文件（使用 `--include-initial` 标志）
- ✅ 将 CSV 复制到 `data/raw_files/dt=YYYY-MM-DD/<source>/` 并生成 manifest
- ✅ 导入到 `serving.duckdb` 的 `raw` 层
- ✅ 标准化字段到 `stg` 层
- ✅ 构建去重/聚合表到 `mart` 层
- ⚠️ **不会**计算特征、标签、OPS 视图（这些在第四步）

**日志输出**：
```
logs/daily/init_backfill.log
logs/daily/dt=2026-02-01.log
logs/daily/dt=2026-02-02.log
...
logs/daily/dt=2026-02-28.log
```

---

## 第四步：运行完整流程（建模 + 发布）

初始化完成后，运行最新一天的完整流程（包括建模、标签、OPS、门禁、发布）：

```bash
# 对最后一天（2026-02-28）运行完整的建模和发布流程
bash scripts/run_daily.sh 2026-02-28
```

**这一步会做什么**：
- ✅ Ingest（仅当日新增数据，不用 `--include-initial`）
- ✅ Load（raw/stg/mart）
- ✅ Model
  - 计算特征（features）
  - 生成标签（labeling）
  - 构建 OPS 视图（ops_views）
- ✅ Publish
  - 数据质量验证（validate）
  - 门禁测试（test_publish_gating.py）
  - 快照发布（snapshot）→ `metabase.duckdb`

**日志**：
```
logs/daily/dt=2026-02-28.log
```

---

## 验证部署成功

```bash
# 查询数据库，确认数据已加载
duckdb data/db/serving.duckdb

# 查看有哪些表
SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'temp') ORDER BY table_schema, table_name;

# 查看 raw 层原始表
SELECT COUNT(*) as cnt FROM raw.bet LIMIT 5;
SELECT COUNT(*) as cnt FROM raw.user LIMIT 5;
SELECT COUNT(*) as cnt FROM raw.recharge LIMIT 5;
SELECT COUNT(*) as cnt FROM raw.withdraw LIMIT 5;
SELECT COUNT(*) as cnt FROM raw.bonus LIMIT 5;

# 查看 mart 层（已去重/聚合）
SELECT COUNT(*) FROM mart.user;
SELECT COUNT(*) FROM mart.bet;

# 查看 ops 层（已包含特征和标签）
SELECT COUNT(*) FROM ops.user_profile LIMIT 5;

# 查看发布库（metabase 的快照）
SELECT * FROM ops.user_profile LIMIT 3;
```

按 Ctrl+D 或输入 `.exit` 退出 DuckDB。

---

## 后续日常运行

部署完成后，每日运行：

```bash
# 跑前一天的数据（自动计算昨天的日期）
bash scripts/run_daily.sh

# 或指定日期
bash scripts/run_daily.sh 2026-03-04
```

---

## 常见问题

### Q：历史数据太多，初始加载太慢怎么办？

**A**：可以按月分批加载：

```bash
# 第一批：2月
bash scripts/init_backfill.sh 2026-02-01 2026-02-28

# 第二批：1月（如需要）
bash scripts/init_backfill.sh 2026-01-01 2026-01-31

# 最后一次完整流程
bash scripts/run_daily.sh 2026-02-28
```

### Q：初始化中途失败，如何重新开始？

**A**：有两种选择：

**方案 1**：清空数据库重新开始
```bash
rm data/db/serving.duckdb data/db/serving.duckdb.wal
bash scripts/init_backfill.sh 2026-02-01 2026-02-28
```

**方案 2**：从中断处继续（需要检查 logs 确认上次跑到哪天）
```bash
# 从 2026-02-15 开始重新加载
bash scripts/init_backfill.sh 2026-02-15 2026-02-28
```

### Q：如何只加载某个数据源？

**A**：用 `--sources` 参数（高级用法）：

```bash
# 只加载用户和投注数据
PYTHONPATH=src .venv/bin/python -m autotag.ingest.downloader \
  --dt 2026-02-01 \
  --include-initial \
  --sources user,bet
```

### Q：发布失败（门禁不通过）怎么办？

**A**：检查日志：

```bash
tail -50 logs/daily/dt=2026-02-28.log | grep -i "error\|fail\|gating"
```

常见原因：
- 关键列有 NULL（检查 raw 层 schema）
- 数据量异常增长（检查 ops 视图数据）
- OPS 视图构建失败（检查 model 层代码）

---

## 部署清单

- [ ] 虚拟环境已创建并激活
- [ ] 依赖已安装（`pip install -r requirements.txt`）
- [ ] `.env` 已配置（API 凭证等）
- [ ] 历史数据已放入 `data/initial_csv/`
- [ ] 初始化加载已完成（`init_backfill.sh`）
- [ ] 完整流程已运行一次（`run_daily.sh`）
- [ ] 数据库验证通过（能查询到数据）
- [ ] 发布库已生成（`metabase.duckdb`）

---

**下一步**：按照上述步骤执行，有问题随时反馈！
