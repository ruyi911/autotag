# autotag

本项目在本地单机环境实现日批数据流水线：从运营 CSV 导入 DuckDB 写库 `serving.duckdb`（`raw/stg/mart/ops_config/ops`），并原子发布只读库 `metabase.duckdb`（仅 `ops`）。

## 1. 环境准备

```bash
cd /Users/momo/Desktop/autotag
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. 目录输入输出

输入：
- `data/dropbox/`（每日导出 CSV）
- `data/initial_csv/`（历史包，首次导入/补数）

输出：
- `data/raw_files/dt=YYYY-MM-DD/<source>/*.csv`
- `data/manifests/dt=YYYY-MM-DD/manifest.json`
- `data/db/serving.duckdb`
- `data/db/metabase.duckdb`
- `logs/daily/dt=YYYY-MM-DD.log`

## 3. 首次部署（加载历史数据）

如果有历史数据在 `data/initial_csv/`，首先需要初始化加载：

```bash
# 1. 确保历史数据已放入 data/initial_csv/
#    目录结构如：data/initial_csv/投注数据/、data/initial_csv/用户数据/ 等

# 2. 运行初始化脚本（加载历史数据到数据库）
bash scripts/init_backfill.sh 2026-02-01 2026-02-28

# 3. 后续再运行最新一天的完整流程（包含建模、发布）
bash scripts/run_daily.sh 2026-02-28
```

**关键点**：`init_backfill.sh` 仅加载数据到 `raw/stg/mart` 层（只做 ingest 和 load），不运行建模和发布。需要最后一次用 `run_daily.sh` 完成建模和发布。

## 4. 日常运行

默认跑前一日：

```bash
scripts/run_daily.sh
```

指定业务日：

```bash
scripts/run_daily.sh 2026-03-03
```

可选参数：

```bash
# 跳过抓取，仅用已有 raw_files 重建后续层
scripts/run_daily.sh 2026-03-03 --skip-download

# 仅跑指定源抓取（其它源按策略处理）
scripts/run_daily.sh 2026-03-03 --sources user,bet,recharge,withdraw,bonus

# 运行模式：daily / replay / realtime
scripts/run_daily.sh 2026-03-03 --mode replay

# 不发布（用于回放/实时）
scripts/run_daily.sh 2026-03-03 --no-publish
```

默认行为：
- `run_daily.sh` 会先执行 `ingest.downloader --fetch`（远端抓取 + 归档 + manifest）
- 脚本内置单实例锁（优先 `flock`，否则 PID 锁），避免并发写 DuckDB
- 失败自动写入 `ops_config.run_history` 并触发 Telegram 告警（开启后）
- 默认任何步骤失败即终止；可通过 `NON_FATAL_STEPS` 显式放宽指定步骤
- 会自动执行 `autotag.ingest.mobile_sync sync-missing`：从 `user_reg_*` 和 `user_login_*` 中筛选用户ID（`reg` 查空/`-`/`*`，`login` 默认只查空/`-`），调用 `queryUsersWithFile` 分批（每批最多 `9999`）补抓并写入手机号敏感表
- 下载策略内置“晚到更新”：
  - `user_reg_daily`：按 `regTime=D` 抓新增用户
  - `user_login_daily`：按 `loginTime=D` 回补登录时间
  - `recharge/withdraw`：按 `D-2~D` 抓近 3 天状态回写
  - 周日默认关闭 `*_full_weekly` 大窗口校正（可设 `ENABLE_WEEKLY_FULL_VARIANTS=1` 打开）

## 5. 补数（某个时间范围）

```bash
scripts/backfill.sh --start-date 2026-02-01 --end-date 2026-02-07
```

这个脚本会循环调用 `run_daily.sh`，包括 ingest、load、model、publish 等完整流程。

增强参数：

```bash
# 仅对指定源做回放
scripts/backfill.sh --start-date 2026-02-01 --end-date 2026-02-07 --only-sources user,bet,recharge,withdraw

# 跳过下载，使用已归档数据重跑
scripts/backfill.sh --start-date 2026-02-01 --end-date 2026-02-07 --skip-download

# 从上次成功日期+1开始补（需 run_history 表已存在）
scripts/backfill.sh --end-date 2026-02-07 --from-last-success
```

## 6. 定时任务（cron, India Time）

编辑 crontab：

```bash
crontab -e
```

示例：每天印度时间 01:00 触发（机器本地时区不影响）：

```cron
TZ=Asia/Kolkata
0 1 * * * cd /Users/momo/Desktop/autotag && /bin/bash scripts/run_daily.sh >> logs/daily/cron.log 2>&1
```

日志清理（保留 90 天）可单独配置：

```cron
30 1 * * * cd /Users/momo/Desktop/autotag && /bin/bash scripts/cleanup_logs.sh 90 >> logs/daily/cron.log 2>&1
```

周日滚动 30 天回放（默认 mutable 源）：

```cron
TZ=Asia/Kolkata
10 2 * * 0 cd /Users/momo/Desktop/autotag && /bin/bash scripts/weekly_replay.sh >> logs/daily/cron.log 2>&1
```

半小时实时更新（全源，默认双小时发布）：

```cron
TZ=Asia/Kolkata
*/30 * * * * cd /Users/momo/Desktop/autotag && /bin/bash scripts/run_realtime.sh >> logs/daily/cron.log 2>&1
```

手机号目录导入（两列：`用户ID`,`手机号`，支持 `csv/txt/tsv/xlsx`）：

```bash
bash scripts/sync_mobile_dir.sh /path/to/mobile_files
bash scripts/sync_mobile_dir.sh /path/to/mobile_files --no-recursive
```

## 7. 门禁与发布

- 门禁：`src/autotag/publish/validate.py` + `tests/test_publish_gating.py`
- 发布：`src/autotag/publish/snapshot.py`
- 发布策略文档：`src/autotag/publish/atomic_publish.md`

发布过程先构建 `metabase.duckdb.tmp`，校验通过后 `os.replace` 原子替换，失败不覆盖旧库。  
每次成功发布后会备份到 `data/db/snapshots/metabase_YYYY-MM-DD_HHMMSS.duckdb`，并按 `METABASE_SNAPSHOT_KEEP` 清理旧快照。

## 8. Telegram 告警

在 `.env` 配置 Telegram Bot：

```bash
ALERT_TELEGRAM_ENABLED=1
TELEGRAM_BOT_TOKEN=123456789:bot-token
TELEGRAM_CHAT_ID=-1001234567890
```

默认发送失败告警和成功摘要（可用 `ALERT_ON_SUCCESS` 控制）。

补充参数（建议）：

```bash
# 晚到更新窗口
USER_FULL_LOOKBACK_DAYS=3650
ORDER_FULL_LOOKBACK_DAYS=30
ENABLE_WEEKLY_FULL_VARIANTS=0
ROLLING_DAYS=30
REPLAY_SOURCES=user,recharge,withdraw
REALTIME_WINDOW_MINUTES=30
REALTIME_SOURCES=user,recharge,withdraw,bet,bonus
REALTIME_PUBLISH_EVERY_2H=1
REALTIME_FALLBACK_TO_DAY=1
EXPORT_SPLIT_ENABLED=1
EXPORT_SPLIT_MINUTES=60
EXPORT_SPLIT_MAX_DEPTH=4

# 手机号敏感表同步
ENABLE_MOBILE_SYNC=1
MOBILE_QUERY_ENDPOINT=/userManage/queryUsersWithFile
MOBILE_QUERY_BATCH_SIZE=9999
MOBILE_SYNC_INCLUDE_MASKED=0

# 门禁
ENABLE_LOGIN_FRESHNESS_GATE=1
ENABLE_STATUS_DRIFT_GATE=1
STATUS_DRIFT_MIN_ORDERS=100

# 在用户全量回溯 (user_full_backfill) 或 weekly_replay 脚本里会
# 自动把上面两个门禁开关设为 0，以避开登录时间新鲜度和订单状态漂移检测。详情请
# 查看 scripts/user_full_backfill.sh 与 scripts/weekly_replay.sh

```


## 文档树结构
autotag/
├── README.md
├── .env.example
├── pyproject.toml                     # 或 requirements.txt
├── configs/
│   ├── sources/                       # 每个数据源：口径+schema+主键+业务日期字段+状态枚举(成功/失败)
│   │   ├── bet.yaml
│   │   ├── recharge.yaml
│   │   ├── withdraw.yaml
│   │   ├── bonus.yaml
│   │   └── user.yaml
│   ├── mappings/                      # ✅(高ROI) 标准化字典：渠道/状态等（从代码中抽离）
│   │   ├── channel_map.csv
│   │   └── status_map.yaml
│   ├── ops_thresholds.yaml            # 默认阈值/参数（启动时可写入 ops_config 表）
│   └── pipelines.yaml                 # daily 编排：哪些 source 参与、依赖顺序、开关
├── scripts/
│   ├── run_daily.sh                   # download→load→model→publish
│   └── backfill.sh                    # 补数：按日期区间重跑
├── src/
│   └── autotag/
│       ├── __init__.py
│       ├── ingest/                    # 拿CSV→归档raw_files→写manifest（ledger）
│       │   ├── downloader.py
│       │   ├── discover.py
│       │   └── manifest.py
│       ├── load/                      # raw_files→serving.duckdb(raw/stg/mart)
│       │   ├── raw_import.py
│       │   ├── normalize.py
│       │   └── build_mart.py           # ✅(高ROI) 原 dedupe.py：mart 不止去重，还会固化事实/汇总/特征底表
│       ├── model/                     # stg/mart→画像/指标/标签 + ops 视图
│       │   ├── features.py
│       │   ├── labeling.py
│       │   └── views_ops.py
│       ├── publish/                   # serving.duckdb → metabase.duckdb
│       │   ├── snapshot.py
│       │   ├── validate.py
│       │   └── atomic_publish.md       # ✅(高ROI,文档) 说明原子发布/保留快照策略（初期先文档化）
│       ├── db/
│       │   ├── duckdb_conn.py
│       │   └── sql/
│       │       ├── raw/               # ✅(高ROI) SQL 分层，便于审阅与维护
│       │       ├── stg/
│       │       ├── mart/
│       │       ├── ops/
│       │       └── publish/
│       └── utils/
│           ├── time.py
│           ├── logging.py
│           └── paths.py
├── data/
│   ├── dropbox/                       # (可选) 后台下载中转目录
│   ├── raw_files/                     # 原始CSV归档（按抓取日分区，不覆盖）
│   │   └── dt=YYYY-MM-DD/
│   │       ├── bet/
│   │       ├── recharge/
│   │       ├── withdraw/
│   │       ├── bonus/
│   │       └── user/
│   ├── manifests/                     # ✅(高ROI) ingestion ledger（同时建议落到 DuckDB 表 raw.manifest_files）
│   │   └── dt=YYYY-MM-DD/
│   │       └── manifest.json
│   ├── db/
│   │   ├── serving.duckdb
│   │   ├── serving.duckdb.wal
│   │   ├── metabase.duckdb
│   │   └── metabasetest.duckdb
│   └── initial_csv/                   # 初始历史包（保留，作为 backfill 来源）
│       └── ...（你现有 data/csv 结构原样迁移/或软链接）
├── logs/
│   └── daily/
│       └── dt=YYYY-MM-DD.log
└── tests/
    ├── test_schema.py
    ├── test_labeling.py
    └── test_publish_gating.py   
