# AutoTag 项目使用文档（基于代码核对）

本文档基于仓库当前代码整理，重点覆盖可直接执行的命令、运行链路和已知限制。

## 1. 项目是什么

AutoTag 是一个本地单机 DuckDB 数据流水线，主流程如下：

1. 采集：从远端 API 或本地 CSV 获取数据（`user/recharge/withdraw/bet/bonus`）。
2. 归档：按业务日写入 `data/raw_files/dt=YYYY-MM-DD/<source>/`，并生成 `manifest.json`。
3. 入库与建模：写入 `serving.duckdb` 的 `raw/stg/mart/ops_config/ops`。
4. 发布：将 `serving.duckdb` 的 `ops` schema 原子发布到 `metabase.duckdb`。

核心编排脚本是 `scripts/run_daily.sh`。

## 2. 环境准备

```bash
cd /Users/momo/Desktop/autotag
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

建议先确认 `.env` 里的以下配置：

- 路径：`DB_PATH`、`METABASE_DB_PATH`、`DROPBOX_PATH`、`RAW_FILES_PATH`、`MANIFESTS_PATH`
- 远端抓取：`ENABLE_REMOTE_FETCH`、`BASE_URL`、`API_USERNAME`、`API_PASSWORD`、`TOTP_SECRET`
- 数据源策略：`CORE_SOURCES`、`OPTIONAL_SOURCES`
- 告警：`ALERT_TELEGRAM_ENABLED`、`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`

## 3. 目录输入输出

输入目录：

- `data/dropbox/`：日常 CSV 输入（本地发现模式）
- `data/initial_csv/`：历史初始化 CSV

输出目录：

- `data/raw_files/dt=YYYY-MM-DD/<source>/*.csv`
- `data/manifests/dt=YYYY-MM-DD/manifest.json`
- `data/db/serving.duckdb`
- `data/db/metabase.duckdb`
- `logs/daily/*.log`

## 4. 日常运行（推荐）

默认跑印度时区前一日：

```bash
bash scripts/run_daily.sh
```

指定业务日：

```bash
bash scripts/run_daily.sh 2026-03-09
```

常用参数：

```bash
# 仅用已归档 raw_files 重跑后续步骤
bash scripts/run_daily.sh 2026-03-09 --skip-download

# 仅跑指定源
bash scripts/run_daily.sh 2026-03-09 --sources user,recharge,withdraw

# 运行模式（daily/replay/realtime）
bash scripts/run_daily.sh 2026-03-09 --mode replay

# 不发布 metabase（只跑到建模/校验前）
bash scripts/run_daily.sh 2026-03-09 --no-publish
```

## 5. 其他运行场景

### 5.1 区间补数（完整流程）

```bash
bash scripts/backfill.sh --start-date 2026-03-01 --end-date 2026-03-07
```

可选参数：

```bash
bash scripts/backfill.sh --start-date 2026-03-01 --end-date 2026-03-07 --only-sources user,recharge
bash scripts/backfill.sh --start-date 2026-03-01 --end-date 2026-03-07 --skip-download
bash scripts/backfill.sh --end-date 2026-03-07 --from-last-success
```

### 5.2 初始化历史 CSV（仅搬运到某一天）

```bash
bash scripts/init_backfill.sh 2026-02-28
```

说明：

- 此脚本当前只接受一个日期参数（不是日期区间）。
- 它会把 `data/initial_csv` 的文件复制到 `data/raw_files/dt=<date>/...`。
- 完成后仍需执行 `run_daily.sh <date>` 做后续入库和建模。

### 5.3 周期回放（滚动窗口）

```bash
bash scripts/weekly_replay.sh
```

可选参数：

```bash
bash scripts/weekly_replay.sh --days 30 --sources user,recharge,withdraw
bash scripts/weekly_replay.sh --start-date 2026-02-01 --end-date 2026-03-01
```

### 5.4 半小时实时模式

```bash
bash scripts/run_realtime.sh
```

可选参数：

```bash
bash scripts/run_realtime.sh --dt 2026-03-10 --sources user,recharge,withdraw
bash scripts/run_realtime.sh --force-publish
```

### 5.5 用户全量回溯

```bash
bash scripts/user_full_backfill.sh --start-date 2026-02-02 --end-date 2026-03-09 --fetch
```

## 6. 单模块直跑（调试用）

```bash
PYTHONPATH=src .venv/bin/python -m autotag.ingest.downloader --dt 2026-03-09 --fetch --mode daily
PYTHONPATH=src .venv/bin/python -m autotag.load.raw_import --dt 2026-03-09
PYTHONPATH=src .venv/bin/python -m autotag.load.normalize --dt 2026-03-09
PYTHONPATH=src .venv/bin/python -m autotag.load.build_mart --dt 2026-03-09
PYTHONPATH=src .venv/bin/python -m autotag.model.features --dt 2026-03-09
PYTHONPATH=src .venv/bin/python -m autotag.model.labeling --dt 2026-03-09
PYTHONPATH=src .venv/bin/python -m autotag.model.views_ops --dt 2026-03-09
PYTHONPATH=src .venv/bin/python -m autotag.publish.snapshot --dt 2026-03-09
```

## 7. 运行验证

查看日志：

```bash
tail -n 100 logs/daily/dt=2026-03-09.log
```

检查关键视图是否有数据：

```bash
duckdb data/db/serving.duckdb "select count(*) from ops.\"用户状态总览\";"
duckdb data/db/serving.duckdb "select max(\"统计日期\") from ops.\"用户状态总览\";"
```

查看运行历史：

```bash
duckdb data/db/serving.duckdb "select dt,mode,status,failed_step,started_at,ended_at from ops_config.run_history order by started_at desc limit 20;"
```

## 8. 已知限制（已实测）

1. `src/autotag/publish/validate.py` 当前存在缩进错误（第 84 行附近），会触发 `IndentationError`。  
   影响：默认发布链路（`publish.validate` / `publish.snapshot`）不可用。
2. 部分文档与脚本实际参数不一致：  
   例如 `init_backfill.sh` 代码只支持单日期；`backfill.sh` 代码要求 `--start-date/--end-date` 形式。
3. `pyproject.toml` 声明了 `autotag = "autotag.cli:main"`，但当前源码中未发现 `src/autotag/cli.py`。

## 9. 我已验证和未验证的边界

我已验证：

- 脚本参数、模块入口、目录路径、关键 SQL 产物（通过源码与命令行核对）。
- Python 语法检查可复现 `validate.py` 的缩进错误。

我做不到（当前信息不足/环境限制）：

- 无法在没有真实 API 凭证和可用网络的情况下验证远端抓取链路成功率。
- 无法确认你生产环境中的 cron、权限和网络策略是否与本地一致。