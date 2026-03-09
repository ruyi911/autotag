# AutoTag 完整数据流分析

## 执行流程概览

当执行 `bash scripts/run_daily.sh 2026-03-04` 时，整个流程分为 **10 个步骤**：

```
downloader → raw_import → normalize → build_mart → features → labeling → ops_views → validate → pytest → snapshot
  (摄入)     (导入raw)   (规范化stg) (去重mart)  (特征)    (标签)   (视图)    (门禁)   (测试)  (发布)
```

---

## 第一阶段：数据摄入（Ingest）

### 步骤 1：downloader（摄入下载器）

**调用位置**：`scripts/run_daily.sh` → 行 49
```bash
python -m autotag.ingest.downloader --dt 2026-03-04
```

**执行文件**：`src/autotag/ingest/downloader.py`

**做什么**：
1. **远程下载**（如果启用 `--fetch` 或 `ENABLE_REMOTE_FETCH=1`）：
   - 调用 API，使用 `--dt` 指定的日期（2026-03-04）构建时间范围
   - `start_time = "2026-03-04 00:00:00"`
   - `end_time = "2026-03-04 23:59:59"`
   - 向 `https://u7rfxayccuiskiz7pc3kr73.boom79.vip` 的各个端点提交导出请求
   - 等待后端生成 Excel/CSV 文件
   - 下载到 `data/dropbox/`

2. **本地文件发现**（无论是否启用远程下载）：
   - 从 `data/dropbox/` 和 `data/initial_csv/` 扫描 CSV 文件
   - 调用 `discover_files()` 根据文件名和表头匹配数据源

3. **选择最新文件**：
   - 每个数据源（user, bet, recharge, withdraw, bonus）只保留最新的一个文件
   - 使用 `_select_latest_per_source()`（按修改时间逆序）

4. **归档与 Manifest**：
   - 将选中的文件 **复制**（不是移动）到：
     ```
     data/raw_files/dt=2026-03-04/
     ├── user/
     │   └── 用户数据2.01-2.28_a1b2c3d4.csv
     ├── bet/
     │   └── 投注2.28_e5f6g7h8.csv
     ├── recharge/
     │   └── 充值2.01-2.28_i9j0k1l2.csv
     ├── withdraw/
     │   └── 提现订单数据2.01-2.28_m3n4o5p6.csv
     └── bonus/
         └── 彩金02-28_q7r8s9t0.csv
     ```
   - 计算每个文件的 SHA256 哈希，添加到文件名（用于去重）
   - 生成 manifest JSON：
     ```
     data/manifests/dt=2026-03-04/manifest.json
     ```
     内容包括：源名、文件名、哈希、行数、创建时间等

**输出**：
- 📁 `data/raw_files/dt=2026-03-04/<source>/*.csv` - 原始 CSV 文件（按源分文件夹）
- 📄 `data/manifests/dt=2026-03-04/manifest.json` - 摄入账本
- 📝 `logs/daily/dt=2026-03-04.log` - 执行日志

**关键特性**：
- ✅ **日期精确控制**：通过 `--dt` 参数精确请求该日期的数据（之前修复的）
- ✅ **重复下载保护**：文件名包含哈希，相同内容不重复下载
- ✅ **只追加不覆盖**：原始文件永久保存用于审计

---

## 第二阶段：数据加载（Load）

### 步骤 2a：初始化数据库架构

**在 raw_import.py 的 main() 中**：
```python
_create_schemas(conn)      # 创建 5 个 schema
_ensure_manifest_table(conn)  # 创建 raw.manifest_files
```

**创建的 Schemas**：
```
raw         ← 原始数据（直接从 CSV 导入）
stg         ← 规范化数据（列名统一、类型转换）
mart        ← 事实表（去重、去重）
ops_config  ← 运营配置表
ops         ← 运营最终视图
```

### 步骤 2b：raw_import（原始导入）

**调用位置**：`scripts/run_daily.sh` → 行 51
```bash
python -m autotag.load.raw_import --dt 2026-03-04
```

**执行文件**：`src/autotag/load/raw_import.py`（220 行）

**执行逻辑**：
1. **读取 manifest**：`data/manifests/dt=2026-03-04/manifest.json`

2. **创建 raw 层表**（如果不存在）：
   ```sql
   CREATE TABLE IF NOT EXISTS raw.raw_user (
     "ID" VARCHAR,
     "用户信息" VARCHAR,
     "手机号" VARCHAR,
     ...
     dt DATE,
     source_file VARCHAR,
     file_hash VARCHAR,
     ingested_at TIMESTAMP DEFAULT now()
   )
   ```

3. **检查重复**（防止同一文件重复导入）：
   ```sql
   SELECT COUNT(*) FROM raw.raw_user
   WHERE dt='2026-03-04' AND file_hash='xxx'
   ```
   如果已存在，**跳过该文件**

4. **导入 CSV**：
   - 清理 CSV：`_sanitize_to_temp_csv()`
     - 去掉 BOM
     - 补齐短行、截断长行
     - 统一为 utf-8 编码
   - 使用 DuckDB 的 `COPY` 或 `INSERT INTO...SELECT` 导入
   - 添加元数据列：`dt`, `source_file`, `file_hash`, `ingested_at`

5. **记录 manifest 表**：
   ```sql
   INSERT INTO raw.manifest_files
   VALUES ('2026-03-04', 'user', '用户数据2.01-2.28_a1b2c3d4.csv', 'a1b2c3d4...', 1234, '/path/to/file', now())
   ```

**数据库内容变化**：
```
数据库：data/db/serving.duckdb
├── raw.manifest_files
│   └── 新增 1 行 per source = 5 行（user, bet, recharge, withdraw, bonus）
├── raw.raw_user
│   └── 新增 ~1000 行（该日期的所有用户）
├── raw.raw_bet
│   └── 新增 ~10000 行（该日期的所有投注）
├── raw.raw_recharge
│   └── 新增 ~500 行（该日期的所有充值）
├── raw.raw_withdraw
│   └── 新增 ~200 行（该日期的所有提现）
└── raw.raw_bonus
    └── 新增 ~300 行（该日期的所有彩金）
```

**重要特性**：
- ✅ **增量添加**（Append-Only）：不删除历史数据，每次都新增
- ✅ **幂等性**：同一文件重复导入会被检测并跳过（通过文件哈希）
- ✅ **元数据追踪**：每行都记录来源文件、哈希、导入时间

### 步骤 3：normalize（规范化）

**调用位置**：`scripts/run_daily.sh` → 行 52
```bash
python -m autotag.load.normalize --dt 2026-03-04
```

**执行文件**：`src/autotag/load/normalize.py`（简单包装）

**实际执行的 SQL**：`src/autotag/db/sql/stg/build_stg.sql`

**做什么**：
将 raw 层的原始数据转换为规范的 stg 层数据。

**例子：raw_user → stg_user**
```sql
CREATE OR REPLACE TABLE stg.stg_user AS
SELECT
  TRIM(BOTH '''' FROM "ID") AS user_id,                      -- 去引号，保留为 VARCHAR
  "用户信息" AS user_name,
  "手机号" AS phone,
  ...
  TRY_CAST("VIP等级" AS INTEGER) AS vip_level,              -- 类型转换
  COALESCE(
    TRY_STRPTIME("注册时间", '%Y-%m-%d %H:%M:%S'),          -- 日期解析（多格式）
    TRY_STRPTIME("注册时间", '%Y/%m/%d %H:%M:%S')
  ) AS register_time,
  ...
  DATE(...) AS biz_date,                                      -- 业务日期
  dt,
  source_file,
  file_hash,
  ingested_at
FROM raw.raw_user
WHERE TRIM(BOTH '''' FROM "ID") <> ''                        -- 过滤空值
```

**关键处理**：
1. **列名映射**：中文 → 英文（`"用户ID"` → `user_id`）
2. **类型转换**：`VARCHAR` → `DOUBLE`, `INTEGER`, `TIMESTAMP`（ID 保留为 VARCHAR）
3. **去重**：去掉引号、空格、逗号等
4. **多格式处理**：`COALESCE(TRY_STRPTIME(...), TRY_STRPTIME(...))`
5. **业务日期提取**：从各种时间戳中提取 `biz_date`
6. **过滤空值**：只保留有效记录（使用非空字符串检查）

**SQL 特点**：
- `CREATE OR REPLACE TABLE` - **全量重建**（每次都删除旧表，重新创建）
- 使用 `TRY_CAST` / `TRY_STRPTIME`：转换失败返回 NULL 而不是报错

**数据库变化**：
```
数据库：data/db/serving.duckdb
stg.stg_user        ← 从 raw.raw_user 重建（~1000 行）
stg.stg_bet         ← 从 raw.raw_bet 重建（~10000 行）
stg.stg_recharge    ← 从 raw.raw_recharge 重建（~500 行）
stg.stg_withdraw    ← 从 raw.raw_withdraw 重建（~200 行）
stg.stg_bonus       ← 从 raw.raw_bonus 重建（~300 行）
```

### 步骤 4：build_mart（构建 Mart 层）

**调用位置**：`scripts/run_daily.sh` → 行 53
```bash
python -m autotag.load.build_mart --dt 2026-03-04
```

**实际执行的 SQL**：`src/autotag/db/sql/mart/build_mart.sql`

**做什么**：从 stg 构建去重的事实表（Fact Tables）。

**例子：fact_user**
```sql
CREATE OR REPLACE TABLE mart.fact_user AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id                              -- 按用户分组
      ORDER BY COALESCE(last_login_time, register_time) DESC,  -- 按最后登录时间倒序
               ingested_at DESC
    ) AS rn
  FROM stg.stg_user
) t
WHERE rn = 1;  -- 只保留第一条（最新的）
```

**核心逻辑**：
- **PARTITION BY 主键**：按各表的主键分组（user_id, order_id, bet_id 等）
- **ORDER BY 时间**：按时间和导入时间倒序排列
- **WHERE rn = 1**：去掉重复，只保留最新的一条

**各表的去重方式**：

| 表 | 主键 | 排序字段 |
|---|---|----|
| fact_user | user_id | last_login_time / register_time |
| fact_recharge | order_id | pay_time / created_time |
| fact_bet | bet_id | biz_date |
| fact_withdraw | withdraw_id | finish_time / submit_time |
| fact_bonus | user_id + claim_time | ingested_at |

**额外的聚合表**：
除了 fact_* 表，还会创建 **user_profile_daily** 等聚合表：
```sql
CREATE OR REPLACE TABLE mart.user_profile_daily AS
WITH keys AS (
  SELECT user_id, biz_date FROM mart.fact_recharge
  UNION
  SELECT user_id, biz_date FROM mart.fact_withdraw
  ...
),
recharge_daily AS (
  SELECT
    user_id,
    biz_date,
    SUM(CASE WHEN status_raw = '充值成功' 
        THEN COALESCE(pay_amt_raw, 0) ELSE 0 END) AS recharge_amt_success,
    COUNT(*) AS recharge_cnt_total,
    ...
  FROM mart.fact_recharge
  GROUP BY 1, 2
),
...
SELECT
  k.user_id,
  k.biz_date,
  r.recharge_amt_success,
  r.recharge_cnt_total,
  w.withdraw_amt_success,
  ...
FROM keys k
LEFT JOIN recharge_daily r ON k.user_id=r.user_id AND k.biz_date=r.biz_date
LEFT JOIN withdraw_daily w ON ...
...
```

**数据库变化**：
```
mart.fact_user          ← 去重后的用户数据（最新版本）
mart.fact_recharge      ← 去重后的充值数据
mart.fact_bet           ← 去重后的投注数据
mart.fact_withdraw      ← 去重后的提现数据
mart.fact_bonus         ← 去重后的彩金数据
mart.user_profile_daily ← 按 (user_id, biz_date) 聚合的日表
```

---

## 第三阶段：建模（Model）

### 步骤 5：features（特征工程）

**调用位置**：`scripts/run_daily.sh` → 行 54
```bash
python -m autotag.model.features --dt 2026-03-04
```

**执行文件**：`src/autotag/model/features.py`

**实际执行的 SQL**：`src/autotag/db/sql/mart/build_features.sql`（变量替换 `?` → 日期）

**做什么**：基于 mart 层的事实表，计算用户及订单的特征。

**可能计算的特征**（需要查看 SQL 文件）：
- 用户维度：
  - 累计充值金额、充值次数
  - 累计提现金额、提现次数
  - 累计投注金额、投注次数
  - 累计彩金金额
  - 注册以来天数、最后活跃天数
  - 平均充值金额、平均投注金额
  - 7天/30天 滑窗特征

- 订单维度：
  - 充值成功率
  - 提现到账率
  - 投注盈亏

**输出**：
- 创建 `mart.user_features`、`mart.order_features` 等特征表
- 通常是按 user_id 或 (user_id, biz_date) 的维度

### 步骤 6：labeling（标签生成）

**调用位置**：`scripts/run_daily.sh` → line 55
```bash
python -m autotag.model.labeling --dt 2026-03-04
```

**执行文件**：`src/autotag/model/labeling.py`

**实际执行的 SQL**：`src/autotag/db/sql/mart/build_labels.sql`

**做什么**：基于特征为用户打标签。

**可能的标签**：
- 高价值用户 / 低价值用户（基于充值金额）
- 活跃用户 / 沉睡用户（基于最后活跃时间）
- 高风险用户 / 低风险用户（基于异常行为）
- 首冲用户 / 老用户（基于注册时间）

**输出**：
- 创建 `mart.user_labels` 标签表
- 每个用户可能有多个标签

### 步骤 7：ops_views（构建 OPS 视图）

**调用位置**：`scripts/run_daily.sh` → line 56
```bash
python -m autotag.model.views_ops --dt 2026-03-04
```

**执行文件**：`src/autotag/model/views_ops.py`

**实际执行的 SQL**：`src/autotag/db/sql/ops/build_ops_views.sql`

**做什么**：为运营人员创建易查询的视图。

**可能的视图**：
```sql
CREATE OR REPLACE VIEW ops."用户状态总览" AS
SELECT
  user_id AS "用户ID",
  user_name AS "用户名",
  CASE WHEN ...  THEN '高价值' ELSE '普通' END AS "用户等级",
  recharge_amt_total AS "累计充值",
  withdraw_amt_total AS "累计提现",
  bet_amt_total AS "累计投注",
  CASE WHEN last_active_day < 7 THEN '活跃' ELSE '非活跃' END AS "状态",
  CURRENT_DATE AS "统计日期"
FROM mart.user_profile_daily
WHERE biz_date = ?  -- 该日期
ORDER BY recharge_amt_total DESC
LIMIT 10000;
```

**特点**：
- 视图名称都是 **中文**（便于运营）
- 包含 "统计日期" 字段（用于门禁检查）
- 通常包含汇总统计（SUM, COUNT 等）

**输出**：
- `ops."用户状态总览"` - 用户状态汇总
- 其他 ops 层视图/表（根据实现）

---

## 第四阶段：质量检查与发布（Publish）

### 步骤 8：validate（数据质量门禁）

**调用位置**：`scripts/run_daily.sh` → line 57
```bash
python -m autotag.publish.validate --dt 2026-03-04
```

**执行文件**：`src/autotag/publish/validate.py`

**门禁检查项**：
```python
def run_gating(dt: str) -> None:
    # 1️⃣ 关键对象存在且非空
    row_cnt = conn.execute('SELECT COUNT(*) FROM ops."用户状态总览"').fetchone()[0]
    if row_cnt <= 0:
        raise RuntimeError('门禁失败: 用户状态总览为空')

    # 2️⃣ 关键列不为空
    null_cnt = conn.execute('''
        SELECT COUNT(*) FROM ops."用户状态总览"
        WHERE "用户ID" IS NULL OR "状态" IS NULL
    ''').fetchone()[0]
    if null_cnt > 0:
        raise RuntimeError(f'门禁失败: 关键列存在空值')

    # 3️⃣ 日期不能是未来
    max_dt = conn.execute('SELECT MAX("统计日期") FROM ops."用户状态总览"').fetchone()[0]
    if max_dt > parse_date(dt):
        raise RuntimeError(f'门禁失败: 存在未来日期')
```

**失败后果**：
- ❌ 抛出异常，中止流程
- ❌ **不执行后续的 pytest 和 snapshot**
- ❌ serving.duckdb 不更新，metabase.duckdb 保持旧版本

### 步骤 9：pytest（发布门禁测试）

**调用位置**：`scripts/run_daily.sh` → line 59
```bash
pytest tests/test_publish_gating.py -q
```

**执行文件**：`tests/test_publish_gating.py`

**测试项**（根据代码框架，可自定义）：
```python
def test_critical_columns_completeness():
    """验证关键列完整性"""
    # 检查 user_id, amount, status 等关键字段无 NULL
    assert True

def test_date_range_validity():
    """验证日期范围有效性"""
    # 检查最近数据不超过 2 小时延迟
    # 检查不存在未来日期数据
    assert True

def test_data_volume_sanity():
    """验证数据量合理性"""
    # 与历史对比，日均增长不超过 ±50%
    assert True

def test_ops_views_queryable():
    """验证 OPS 视图可查询"""
    # 检查所有关键 OPS 视图存在且非空
    assert True
```

**失败后果**：同 validate()

### 步骤 10：snapshot（原子发布）

**调用位置**：`scripts/run_daily.sh` → line 61
```bash
python -m autotag.publish.snapshot --dt 2026-03-04
```

**执行文件**：`src/autotag/publish/snapshot.py`

**核心逻辑**：
```python
def atomic_publish(dt: str) -> None:
    run_gating(dt)  # 再次检查门禁（确保）

    serving = get_serving_db_path()     # data/db/serving.duckdb
    target = get_metabase_db_path()     # data/db/metabase.duckdb
    tmp = Path(str(target) + ".tmp")    # data/db/metabase.duckdb.tmp

    tmp.unlink(missing_ok=True)  # 清理旧的临时文件

    # 1️⃣ 在临时库中创建 ops schema
    with duckdb_conn(tmp, read_only=False) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
        conn.execute(f"ATTACH '{serving}' AS serving (READ_ONLY)")

        # 2️⃣ 从 serving.ops 复制所有表/视图到 tmp 的 ops schema
        objects = conn.execute("""
            SELECT table_name AS object_name FROM duckdb_tables()
            WHERE database_name = 'serving' AND schema_name = 'ops'
            UNION
            SELECT view_name FROM duckdb_views()
            WHERE database_name = 'serving' AND schema_name = 'ops'
        """).fetchall()

        for (name,) in objects:
            conn.execute(f'DROP TABLE IF EXISTS ops."{name}"')
            conn.execute(f'CREATE TABLE ops."{name}" AS SELECT * FROM serving.ops."{name}"')

        # 3️⃣ 验证临时库有数据
        published_rows = conn.execute(
            'SELECT COUNT(*) FROM ops."用户状态总览"'
        ).fetchone()[0]
        if published_rows <= 0:
            raise RuntimeError('发布失败: 临时 metabase 库为空')

        conn.execute("DETACH serving")

    # 4️⃣ 原子替换（Python 的 os.replace）
    os.replace(tmp, target)  # 如果失败，tmp 存在，target 不变

    print("发布成功!")
```

**关键特点**：
- ✅ **原子性**：使用 `os.replace()` 进行原子替换
  - 新库创建成功后才替换
  - 如果中途失败，老库保持不变
- ✅ **快照管理**：每次都创建新的 metabase.duckdb
  - 可以通过备份目录实现版本管理
- ✅ **只读 ATTACH**：serving 库以只读模式挂载，避免意外修改

**输出**：
- 📁 `data/db/metabase.duckdb` - 新的发布库（覆盖旧版本）
- 📁 `data/db/metabase.duckdb.wal` - 预写日志

---

## 数据库架构总览

```
serving.duckdb（生产库，可读写）
├── raw（原始数据）
│   ├── manifest_files       ← ingest.downloader 写入（记录文件元数据）
│   ├── raw_user             ← load.raw_import 插入（全追加）
│   ├── raw_bet
│   ├── raw_recharge
│   ├── raw_withdraw
│   └── raw_bonus
├── stg（规范化）
│   ├── stg_user             ← load.normalize 重建（CREATE OR REPLACE）
│   ├── stg_bet
│   ├── stg_recharge
│   ├── stg_withdraw
│   └── stg_bonus
├── mart（事实表与聚合）
│   ├── fact_user            ← load.build_mart 重建（去重）
│   ├── fact_bet
│   ├── fact_recharge
│   ├── fact_withdraw
│   ├── fact_bonus
│   ├── user_profile_daily   ← 聚合表
│   └── user_features        ← model.features 重建
├── ops_config（运营配置）
│   └── （预留）
└── ops（运营视图）
    ├── 用户状态总览         ← model.views_ops 重建（供发布）
    └── ...

metabase.duckdb（只读库，用于 BI）
└── ops（仅包含 ops 层）
    └── 用户状态总览         ← publish.snapshot 复制过来
```

---

## 数据流向图

```
① Dropbox / API                    ← 外部数据源
   ↓
② data/raw_files/dt=XXX/          ← 原始 CSV 归档（downloader）
   ↓
③ serving.duckdb
   ├─ raw.raw_* (INSERT)          ← 每行数据逐条插入（raw_import）
   │   ↓
   ├─ stg.stg_* (CREATE OR REPLACE)  ← 全量重建，同时做类型转换（normalize）
   │   ↓
   ├─ mart.fact_*                 ← 全量重建，去重（build_mart）
   │   ↓
   ├─ mart.user_features          ← 特征表（features）
   │   ↓
   ├─ mart.user_labels            ← 标签表（labeling）
   │   ↓
   └─ ops.* (CREATE OR REPLACE)   ← OPS 视图，供查询（views_ops）
       ↓ (通过 validate 和 pytest 门禁检查)
       ↓
④ metabase.duckdb                 ← 快照复制，原子替换（snapshot）
   └─ ops.*                        ← BI 库只读查询
```

---

## 关键数据流特性总结

### 1️⃣ **增量 vs 全量**

| 层 | 方式 | 原因 |
|---|---|----|
| raw | 增量（INSERT） | 保留完整历史，支持审计 |
| stg | 全量重建（CREATE OR REPLACE） | 规范化规则一致，避免重复处理 |
| mart | 全量重建（CREATE OR REPLACE） | 去重逻辑需要看全量数据 |
| ops | 全量重建（CREATE OR REPLACE） | 聚合统计需要完整数据 |
| metabase | 快照替换（os.replace） | 保证原子性，避免中间状态 |

### 2️⃣ **去重机制**

```
raw 层：
- 通过 manifest 中的文件哈希检查是否已导入
- 同一文件重复导入会被跳过

mart 层：
- 使用 ROW_NUMBER() OVER (PARTITION BY 主键 ORDER BY 时间 DESC)
- 每个主键只保留最新的一条记录
```

### 3️⃣ **日期维度**

```
downloader:
  - 指定 --dt，请求该日期的数据
  - data/raw_files/dt=2026-03-04/

其他模块:
  - --dt 参数用于 SQL 中的日期过滤（在 SQL 文件中使用 ? 占位符）
  - 影响 OPS 视图的 "统计日期" 字段
```

### 4️⃣ **事务与一致性**

```
DuckDB 特性：
- 单进程访问：同一时间只有一个 Python 进程访问 serving.duckdb
- 自动 ACID：每个 SQL 语句都是原子的
- metabase 发布时：
  - 先在临时文件中创建完整新库
  - 验证通过后用 os.replace() 原子替换
  - 避免中间状态暴露给查询端
```

---

## 优缺点分析

### ✅ 优点

1. **架构清晰**
   - 5 层分离（raw/stg/mart/ops_config/ops）
   - 每层职责明确：原始、规范、事实、配置、视图
   - 便于理解和维护

2. **数据可追溯**
   - raw 层保存完整历史，永不覆盖
   - 每行数据带有源文件、哈希、导入时间
   - 支持事后审计

3. **处理规则集中**
   - SQL 文件单独存放（`db/sql/*/`）
   - 便于版本管理和代码审查
   - 修改业务规则只需改 SQL

4. **去重保险**
   - raw 层通过文件哈希去重
   - mart 层通过主键去重
   - 双保险机制

5. **原子发布**
   - metabase.duckdb 通过临时文件 + os.replace 实现原子替换
   - 失败不影响旧库
   - 避免数据不一致

### ⚠️ 缺点

1. **存储浪费**
   - raw 层完整保存所有历史数据，占用大量磁盘
   - 对于长期运行（数年）的系统，raw 层会很庞大
   - 无内置的数据归档机制

2. **查询性能**
   - 每天都 CREATE OR REPLACE stg 和 mart，重建整个表
   - 对于大数据量（百万级），重建可能很慢
   - 没有分区或增量更新机制

3. **内存占用**
   - DuckDB 在内存中执行复杂 SQL（JOIN、GROUP BY）
   - 对于大表，可能触发内存溢出
   - 没有 spill-to-disk 的明确配置

4. **日期参数没有统一处理**
   - 每个 SQL 文件中需要手动使用 `?` 占位符
   - Python 代码中需要字符串替换（`sql.replace("?", ...)`）
   - 容易出错（SQL 注入风险，虽然简单数据不大）

5. **缺少增量更新支持**
   - 每次都全量重建 stg/mart/ops
   - 无法充分利用之前计算的结果
   - 不支持"只更新今天的数据"这种常见需求

6. **运维复杂度**
   - 需要定期清理 raw 层历史数据
   - 需要定期备份 metabase.duckdb（目前只有临时替换，无版本历史）
   - 没有自动故障恢复机制

### 🎯 改进建议

| 问题 | 改进方案 |
|---|---|
| raw 层存储爆炸 | 按月或按年分区，自动归档到冷存储 |
| stg/mart 重建慢 | 改成增量更新：DELETE WHERE biz_date = ? 然后 INSERT |
| 日期参数管理 | 使用 SQL 参数化查询（DuckDB 的 `execute(sql, params)`） |
| 内存溢出 | 分片处理（按日期、按用户 ID 分批） |
| 无版本历史 | 为 metabase.duckdb 维护快照库（按日期备份） |
| 可观测性 | 添加 metrics 表（记录每步的行数、耗时） |

---

## 总结

**AutoTag 的数据流是一个典型的 ETL 管道**：

```
Extract (downloader)
  ↓
Transform (raw_import → normalize → build_mart → features → labeling)
  ↓
Load (ops_views → validate → snapshot → metabase)
```

**核心特点**：
1. ✅ 架构分层清晰，便于维护
2. ✅ 数据完全可追踪，支持审计
3. ⚠️ 存储和性能需要优化（增量更新、分区、归档）
4. ⚠️ 运维成本相对较高（手动管理参数、无自动恢复）

**最适合的使用场景**：
- 数据量中等（百万到千万级）
- 需要完整审计追踪
- 容忍每天 5-30 分钟的处理延迟
- 查询频率不超高（非实时 OLAP）

**不适合的场景**：
- 数据量超大（亿级）
- 需要实时或分钟级更新
- 存储空间有限
- 需要非常高的查询性能
