# 回测分析指南

## 问题
"用户状态总览"表默认统计到昨天的数据。如果想回测分析某个更早日期的标签准确性，需要手动修改统计日期。

## 解决方案

### 快速回测（仅重新计算标签）

指定日期，重新计算该日期的用户标签和状态：

```bash
bash scripts/backtest.sh --dt 2026-03-05
```

**流程**：
1. 重新构建特征表（feature engineering）
2. 重新计算标签（label generation）
3. 重新构建用户状态总览表
4. 回测完成，可在 Metabase 中查看

**查询验证**：
```sql
SELECT MAX("统计日期") FROM ops."用户状态总览";
```

### 回测 + 发布到 Metabase

如果想把回测结果发布到 Metabase 仪表板：

```bash
bash scripts/backtest.sh --dt 2026-03-05 --publish
```

## 技术细节

### 日期参数的传递链路

1. **CLI 参数** → `--dt 2026-03-05`
2. **features.py** → `_run_sql_file(conn, "mart/build_features.sql", args.dt)`
3. **build_features.sql** → `SELECT ?::DATE AS as_of_date` 替换为 `DATE '2026-03-05'`
4. **结果** → `user_last_activity` 表中 `as_of_date = 2026-03-05`
5. **级联** → `user_state_engine` 和 `ops."用户状态总览"` 都基于此日期计算

### 相关文件

| 文件 | 作用 |
|------|------|
| `src/autotag/model/features.py` | 特征构建（接收 `--dt` 参数） |
| `src/autotag/model/labeling.py` | 标签生成（接收 `--dt` 参数） |
| `src/autotag/model/views_ops.py` | 视图构建 |
| `src/autotag/db/sql/mart/build_features.sql` | 特征 SQL（使用 `?` 占位符） |
| `src/autotag/db/sql/mart/build_labels.sql` | 标签 SQL（使用 `?` 占位符） |
| `src/autotag/db/sql/ops/build_ops_views.sql` | 用户状态总览视图定义 |

## 回测场景示例

### 场景 1：验证标签在特定日期的准确性

```bash
# 重新计算 2026-02-28 的所有用户标签
bash scripts/backtest.sh --dt 2026-02-28

# 在 DuckDB 中查询该日期的数据
sqlite3 data/db/serving.duckdb <<'SQL'
SELECT 
  "用户ID",
  "用户状态",
  "状态子类",
  "最后投注日",
  "最后充值成功日"
FROM ops."用户状态总览"
WHERE "统计日期" = '2026-02-28'
LIMIT 10;
SQL
```

### 场景 2：对比两个不同日期的标签分布

```bash
# 先计算日期 A 的标签
bash scripts/backtest.sh --dt 2026-02-28

# 导出日期 A 的汇总数据
sqlite3 data/db/serving.duckdb <<'SQL'
SELECT "用户状态", COUNT(*) FROM ops."用户状态总览"
WHERE "统计日期" = '2026-02-28'
GROUP BY 1;
SQL

# 计算日期 B 的标签
bash scripts/backtest.sh --dt 2026-03-01

# 导出日期 B 的汇总数据
sqlite3 data/db/serving.duckdb <<'SQL'
SELECT "用户状态", COUNT(*) FROM ops."用户状态总览"
WHERE "统计日期" = '2026-03-01'
GROUP BY 1;
SQL
```

### 场景 3：查看用户状态转移

```bash
# 先计算两个日期的标签
bash scripts/backtest.sh --dt 2026-02-28
bash scripts/backtest.sh --dt 2026-03-01

# 查询同一用户在两个日期的状态变化
sqlite3 data/db/serving.duckdb <<'SQL'
SELECT
  a."用户ID",
  a."用户状态" AS 状态_0228,
  b."用户状态" AS 状态_0301,
  CASE WHEN a."用户状态" <> b."用户状态" THEN '变化' ELSE '不变' END AS 是否变化
FROM ops."用户状态总览" a
LEFT JOIN ops."用户状态总览" b ON a."用户ID" = b."用户ID" AND b."统计日期" = '2026-03-01'
WHERE a."统计日期" = '2026-02-28'
  AND a."用户状态" <> b."用户状态"
LIMIT 20;
SQL
```

## 注意事项

⚠️ **重要**：回测会覆盖现有的特征和标签数据。如果需要保留原来的生产数据，建议：

1. **备份生产库**：
   ```bash
   cp data/db/serving.duckdb data/db/serving.duckdb.bak.$(date +%Y%m%d_%H%M%S)
   ```

2. **或者使用临时库**：创建一个独立的 DuckDB 文件用于回测，而不是修改生产库。

3. **回到最新日期**：完成回测后，重新运行当天的处理流程恢复正常：
   ```bash
   bash scripts/run_daily.sh --skip-download
   ```

## 常见问题

### Q: 回测后能直接恢复到原来的日期吗？
A: 可以。重新运行当天的日常处理即可：
```bash
bash scripts/run_daily.sh --skip-download
```
这会用最新的实际数据覆盖回测数据。

### Q: 能同时回测多个日期吗？
A: 目前脚本一次只支持一个日期。如需批量回测，可以循环调用：
```bash
for dt in 2026-02-28 2026-02-27 2026-02-26; do
  bash scripts/backtest.sh --dt "$dt"
  # 导出结果...
done
```

### Q: 回测时使用的数据源是什么？
A: 使用 `stg.*`（stage）层的实际业务数据。这些数据来自原始 CSV 文件（data/raw_files）。只要原始数据完整，回测结果就是准确的。

### Q: 能只回测特定用户的标签吗？
A: 可以。回测后，用 SQL 筛选即可：
```sql
SELECT * FROM ops."用户状态总览"
WHERE "统计日期" = '2026-03-05'
  AND "用户ID" = '12345';
```
