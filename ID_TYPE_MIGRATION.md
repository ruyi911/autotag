# ID 类型从 BIGINT 迁移到 VARCHAR - 修改总结

## 修改日期
2026-03-05

## 概述
将所有SQL文件中的ID字段类型从 BIGINT 改为 VARCHAR，以支持非数字和大数字的用户ID。

## 修改范围

### 1. SQL 文件修改

#### [build_stg.sql](src/autotag/db/sql/stg/build_stg.sql)
**修改内容**：5 个表的 ID 字段转换

| 表名 | 修改项 | 修改前 | 修改后 |
|-----|-------|--------|--------|
| stg_user | user_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |
| stg_user | parent_user_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |
| stg_recharge | user_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |
| stg_bet | bet_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |
| stg_bet | user_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |
| stg_withdraw | withdraw_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |
| stg_withdraw | user_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |
| stg_bonus | user_id | `TRY_CAST(TRIM(...) AS BIGINT)` | `TRIM(...)` |

**WHERE 条件修改**：所有表都更新了过滤条件

| 原条件 | 新条件 |
|--------|--------|
| `WHERE TRY_CAST(...) IS NOT NULL` | `WHERE TRIM(...) <> ''` |

#### [build_mart.sql](src/autotag/db/sql/mart/build_mart.sql)
**修改内容**：3 个事实表的 WHERE 条件

| 表名 | 修改项 | 修改前 | 修改后 |
|-----|-------|--------|--------|
| fact_bet | WHERE bet_id | `WHERE bet_id IS NOT NULL` | `WHERE bet_id <> ''` |
| fact_withdraw | WHERE withdraw_id | `WHERE withdraw_id IS NOT NULL` | `WHERE withdraw_id <> ''` |
| fact_bonus | WHERE user_id | `WHERE user_id IS NOT NULL` | `WHERE user_id <> ''` |

#### [DATA_FLOW_ANALYSIS.md](DATA_FLOW_ANALYSIS.md)
**修改内容**：更新了 stg 层处理的代码示例和说明

- 更新了 `stg_user` 示例中的 user_id 转换方式
- 更新了关键处理步骤中的类型转换说明
- 更新了 WHERE 条件说明

### 2. 受影响的下游处理

#### Python 代码 - 无需修改
- `src/autotag/load/normalize.py` - 直接执行 SQL 文件
- `src/autotag/load/build_mart.py` - 直接执行 SQL 文件
- `src/autotag/model/features.py` - 直接执行 SQL 文件
- `src/autotag/model/labeling.py` - 直接执行 SQL 文件
- `src/autotag/model/views_ops.py` - 直接执行 SQL 文件

> Python 代码主要作用是执行 SQL，没有硬编码的类型检查或类型转换逻辑，因此无需修改。

#### SQL JOIN/GROUP BY - 无需修改
- `mart.user_profile_daily` 中对 user_id 的 JOIN 操作自动支持 VARCHAR
- `mart.user_window_metrics` 中对 user_id 的 PARTITION BY 操作自动支持 VARCHAR
- `mart.user_last_activity` 中对 user_id 的 PARTITION BY 操作自动支持 VARCHAR
- `mart.user_state_engine` 中对 user_id 的 JOIN 操作自动支持 VARCHAR

> DuckDB SQL 的 JOIN、GROUP BY、PARTITION BY 操作不依赖数据类型，自动支持 VARCHAR。

## 类型转换影响评估

### ✅ 兼容性分析

| 操作 | 影响 | 说明 |
|------|------|------|
| **字符串函数** | ✅ 无影响 | TRIM、SUBSTR、REGEXP_REPLACE 等都是字符串操作 |
| **JOIN/GROUP BY** | ✅ 无影响 | DuckDB 支持任何类型的 JOIN 和 GROUP BY |
| **排序** | ✅ 无影响 | VARCHAR 排序使用字典序，结果可能不同但合理 |
| **比较操作** | ✅ 无影响 | `=`、`<>`、`IN` 等都支持 VARCHAR |
| **聚合函数** | ✅ 无影响 | COUNT、SUM、MAX、MIN 等不依赖类型 |
| **窗口函数** | ✅ 无影响 | ROW_NUMBER()、PARTITION BY 不依赖类型 |
| **NULL 检查** | ⚠️ 需要调整 | 从 `IS NOT NULL` 改为 `<> ''` |

### ⚠️ 潜在问题与解决方案

#### 问题1：字符串排序 vs 数字排序
**现象**：ID 排序结果不同
- **BIGINT 排序**：1, 2, 10, 100, ...（数字大小排序）
- **VARCHAR 排序**：1, 10, 100, 2, ...（字典序排序）

**解决方案**：
```sql
-- 如果需要数字排序，使用 TRY_CAST
ORDER BY TRY_CAST(user_id AS BIGINT) DESC
```

#### 问题2：前导零处理
**现象**：`"001"` 和 `"1"` 被视为不同值
- **BIGINT**：`001` → `1`（转换后相同）
- **VARCHAR**：`001` ≠ `1`（字符不同）

**解决方案**：确保输入数据已标准化，或在 JOIN 时使用转换：
```sql
LEFT JOIN mart.fact_user u ON CAST(e.user_id AS BIGINT) = u.user_id
```

#### 问题3：超大数字
**现象**：超过 BIGINT 范围的数字
- **BIGINT**：溢出报错
- **VARCHAR**：正常存储

**优势**：✅ 现在支持超大数字

## 验证清单

- [x] `src/autotag/db/sql/stg/build_stg.sql` - 所有 ID 字段已转换为 VARCHAR
- [x] `src/autotag/db/sql/mart/build_mart.sql` - WHERE 条件已更新
- [x] WHERE 条件使用 `<> ''` 而不是 `IS NOT NULL`
- [x] DATA_FLOW_ANALYSIS.md 代码示例已更新
- [x] 无 BIGINT ID 定义残留（验证：grep 无结果）
- [x] Python 代码无需修改
- [x] JOIN/GROUP BY/PARTITION BY 操作无需修改

## 回滚方案

如果需要回滚，运行以下命令恢复到 BIGINT：

```bash
# 恢复所有 TRY_CAST(...AS BIGINT)
# 恢复所有 WHERE ... IS NOT NULL
# 编辑 DATA_FLOW_ANALYSIS.md 中的代码示例
git checkout -- src/autotag/db/sql/stg/build_stg.sql
git checkout -- src/autotag/db/sql/mart/build_mart.sql
git checkout -- DATA_FLOW_ANALYSIS.md
```

## 测试建议

### 1. 单元测试
```sql
-- 测试 ID 为 VARCHAR 后的正常工作
SELECT COUNT(*) FROM stg.stg_user WHERE user_id <> '';
SELECT COUNT(*) FROM mart.fact_user;
```

### 2. 集成测试
```bash
python -m autotag.load.normalize --dt 2026-03-05
python -m autotag.load.build_mart --dt 2026-03-05
python -m autotag.model.features --dt 2026-03-05
python -m autotag.model.labeling --dt 2026-03-05
python -m autotag.model.views_ops --dt 2026-03-05
```

### 3. 数据质量检查
```sql
-- 检查 user_id 中是否有非数字值
SELECT DISTINCT user_id FROM stg.stg_user 
WHERE user_id ~ '[^0-9]' LIMIT 10;

-- 检查是否有重复的 user_id（可能来自前导零差异）
SELECT user_id, COUNT(*) cnt FROM stg.stg_user 
GROUP BY user_id HAVING cnt > 1;
```

## 修改历史

| 日期 | 修改内容 | 状态 |
|------|---------|------|
| 2026-03-05 | 初始 ID 类型迁移 | ✅ 完成 |

---

**修改人**：GitHub Copilot  
**审核状态**：待审核  
**生产部署**：未部署
