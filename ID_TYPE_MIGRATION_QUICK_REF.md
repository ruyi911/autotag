# ID 类型迁移 - 影响分析速查表

## 📋 修改总览

### 修改的文件（3个）
1. **build_stg.sql** - 5 个表的 8 个 ID 字段
2. **build_mart.sql** - 3 个表的 WHERE 条件  
3. **DATA_FLOW_ANALYSIS.md** - 代码示例和说明

### 修改的字段（8个）
- `stg_user.user_id`
- `stg_user.parent_user_id`
- `stg_recharge.user_id`
- `stg_bet.bet_id`
- `stg_bet.user_id`
- `stg_withdraw.withdraw_id`
- `stg_withdraw.user_id`
- `stg_bonus.user_id`

## 🔄 类型转换对比

### 前后对比

```diff
- TRY_CAST(TRIM(BOTH '''' FROM "ID") AS BIGINT) AS user_id
+ TRIM(BOTH '''' FROM "ID") AS user_id

- WHERE TRY_CAST(TRIM(BOTH '''' FROM "ID") AS BIGINT) IS NOT NULL
+ WHERE TRIM(BOTH '''' FROM "ID") <> ''
```

## ✅ 不受影响的操作

| 操作类型 | 示例 | 原因 |
|---------|------|------|
| **JOIN** | `ON u.user_id = e.user_id` | DuckDB 自动处理类型比较 |
| **GROUP BY** | `GROUP BY user_id` | 分组键类型无关 |
| **PARTITION BY** | `PARTITION BY user_id` | 窗口函数类型无关 |
| **ORDER BY** | `ORDER BY user_id DESC` | 字典序排序 |
| **DISTINCT** | `DISTINCT user_id` | 去重类型无关 |
| **COUNT/SUM** | `COUNT(DISTINCT user_id)` | 聚合类型无关 |
| **WHERE IN** | `WHERE user_id IN (...)` | IN 操作类型兼容 |

## ⚠️ 可能需要调整的场景

### 1. 如果需要数字排序
```sql
-- 字典序（当前行为）
ORDER BY user_id DESC
-- 结果：100, 50, 9, 8, 7...

-- 数字排序（如果需要）
ORDER BY TRY_CAST(user_id AS BIGINT) DESC
-- 结果：100, 50, 9, 8, 7...
```

### 2. 如果处理前导零重复
```sql
-- 检查潜在重复
SELECT TRY_CAST(user_id AS BIGINT) AS numeric_id, COUNT(*)
FROM stg.stg_user
GROUP BY TRY_CAST(user_id AS BIGINT)
HAVING COUNT(*) > 1;
```

### 3. 如果需要数值比较
```sql
-- VARCHAR 比较（字典序）
WHERE user_id > '100'  -- 返回: 101, 102, ... 899, 9, 90...

-- 数值比较（转换后）
WHERE TRY_CAST(user_id AS BIGINT) > 100  -- 返回: 101, 102, ...
```

## 🔍 验证步骤

### 第1步：检查修改
```bash
# 确认没有 BIGINT ID 定义
grep -r "AS BIGINT" src/autotag/db/sql/

# 应该返回：No output（无结果）
```

### 第2步：检查 WHERE 条件
```bash
# 检查 WHERE 条件更新
grep "WHERE.*<> ''" src/autotag/db/sql/stg/build_stg.sql
# 应该返回 5 行（5 个表）

grep "WHERE.*<> ''" src/autotag/db/sql/mart/build_mart.sql
# 应该返回 3 行（3 个表）
```

### 第3步：运行测试
```bash
# 构建 STG 层
python -m autotag.load.normalize --dt 2026-03-05

# 构建 MART 层
python -m autotag.load.build_mart --dt 2026-03-05

# 检查数据
duckdb data/db/serving.duckdb \
  "SELECT COUNT(*), COUNT(DISTINCT user_id) FROM stg.stg_user"
```

## 🚀 后续建议

### 短期（立即）
- [x] 修改 SQL 文件
- [x] 修改 WHERE 条件
- [x] 更新文档

### 中期（本周）
- [ ] 运行集成测试
- [ ] 验证数据质量
- [ ] 检查是否有排序依赖数字大小的代码

### 长期（可选）
- [ ] 评估性能影响
- [ ] 考虑在特定查询中使用 CAST 优化排序
- [ ] 更新 Metabase 视图层的显示格式

## 📞 常见问题

**Q: 为什么要改成 VARCHAR？**
A: 支持非纯数字的 ID、超大数字，以及不同的 ID 格式。

**Q: 会不会变慢？**
A: 基本无影响。DuckDB 优化器自动处理 VARCHAR 的 JOIN 和 GROUP BY，性能差异忽略不计。

**Q: 排序结果会不同吗？**
A: 是的，如果之前依赖数字排序。使用 `CAST(id AS BIGINT)` 可恢复原排序。

**Q: 需要备份吗？**
A: 建议备份 `data/db/serving.duckdb`，但回滚很简单（git checkout）。

**Q: 旧数据怎么办？**
A: STG 和 MART 层每次都重建（`CREATE OR REPLACE TABLE`），会自动用新类型。
