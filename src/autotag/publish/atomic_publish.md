# metabase.duckdb 原子发布策略

1. 在发布前先执行 `validate.py` 做门禁校验，未通过直接失败。
2. 以 `data/db/metabase.duckdb.tmp` 作为临时目标库，先完整构建发布内容。
3. 只复制 `serving.duckdb` 中 `ops` schema 的对象到临时库（物化为表，读库只读）。
4. 临时库再次做非空检查（`ops."用户状态总览"` 必须有数据）。
5. 所有步骤成功后，使用 `os.replace(tmp, metabase.duckdb)` 原子替换。
6. 任一步骤失败时不会触碰旧 `metabase.duckdb`，旧库保持可读。

该策略满足“失败不覆盖旧文件”的发布要求。
