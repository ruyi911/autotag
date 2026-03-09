# Skip Download Mode (--skip-download)

## 概述
当使用 `bash scripts/run_daily.sh --skip-download` 时，脚本会：
1. **跳过数据下载**（ingest.downloader）
2. **加载已有数据**到本地数据仓库（raw_import）
3. **处理并发布数据**到 Metabase（normalize → build_mart → features → labeling → views_ops → publish）

## 门禁行为
在 `--skip-download` 模式下，以下门禁会被**自动禁用**：

| 门禁 | 环境变量 | 原因 |
|------|---------|------|
| **登录时间新鲜度** | `ENABLE_LOGIN_FRESHNESS_GATE=0` | 没有新数据，登录时间不会更新 |
| **订单状态漂移** | `ENABLE_STATUS_DRIFT_GATE=0` | 没有新数据，状态不会发生变化 |

这些禁用是自动的，无需手动配置。

## 使用场景

### 场景 1：仅发布数据到 Metabase（推荐）
```bash
bash scripts/run_daily.sh --skip-download
```
- ✅ 使用已有的本地数据
- ✅ 执行完整的 ETL 流程（归一化、特征、标签等）
- ✅ 将结果发布到 Metabase
- ✅ 门禁检查被禁用（无误报）
- ⏱️ 耗时：约 20 秒

### 场景 2：完整的日常更新（包含下载）
```bash
bash scripts/run_daily.sh
```
- 下载最新数据
- 执行 ETL
- 门禁检查**启用**（验证数据质量）
- ⏱️ 耗时：约 3-5 分钟

### 场景 3：指定特定日期
```bash
bash scripts/run_daily.sh --skip-download 2026-03-07
```
以日期 `2026-03-07` 的数据重新发布。

## 技术细节

脚本在跳过下载时的操作：

```bash
if [[ "$SKIP_DOWNLOAD" -eq 0 ]]; then
  # 正常下载流程
  PYTHONPATH=src .venv/bin/python -m autotag.ingest.downloader --fetch ...
else
  # 跳过下载，禁用门禁
  export ENABLE_LOGIN_FRESHNESS_GATE=0
  export ENABLE_STATUS_DRIFT_GATE=0
fi
```

这确保：
1. 不会尝试从远端 API 获取数据
2. 门禁检查读取 `env` 变量，跳过那些对增量数据无意义的检查
3. 其他常规检查（如关键列完整性）仍然运行

## 日志示例

```
[2026-03-09 17:38:15] skip ingest.downloader by --skip-download
[2026-03-09 17:38:15] start load.raw_import
[2026-03-09 17:38:21] start load.normalize
[2026-03-09 17:38:31] start load.build_mart
[2026-03-09 17:38:32] start model.views_ops
[2026-03-09 17:38:32] start publish.validate
✓ All gating checks passed (with freshness/drift disabled)
[2026-03-09 17:38:33] start publish.snapshot
[2026-03-09 17:38:34] completed ✓
```

## 故障排除

### Q: 为什么跳过下载后仍然很慢？
A: ETL 流程（建立 mart、构建特征等）仍然需要时间。加速方法：
- `bash scripts/run_daily.sh --skip-download --no-publish` 仅处理不发布

### Q: 可以选择发布特定的数据源吗？
A: 暂不支持。如需细粒度控制，请直接调用相关模块，如：
```bash
PYTHONPATH=src python -m autotag.load.normalize --dt 2026-03-08
```

### Q: 门禁完全不运行了吗？
A: 不是。以下检查**仍然运行**：
- ✅ 关键对象存在且非空
- ✅ 关键列不为空
- ✅ 日期范围合法性
- ❌ 登录时间新鲜度（禁用）
- ❌ 订单状态漂移（禁用）

---

**推荐用法**：在已成功下载数据后，使用 `--skip-download` 来快速重新发布和调试。
