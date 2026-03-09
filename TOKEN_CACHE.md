# Token 缓存与登录重试功能说明

## 概述

优化了远程数据获取流程中的登录机制，实现了 Token 缓存和自动重试功能，避免每次数据拉取都需要重新登录。

## 主要改进

### 1. Token 缓存机制
- **缓存位置**：`~/.autotag/cache/api_token.json`
- **有效期**：48小时（可配置）
- **自动过期检查**：Token 过期时自动清除并重新登录
- **持久化存储**：跨进程/会话保留 Token

### 2. 登录重试机制
- **重试次数**：最多 3 次（可配置）
- **退避策略**：指数退避，间隔为 2s, 4s, 8s
- **失败处理**：所有重试失败后抛出异常，不影响已有的错误处理逻辑

### 3. 性能收益
- **首次登录**：正常登录流程（~2 秒）
- **后续 48 小时内**：跳过登录，直接使用缓存 Token（节省 2 秒/次）
- **周日全量任务**：可节省 ~50 秒登录时间（25 次任务 × 2s）

## 实现细节

### 新增模块：[token_cache.py](../src/autotag/ingest/token_cache.py)

#### TokenInfo 类
```python
@dataclass
class TokenInfo:
    access_token: str          # Token 字符串
    created_at: str            # 创建时间 (ISO格式)
    ttl_hours: int = 48        # 有效期（小时）
    
    def is_expired(self) -> bool        # 检查过期
    def to_dict(self) -> dict           # 序列化
    @classmethod
    def from_dict(cls, data) -> TokenInfo  # 反序列化
```

#### TokenCache 类
```python
class TokenCache:
    def get_valid_token() -> TokenInfo | None    # 获取有效Token，过期返回None
    def save_token(token, ttl_hours=48)          # 保存Token到缓存
    def clear()                                   # 清除缓存
    def get_or_refresh() -> str | None           # 获取Token字符串或None
```

### 修改的函数

#### _login_with_retry(base_url, username, password, totp_secret, max_retries=3)
新增函数，实现登录重试逻辑：
- 最多重试 3 次
- 每次失败后采用指数退避（2s, 4s, 8s）
- 成功时立即返回 Token
- 所有重试失败时抛出异常

#### _remote_fetch() 改进
```python
# 原逻辑：每次调用都登录
# 新逻辑：
# 1. 检查缓存是否有有效Token
# 2. 如果有，直接使用（跳过登录）
# 3. 如果无，执行 _login_with_retry()
# 4. 登录成功后保存到缓存（48小时有效期）
```

## 使用示例

### 第一次运行
```bash
# 首次登录，会进行身份验证并保存Token
$ bash scripts/run_daily.sh
[ingest] no valid cached token, performing login
[ingest] login success at attempt 1/3
[token_cache] token saved (ttl: 48h)
[ingest] remote fetch started...
```

### 后续 48 小时内运行
```bash
# 直接使用缓存Token，跳过登录
$ bash scripts/run_daily.sh
[ingest] using cached token, skipping login
[token_cache] using cached token (created: 2026-03-09T10:13:04)
[ingest] remote fetch started...
```

### Token 过期后运行
```bash
# Token 过期，自动重新登录
$ bash scripts/run_daily.sh
[token_cache] token expired at 2026-03-07T10:13:04
[token_cache] token cleared
[ingest] no valid cached token, performing login
[ingest] login success at attempt 1/3
[token_cache] token saved (ttl: 48h)
```

## 缓存管理

### 查看缓存
```bash
cat ~/.autotag/cache/api_token.json
```

### 手动清除缓存
```bash
rm -rf ~/.autotag/cache/api_token.json
```

### 环境变量配置（未来扩展）
```bash
# 可在 .env 中配置（当前硬编码为 48小时和3次重试）
# TOKEN_CACHE_TTL_HOURS=48
# LOGIN_MAX_RETRIES=3
```

## 测试

运行测试套件验证功能：
```bash
cd /Users/momo/Desktop/autotag
PYTHONPATH=src python tests/test_token_cache.py
```

测试覆盖：
- ✓ 获取不存在的 Token
- ✓ 保存 Token
- ✓ 读取有效 Token
- ✓ 清除 Token
- ✓ Token 过期检测
- ✓ TokenInfo 序列化/反序列化

## 向后兼容性

- ✓ 无破坏性改变
- ✓ Token 缓存文件不存在时自动重新登录
- ✓ 无需修改现有脚本或调用方式

## 故障排查

### 问题：仍然频繁登录
**原因**：Token 缓存文件权限问题或磁盘空间不足  
**解决**：
```bash
rm -rf ~/.autotag/cache
chmod 700 ~/.autotag/cache
```

### 问题：登录失败次数过多
**原因**：可能是网络问题或服务端故障  
**查看日志**：查阅 `logs/daily/` 中的日志文件

### 问题：Token 缓存未被使用
**验证**：检查日志是否包含 `using cached token` 或 `no valid cached token`

## 后续优化方向

1. **可配置 TTL**：将 48小时改为环境变量配置
2. **可配置重试次数**：将 3 次改为环境变量配置
3. **Token 刷新**：支持主动刷新而非等待过期
4. **多 API 支持**：如需多个不同 API 的 Token，可扩展为字典存储
5. **加密存储**：对缓存的 Token 进行加密存储（若有安全需求）
