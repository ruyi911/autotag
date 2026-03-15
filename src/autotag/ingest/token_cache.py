"""Token缓存和管理模块，支持持久化和自动过期。"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


@dataclass
class TokenInfo:
    """Token信息容器。"""
    access_token: str
    created_at: str  # ISO格式时间戳
    ttl_hours: int = 48  # 默认48小时有效

    def is_expired(self) -> bool:
        """检查token是否过期。"""
        created = datetime.fromisoformat(self.created_at)
        expires_at = created + timedelta(hours=self.ttl_hours)
        return datetime.now(UTC).replace(tzinfo=None) > expires_at.replace(tzinfo=None)

    def to_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "created_at": self.created_at,
            "ttl_hours": self.ttl_hours,
        }

    @classmethod
    def from_dict(cls, data: dict) -> TokenInfo:
        return cls(
            access_token=data["access_token"],
            created_at=data["created_at"],
            ttl_hours=data.get("ttl_hours", 48),
        )


def build_token_namespace(base_url: str, username: str) -> str:
    """按接口环境+账号构造token命名空间，避免跨账号串用缓存。"""
    return f"{base_url.rstrip('/')}|{username.strip()}"


class TokenCache:
    """Token缓存管理器。"""

    def __init__(self, cache_dir: Path | None = None, namespace: str = ""):
        """初始化缓存。

        Args:
            cache_dir: 缓存目录，默认为~/.autotag/cache
            namespace: 缓存命名空间。为空时兼容历史默认文件名。
        """
        if cache_dir is None:
            cache_dir = Path.home() / ".autotag" / "cache"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.namespace = namespace.strip()
        if self.namespace:
            digest = hashlib.sha256(self.namespace.encode("utf-8")).hexdigest()[:16]
            self.token_file = self.cache_dir / f"api_token_{digest}.json"
        else:
            self.token_file = self.cache_dir / "api_token.json"

    def get_valid_token(self) -> TokenInfo | None:
        """获取有效的缓存token。

        Returns:
            有效的TokenInfo，如果不存在或已过期则返回None
        """
        if not self.token_file.exists():
            return None

        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            token_info = TokenInfo.from_dict(data)

            if token_info.is_expired():
                print(f"[token_cache] token expired at {token_info.created_at}", flush=True)
                self.clear()
                return None

            print(f"[token_cache] using cached token (created: {token_info.created_at})", flush=True)
            return token_info
        except Exception as exc:
            print(f"[token_cache] failed to load cached token: {exc}", flush=True)
            self.clear()
            return None

    def save_token(self, token: str, ttl_hours: int = 48) -> TokenInfo:
        """保存token到缓存。

        Args:
            token: 访问token
            ttl_hours: token有效期（小时）

        Returns:
            保存的TokenInfo
        """
        token_info = TokenInfo(
            access_token=token,
            created_at=datetime.now(UTC).isoformat(timespec="seconds"),
            ttl_hours=ttl_hours,
        )

        try:
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(token_info.to_dict(), f, ensure_ascii=False, indent=2)
            print(f"[token_cache] token saved (ttl: {ttl_hours}h)", flush=True)
            return token_info
        except Exception as exc:
            print(f"[token_cache] failed to save token: {exc}", flush=True)
            raise

    def clear(self) -> None:
        """清除缓存的token。"""
        if self.token_file.exists():
            try:
                self.token_file.unlink()
                print(f"[token_cache] token cleared", flush=True)
            except Exception as exc:
                print(f"[token_cache] failed to clear token: {exc}", flush=True)

    def get_or_refresh(self) -> str | None:
        """获取有效的token。

        如果缓存中有有效token，返回；否则返回None。
        注意：调用者需要处理None情况，执行登录逻辑。

        Returns:
            有效的token字符串，或None
        """
        token_info = self.get_valid_token()
        return token_info.access_token if token_info else None
