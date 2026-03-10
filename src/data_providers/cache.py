"""磁盘缓存装饰器 — 包装任何 DataProvider，避免重复下载

设计原则:
  - 透明包装: CachingProvider IS-A DataProvider，对调用方完全透明
  - TTL 控制: 默认 8 小时，交易日内复用，盘后自动过期
  - 按日期组织: cache_dir/{symbol}_{provider}_{date}.json
  - 自动清理: 保留最近 3 天的缓存，清理更旧的
  - 强制刷新: force_refresh=True 跳过缓存直接下载
"""

import json
import logging
import os
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

from .base import DataProvider
from ..analyzer import StockData

logger = logging.getLogger(__name__)

# Default cache directory (relative to project root)
_DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "cache"


class CachingProvider(DataProvider):
    """透明磁盘缓存层，包装任何 DataProvider。

    Usage:
        inner = YfinanceProvider()
        cached = CachingProvider(inner)
        stock = cached.fetch("AAPL")  # 首次: 调用 inner.fetch + 写缓存
        stock = cached.fetch("AAPL")  # 后续: 直接读缓存 (TTL 内)
    """

    def __init__(
        self,
        inner: DataProvider,
        cache_dir: Optional[str] = None,
        ttl_hours: float = 8.0,
        max_age_days: int = 3,
    ):
        """
        Args:
            inner: The real data provider to wrap.
            cache_dir: Directory for cache files. Defaults to data/cache/.
            ttl_hours: Cache time-to-live in hours. Default 8h.
            max_age_days: Auto-cleanup caches older than this. Default 3 days.
        """
        self._inner = inner
        self._cache_dir = Path(cache_dir) if cache_dir else _DEFAULT_CACHE_DIR
        self._ttl = timedelta(hours=ttl_hours)
        self._max_age_days = max_age_days

        # Ensure cache dir exists
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        # Run cleanup on init (lazy, won't slow down)
        self._cleanup_old_caches()

    @property
    def name(self) -> str:
        return self._inner.name

    def is_available(self) -> bool:
        return self._inner.is_available()

    def fetch(self, symbol: str) -> StockData:
        """Fetch with disk cache. Returns cached data if fresh, otherwise fetches and caches."""
        cache_file = self._cache_path(symbol)

        # Try reading cache
        if cache_file.exists():
            age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if age < self._ttl:
                try:
                    cached_data = json.loads(cache_file.read_text(encoding="utf-8"))
                    stock = StockData.from_dict(cached_data)
                    logger.info(
                        f"[Cache] {symbol} — 命中磁盘缓存 "
                        f"(来源: {self._inner.name}, 缓存时间: {self._format_age(age)})"
                    )
                    return stock
                except Exception as e:
                    logger.warning(f"[Cache] {symbol} — 缓存读取失败，重新获取: {e}")

        # Cache miss or expired — fetch from real provider
        logger.info(f"[Cache] {symbol} — 缓存未命中，从 {self._inner.name} 获取")
        stock = self._inner.fetch(symbol)

        # Record fetch timestamp
        stock._fetched_at = datetime.now().isoformat()

        # 数据质量保护：不缓存核心覆盖率极低的数据（可能是获取失败或串线脏数据）
        min_cache_fields = 3  # 至少要有 3 个核心字段才缓存
        core_fields = ['price', 'pe', 'eps', 'market_cap', 'revenue', 'roe', 'pb', 'name']
        present_count = sum(1 for f in core_fields
                           if getattr(stock, f, None) is not None
                           and getattr(stock, f, None) != stock.symbol)
        if present_count < min_cache_fields:
            logger.warning(
                f"[Cache] {symbol} — 数据质量过低 ({present_count}/{len(core_fields)} 核心字段)，"
                f"跳过缓存以防保存脏数据"
            )
            return stock

        # Write to cache
        try:
            cache_data = stock.to_dict()
            cache_file.write_text(
                json.dumps(cache_data, ensure_ascii=False, default=str, indent=2),
                encoding="utf-8",
            )
            logger.info(f"[Cache] {symbol} — 已缓存到 {cache_file.name}")
        except Exception as e:
            logger.warning(f"[Cache] {symbol} — 缓存写入失败: {e}")

        return stock

    def invalidate(self, symbol: str):
        """Remove cached data for a specific symbol (force refresh on next fetch)."""
        cache_file = self._cache_path(symbol)
        if cache_file.exists():
            cache_file.unlink()
            logger.info(f"[Cache] {symbol} — 缓存已清除")

    def invalidate_all(self):
        """Clear all cached data."""
        count = 0
        for f in self._cache_dir.glob("*.json"):
            f.unlink()
            count += 1
        logger.info(f"[Cache] 已清除 {count} 个缓存文件")

    def get_cache_info(self, symbol: str) -> Optional[dict]:
        """Get cache status for a symbol.

        Returns:
            dict with keys: is_cached, cache_age, cache_age_str, fetched_at, source
            or None if not cached.
        """
        cache_file = self._cache_path(symbol)
        if not cache_file.exists():
            return None

        mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        age = datetime.now() - mtime
        is_fresh = age < self._ttl

        return {
            "is_cached": True,
            "is_fresh": is_fresh,
            "cache_age_seconds": age.total_seconds(),
            "cache_age_str": self._format_age(age),
            "cached_at": mtime.isoformat(),
            "source": self._inner.name,
            "cache_file": str(cache_file),
        }

    # ── Internal helpers ──

    def _cache_path(self, symbol: str) -> Path:
        """Build cache file path: {cache_dir}/{SYMBOL}_{provider}_{date}.json"""
        import re
        safe_symbol = re.sub(r'[/\.\s]+', '_', symbol.strip())
        return self._cache_dir / f"{safe_symbol}_{self._inner.name}_{date.today()}.json"

    def _cleanup_old_caches(self):
        """Remove cache files older than max_age_days."""
        if not self._cache_dir.exists():
            return
        cutoff = datetime.now() - timedelta(days=self._max_age_days)
        removed = 0
        try:
            for f in self._cache_dir.glob("*.json"):
                if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                    f.unlink()
                    removed += 1
            if removed:
                logger.info(f"[Cache] 自动清理了 {removed} 个过期缓存文件")
        except Exception as e:
            logger.warning(f"[Cache] 自动清理失败: {e}")

    @staticmethod
    def _format_age(age: timedelta) -> str:
        """Format timedelta as human-readable string."""
        total_seconds = int(age.total_seconds())
        if total_seconds < 60:
            return f"{total_seconds}秒前"
        minutes = total_seconds // 60
        if minutes < 60:
            return f"{minutes}分钟前"
        hours = minutes // 60
        remaining_min = minutes % 60
        if hours < 24:
            return f"{hours}h{remaining_min:02d}m前"
        days = hours // 24
        return f"{days}天前"
