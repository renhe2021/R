"""数据源工厂 - 根据配置选择数据提供者

数据源优先级策略:
  - "bloomberg_first": Bloomberg 优先，失败时返回状态（需用户选择备选源），不自动降级
  - "auto": 自动降级 Bloomberg → FMP → Finnhub → yfinance（旧行为，保留兼容）
  - "<provider_name>": 直接指定

缓存策略:
  - 所有 provider 自动包装 CachingProvider（可通过 use_cache=False 关闭）
  - 默认 TTL=8h，交易日内复用
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from .base import DataProvider

logger = logging.getLogger(__name__)

# ── 缓存配置 ──────────────────────────────────────────────────
# 磁盘缓存默认关闭：数据量不大（30股 × 5s/股 ≈ 2.5min），每次 Pipeline 重新获取更可靠。
# 磁盘缓存在并发场景下容易缓存脏数据（已被 Bloomberg 串线问题证实）。
# Pipeline 内同次运行的数据复用（data_map 传递到 Stage 4/5/6）依然有效，不受此开关影响。
_CACHE_ENABLED: bool = False  # Global cache switch — 默认关闭
_CACHE_TTL_HOURS: float = 8.0


def set_cache_enabled(enabled: bool):
    """Enable/disable the disk cache globally."""
    global _CACHE_ENABLED
    _CACHE_ENABLED = enabled
    logger.info(f"[Cache] 磁盘缓存{'已启用' if enabled else '已关闭'}")


def _wrap_with_cache(provider: DataProvider) -> DataProvider:
    """Wrap a provider with CachingProvider if caching is enabled."""
    if not _CACHE_ENABLED:
        return provider
    try:
        from .cache import CachingProvider
        # 如果已经是 CachingProvider，不要重复包装
        if isinstance(provider, CachingProvider):
            return provider
        return CachingProvider(provider, ttl_hours=_CACHE_TTL_HOURS)
    except Exception as e:
        logger.warning(f"[Cache] 无法启用缓存，使用原始 provider: {e}")
        return provider


# ── Provider 实例缓存（单例模式） ─────────────────────────────
# 避免每次 get_data_provider() 都创建新实例（尤其是 Bloomberg 等有状态连接的 provider）
import threading

_provider_instances: dict = {}  # key: provider_name -> CachingProvider(inner)
_provider_lock = threading.Lock()


def _get_or_create_provider(key: str, factory_fn) -> DataProvider:
    """线程安全的 provider 单例获取。
    
    Args:
        key: 缓存键（如 "bloomberg", "yfinance"）
        factory_fn: 创建 provider 的工厂函数（无参数，返回 DataProvider）
    
    Returns:
        缓存的或新创建的 provider 实例（已包装 CachingProvider）
    """
    with _provider_lock:
        if key in _provider_instances:
            cached = _provider_instances[key]
            logger.debug(f"[Factory] 复用已有 provider: {key} ({type(cached).__name__})")
            return cached
        
        provider = factory_fn()
        wrapped = _wrap_with_cache(provider)
        _provider_instances[key] = wrapped
        logger.info(f"[Factory] 创建新 provider: {key} ({type(wrapped).__name__})")
        return wrapped


def clear_provider_cache(provider_name: str = None):
    """清除 provider 实例缓存。
    
    Args:
        provider_name: 指定清除某个 provider，None 则清除全部。
    """
    with _provider_lock:
        if provider_name:
            removed = _provider_instances.pop(provider_name, None)
            if removed:
                # 如果有 close 方法，显式关闭连接
                inner = getattr(removed, '_inner', removed)
                if hasattr(inner, 'close'):
                    try:
                        inner.close()
                    except Exception:
                        pass
                logger.info(f"[Factory] 已清除 provider 缓存: {provider_name}")
        else:
            for name, prov in list(_provider_instances.items()):
                inner = getattr(prov, '_inner', prov)
                if hasattr(inner, 'close'):
                    try:
                        inner.close()
                    except Exception:
                        pass
            _provider_instances.clear()
            logger.info("[Factory] 已清除所有 provider 缓存")


# ── 数据源探测结果 ──────────────────────────────────────────────

@dataclass
class DataSourceProbeResult:
    """Bloomberg-first 探测结果：告诉调用者 Bloomberg 是否可用，若不可用给出备选列表。"""
    bloomberg_available: bool = False
    provider: Optional[DataProvider] = None  # 成功时的 provider
    available_alternatives: List[str] = field(default_factory=list)  # 备选源名称
    error_message: str = ""  # Bloomberg 失败原因


def probe_bloomberg_first(**kwargs) -> DataSourceProbeResult:
    """尝试 Bloomberg，成功则返回 provider，失败则列出所有可用备选数据源（不自动降级）。

    单例策略: Bloomberg provider 通过 _get_or_create_provider 复用，
    避免每次 probe 都创建新的 blpapi session（连接风暴）。

    Returns:
        DataSourceProbeResult: bloomberg_available=True 时 provider 已就绪；
                               False 时 available_alternatives 列出可选备选源。
    """
    result = DataSourceProbeResult()

    # 1. 尝试 Bloomberg — 通过 get_data_provider 走单例路径
    try:
        provider = get_data_provider("bloomberg", **kwargs)
        logger.info("[bloomberg_first] Bloomberg 可用（单例复用）")
        result.bloomberg_available = True
        result.provider = provider
        return result
    except ImportError:
        result.error_message = "blpapi 未安装"
    except Exception as e:
        err_msg = str(e)
        if "Terminal 未启动" in err_msg or "未安装" in err_msg:
            result.error_message = "blpapi 已安装但 Bloomberg Terminal 未启动"
        else:
            result.error_message = f"Bloomberg 连接失败: {err_msg[:200]}"

    logger.info(f"[bloomberg_first] Bloomberg 不可用: {result.error_message}")

    # 2. 探测所有可用备选源
    #    注意：轻量 provider（yfinance 等）无状态连接，创建实例成本极低
    alternatives: List[str] = []

    # yfinance（免费，最可靠）
    try:
        from .yfinance_provider import YfinanceProvider
        yf = YfinanceProvider()
        if yf.is_available():
            alternatives.append("yfinance")
    except Exception:
        pass

    # FMP
    try:
        from .fmp_provider import FMPProvider
        fmp_key = kwargs.get("api_key", "") or kwargs.get("fmp_api_key", "")
        if fmp_key:
            fp = FMPProvider(api_key=fmp_key)
            if fp.is_available():
                alternatives.append("fmp")
    except Exception:
        pass

    # Finnhub
    try:
        from .finnhub_provider import FinnhubProvider
        fh_key = kwargs.get("finnhub_api_key", "")
        if fh_key:
            fh = FinnhubProvider(api_key=fh_key)
            if fh.is_available():
                alternatives.append("finnhub")
    except Exception:
        pass

    # Yahoo Direct
    try:
        from .yahoo_direct_provider import YahooDirectProvider
        yd = YahooDirectProvider()
        if yd.is_available():
            alternatives.append("yahoo_direct")
    except Exception:
        pass

    result.available_alternatives = alternatives
    logger.info(f"[bloomberg_first] 可用备选数据源: {alternatives}")
    return result


def get_data_provider(provider_name: str = "auto", **kwargs) -> DataProvider:
    """获取数据提供者实例（单例模式，同一 provider_name 复用同一实例）

    Args:
        provider_name: "bloomberg" / "yfinance" / "fmp" / "finnhub" / "yahoo_direct" / "auto"
        **kwargs: 传递给 provider 的参数 (如 host, port, api_key)

    Note:
        "bloomberg_first" 模式请使用 probe_bloomberg_first() 函数代替，
        因为该模式需要与用户交互（选择备选数据源），不适合直接返回 provider。

    单例策略:
        同一 provider_name 在进程生命周期内只创建一个实例。
        这对 Bloomberg 等有状态连接的 provider 尤其重要，
        避免每次调用都创建新 session（连接风暴）。
    """
    if provider_name == "bloomberg":
        def _make_bloomberg():
            from .bloomberg import BloombergProvider
            host = kwargs.get("host", "localhost")
            port = kwargs.get("port", 8194)
            provider = BloombergProvider(host=host, port=port)
            if not provider.is_available():
                raise ImportError(
                    "blpapi 未安装。请运行:\n"
                    "  pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi\n"
                    "并确保 Bloomberg Terminal 已启动。"
                )
            return provider
        return _get_or_create_provider("bloomberg", _make_bloomberg)

    if provider_name == "yfinance":
        def _make_yfinance():
            from .yfinance_provider import YfinanceProvider
            return YfinanceProvider()
        return _get_or_create_provider("yfinance", _make_yfinance)

    if provider_name == "yahoo_direct":
        def _make_yahoo_direct():
            from .yahoo_direct_provider import YahooDirectProvider
            return YahooDirectProvider()
        return _get_or_create_provider("yahoo_direct", _make_yahoo_direct)

    if provider_name == "fmp":
        def _make_fmp():
            from .fmp_provider import FMPProvider
            api_key = kwargs.get("api_key", "")
            provider = FMPProvider(api_key=api_key)
            if not provider.is_available():
                raise ValueError(
                    "FMP 需要 API Key。请在 config.yaml 中配置:\n"
                    "  data:\n"
                    "    fmp:\n"
                    "      api_key: \"your_key\"\n"
                    "免费申请: https://financialmodelingprep.com/developer/docs/"
                )
            return provider
        return _get_or_create_provider("fmp", _make_fmp)

    if provider_name == "finnhub":
        def _make_finnhub():
            from .finnhub_provider import FinnhubProvider
            api_key = kwargs.get("api_key", "")
            provider = FinnhubProvider(api_key=api_key)
            if not provider.is_available():
                raise ValueError(
                    "Finnhub 需要 API Key。请在 config.yaml 中配置:\n"
                    "  data:\n"
                    "    finnhub:\n"
                    "      api_key: \"your_key\"\n"
                    "免费申请: https://finnhub.io/register"
                )
            return provider
        return _get_or_create_provider("finnhub", _make_finnhub)

    # auto: 优先 Bloomberg -> FMP -> Finnhub -> yfinance (保留旧行为兼容)
    if provider_name == "auto":
        # auto 模式也走单例 — 一旦确定了最佳 provider，后续调用直接复用
        with _provider_lock:
            if "auto" in _provider_instances:
                return _provider_instances["auto"]

        # 尝试各数据源
        try:
            from .bloomberg import BloombergProvider
            bp = BloombergProvider(**kwargs)
            if bp.is_available():
                logger.info("[auto] 使用 Bloomberg 数据源")
                wrapped = _wrap_with_cache(bp)
                with _provider_lock:
                    _provider_instances["auto"] = wrapped
                return wrapped
        except Exception:
            pass

        try:
            from .fmp_provider import FMPProvider
            fmp_key = kwargs.get("api_key", "") or kwargs.get("fmp_api_key", "")
            if fmp_key:
                fp = FMPProvider(api_key=fmp_key)
                if fp.is_available():
                    logger.info("[auto] 使用 FMP 数据源")
                    wrapped = _wrap_with_cache(fp)
                    with _provider_lock:
                        _provider_instances["auto"] = wrapped
                    return wrapped
        except Exception:
            pass

        try:
            from .finnhub_provider import FinnhubProvider
            fh_key = kwargs.get("finnhub_api_key", "")
            if fh_key:
                fh = FinnhubProvider(api_key=fh_key)
                if fh.is_available():
                    logger.info("[auto] 使用 Finnhub 数据源")
                    wrapped = _wrap_with_cache(fh)
                    with _provider_lock:
                        _provider_instances["auto"] = wrapped
                    return wrapped
        except Exception:
            pass

        from .yfinance_provider import YfinanceProvider
        yf = YfinanceProvider()
        if yf.is_available():
            logger.info("[auto] 降级使用 yfinance")
            wrapped = _wrap_with_cache(yf)
            with _provider_lock:
                _provider_instances["auto"] = wrapped
            return wrapped

        raise RuntimeError("没有可用的数据源。请安装 blpapi 或 yfinance。")

    raise ValueError(f"未知数据源: {provider_name}，可选: bloomberg / yfinance / fmp / finnhub / auto")


def get_all_available_providers(**kwargs) -> list[DataProvider]:
    """获取所有可用的数据提供者（用于交叉验证）"""
    providers = []

    # yfinance
    try:
        from .yfinance_provider import YfinanceProvider
        yf = YfinanceProvider()
        if yf.is_available():
            providers.append(yf)
    except Exception:
        pass

    # yahoo_direct (独立第二源)
    try:
        from .yahoo_direct_provider import YahooDirectProvider
        yd = YahooDirectProvider()
        if yd.is_available():
            providers.append(yd)
    except Exception:
        pass

    # FMP
    try:
        from .fmp_provider import FMPProvider
        api_key = kwargs.get("fmp_api_key", "")
        if api_key:
            fmp = FMPProvider(api_key=api_key)
            if fmp.is_available():
                providers.append(fmp)
    except Exception:
        pass

    # Finnhub
    try:
        from .finnhub_provider import FinnhubProvider
        fh_key = kwargs.get("finnhub_api_key", "")
        if fh_key:
            fh = FinnhubProvider(api_key=fh_key)
            if fh.is_available():
                providers.append(fh)
    except Exception:
        pass

    # Bloomberg — 走单例路径，避免额外创建 session
    try:
        bp = get_data_provider("bloomberg", **kwargs)
        providers.append(bp)
    except Exception:
        pass

    return providers
