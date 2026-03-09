"""数据源工厂 - 根据配置选择数据提供者

数据源优先级策略:
  - "bloomberg_first": Bloomberg 优先，失败时返回状态（需用户选择备选源），不自动降级
  - "auto": 自动降级 Bloomberg → FMP → Finnhub → yfinance（旧行为，保留兼容）
  - "<provider_name>": 直接指定
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from .base import DataProvider

logger = logging.getLogger(__name__)


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

    Returns:
        DataSourceProbeResult: bloomberg_available=True 时 provider 已就绪；
                               False 时 available_alternatives 列出可选备选源。
    """
    result = DataSourceProbeResult()

    # 1. 尝试 Bloomberg
    try:
        from .bloomberg import BloombergProvider
        bp = BloombergProvider(
            host=kwargs.get("host", "localhost"),
            port=kwargs.get("port", 8194),
        )
        if bp.is_available():
            logger.info("[bloomberg_first] Bloomberg 可用")
            result.bloomberg_available = True
            result.provider = bp
            return result
        else:
            result.error_message = "blpapi 已安装但 Bloomberg Terminal 未启动"
    except ImportError:
        result.error_message = "blpapi 未安装"
    except Exception as e:
        result.error_message = f"Bloomberg 连接失败: {str(e)[:200]}"

    logger.info(f"[bloomberg_first] Bloomberg 不可用: {result.error_message}")

    # 2. 探测所有可用备选源（不创建连接，只检测可用性）
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
    """获取数据提供者实例

    Args:
        provider_name: "bloomberg" / "yfinance" / "fmp" / "finnhub" / "yahoo_direct" / "auto"
        **kwargs: 传递给 provider 的参数 (如 host, port, api_key)

    Note:
        "bloomberg_first" 模式请使用 probe_bloomberg_first() 函数代替，
        因为该模式需要与用户交互（选择备选数据源），不适合直接返回 provider。
    """
    if provider_name == "bloomberg":
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

    if provider_name == "yfinance":
        from .yfinance_provider import YfinanceProvider
        return YfinanceProvider()

    if provider_name == "yahoo_direct":
        from .yahoo_direct_provider import YahooDirectProvider
        return YahooDirectProvider()

    if provider_name == "fmp":
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

    if provider_name == "finnhub":
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

    # auto: 优先 Bloomberg -> FMP -> Finnhub -> yfinance (保留旧行为兼容)
    if provider_name == "auto":
        try:
            from .bloomberg import BloombergProvider
            bp = BloombergProvider(**kwargs)
            if bp.is_available():
                logger.info("[auto] 使用 Bloomberg 数据源")
                return bp
        except Exception:
            pass

        try:
            from .fmp_provider import FMPProvider
            fmp_key = kwargs.get("api_key", "") or kwargs.get("fmp_api_key", "")
            if fmp_key:
                fp = FMPProvider(api_key=fmp_key)
                if fp.is_available():
                    logger.info("[auto] 使用 FMP 数据源")
                    return fp
        except Exception:
            pass

        try:
            from .finnhub_provider import FinnhubProvider
            fh_key = kwargs.get("finnhub_api_key", "")
            if fh_key:
                fh = FinnhubProvider(api_key=fh_key)
                if fh.is_available():
                    logger.info("[auto] 使用 Finnhub 数据源")
                    return fh
        except Exception:
            pass

        from .yfinance_provider import YfinanceProvider
        yf = YfinanceProvider()
        if yf.is_available():
            logger.info("[auto] 降级使用 yfinance")
            return yf

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

    # Bloomberg
    try:
        from .bloomberg import BloombergProvider
        bp = BloombergProvider(
            host=kwargs.get("host", "localhost"),
            port=kwargs.get("port", 8194),
        )
        if bp.is_available():
            providers.append(bp)
    except Exception:
        pass

    return providers
