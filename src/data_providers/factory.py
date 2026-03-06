"""数据源工厂 - 根据配置选择数据提供者"""

import logging
from .base import DataProvider

logger = logging.getLogger(__name__)


def get_data_provider(provider_name: str = "auto", **kwargs) -> DataProvider:
    """获取数据提供者实例

    Args:
        provider_name: "bloomberg" / "yfinance" / "fmp" / "finnhub" / "auto"
        **kwargs: 传递给 provider 的参数 (如 host, port, api_key)
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

    # auto: 优先 Bloomberg -> FMP -> Finnhub -> yfinance
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
