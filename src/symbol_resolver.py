"""统一股票代码解析器 — 支持模糊输入、多市场识别、自动补全后缀。

核心能力：
1. 纯数字识别：700 → 0700.HK（港股），600519 → 600519.SS（A股上交所）
2. 中文名/别名模糊搜索：腾讯 → 0700.HK，茅台 → 600519.SS
3. 带后缀直通：AAPL → AAPL，0700.HK → 0700.HK
4. 针对不同 API 输出对应格式（yfinance/FMP/Finnhub/Bloomberg）

设计原则：
- 不依赖外部 API 做 symbol lookup（避免额外网络请求）
- 内置常用股票别名映射 + 规则推断
- 可扩展：后续可接入在线 search API 做更完整的模糊查找
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

logger = logging.getLogger(__name__)


# ─── 数据结构 ───

@dataclass
class ResolvedSymbol:
    """解析后的标准化股票代码"""
    original_input: str          # 用户原始输入
    canonical: str               # 标准代码（如 0700.HK, 600519.SS, AAPL）
    market: str                  # 市场标识: US / HK / CN_SH / CN_SZ / JP / UK / ...
    name: str = ""               # 公司名称（如果已知）
    name_cn: str = ""            # 中文名称（如果已知）
    currency: str = ""           # 交易货币
    confidence: float = 1.0      # 识别置信度 (0-1)
    source: str = "rule"         # 识别来源: rule / alias / search

    # 各 API 格式
    @property
    def yfinance(self) -> str:
        """yfinance 格式：0700.HK, 600519.SS, AAPL"""
        return self.canonical

    @property
    def fmp(self) -> str:
        """FMP 格式：0700.HK, 600519.SS, AAPL"""
        return self.canonical

    @property
    def finnhub(self) -> str:
        """Finnhub 格式 — 需要交易所前缀"""
        _map = {
            "HK": self.canonical,               # 0700.HK 直接用
            "CN_SH": f"{self._digits}.SS",      # 上交所
            "CN_SZ": f"{self._digits}.SZ",      # 深交所
            "US": self._base,                    # AAPL
            "JP": self.canonical,
            "UK": self.canonical,
        }
        return _map.get(self.market, self.canonical)

    @property
    def bloomberg(self) -> str:
        """Bloomberg 格式：700 HK Equity, AAPL US Equity"""
        _map = {
            "HK": f"{self._digits_no_pad} HK Equity",
            "CN_SH": f"{self._digits} CH Equity",
            "CN_SZ": f"{self._digits} CH Equity",
            "US": f"{self._base} US Equity",
            "JP": f"{self._digits} JP Equity",
            "UK": f"{self._base} LN Equity",
        }
        return _map.get(self.market, f"{self._base} US Equity")

    @property
    def _base(self) -> str:
        """去掉后缀的基础代码"""
        return self.canonical.split(".")[0] if "." in self.canonical else self.canonical

    @property
    def _digits(self) -> str:
        """纯数字部分"""
        return re.sub(r"[^0-9]", "", self._base)

    @property
    def _digits_no_pad(self) -> str:
        """去掉前导零的数字"""
        return self._digits.lstrip("0") or "0"


# ─── 常用股票别名映射（中文名 → canonical） ───

ALIAS_MAP: Dict[str, Tuple[str, str, str]] = {
    # (canonical, market, name_en)

    # ── 港股热门 ──
    "腾讯": ("0700.HK", "HK", "Tencent Holdings"),
    "腾讯控股": ("0700.HK", "HK", "Tencent Holdings"),
    "tencent": ("0700.HK", "HK", "Tencent Holdings"),
    "阿里": ("9988.HK", "HK", "Alibaba Group"),
    "阿里巴巴": ("9988.HK", "HK", "Alibaba Group"),
    "alibaba": ("9988.HK", "HK", "Alibaba Group"),
    "baba": ("BABA", "US", "Alibaba Group"),
    "美团": ("3690.HK", "HK", "Meituan"),
    "meituan": ("3690.HK", "HK", "Meituan"),
    "京东": ("9618.HK", "HK", "JD.com"),
    "jd": ("JD", "US", "JD.com"),
    "小米": ("1810.HK", "HK", "Xiaomi"),
    "xiaomi": ("1810.HK", "HK", "Xiaomi"),
    "百度": ("9888.HK", "HK", "Baidu"),
    "baidu": ("BIDU", "US", "Baidu"),
    "网易": ("9999.HK", "HK", "NetEase"),
    "netease": ("NTES", "US", "NetEase"),
    "比亚迪": ("1211.HK", "HK", "BYD"),
    "byd": ("1211.HK", "HK", "BYD"),
    "汇丰": ("0005.HK", "HK", "HSBC Holdings"),
    "hsbc": ("0005.HK", "HK", "HSBC Holdings"),
    "友邦": ("1299.HK", "HK", "AIA Group"),
    "aia": ("1299.HK", "HK", "AIA Group"),
    "港交所": ("0388.HK", "HK", "HKEX"),
    "hkex": ("0388.HK", "HK", "HKEX"),
    "中移动": ("0941.HK", "HK", "China Mobile"),
    "中国移动": ("0941.HK", "HK", "China Mobile"),
    "建行": ("0939.HK", "HK", "CCB"),
    "建设银行": ("0939.HK", "HK", "CCB"),
    "工行": ("1398.HK", "HK", "ICBC"),
    "工商银行": ("1398.HK", "HK", "ICBC"),
    "中国平安": ("2318.HK", "HK", "Ping An"),
    "平安": ("2318.HK", "HK", "Ping An"),
    "快手": ("1024.HK", "HK", "Kuaishou"),
    "哔哩哔哩": ("9626.HK", "HK", "Bilibili"),
    "bilibili": ("BILI", "US", "Bilibili"),
    "bilibili港": ("9626.HK", "HK", "Bilibili"),
    "理想汽车": ("2015.HK", "HK", "Li Auto"),
    "蔚来": ("9866.HK", "HK", "NIO"),
    "nio": ("NIO", "US", "NIO"),
    "中芯国际": ("0981.HK", "HK", "SMIC"),
    "联想": ("0992.HK", "HK", "Lenovo"),

    # ── A 股热门 ──
    "茅台": ("600519.SS", "CN_SH", "Kweichow Moutai"),
    "贵州茅台": ("600519.SS", "CN_SH", "Kweichow Moutai"),
    "五粮液": ("000858.SZ", "CN_SZ", "Wuliangye"),
    "招商银行": ("600036.SS", "CN_SH", "CMB"),
    "招行": ("600036.SS", "CN_SH", "CMB"),
    "宁德时代": ("300750.SZ", "CN_SZ", "CATL"),
    "catl": ("300750.SZ", "CN_SZ", "CATL"),
    "中国中免": ("601888.SS", "CN_SH", "CTG Duty Free"),
    "隆基绿能": ("601012.SS", "CN_SH", "LONGi Green Energy"),
    "万科": ("000002.SZ", "CN_SZ", "Vanke"),
    "格力": ("000651.SZ", "CN_SZ", "Gree Electric"),
    "格力电器": ("000651.SZ", "CN_SZ", "Gree Electric"),
    "美的": ("000333.SZ", "CN_SZ", "Midea"),
    "美的集团": ("000333.SZ", "CN_SZ", "Midea"),
    "海天味业": ("603288.SS", "CN_SH", "Haitian"),
    "中国石油": ("601857.SS", "CN_SH", "PetroChina"),
    "中石油": ("601857.SS", "CN_SH", "PetroChina"),
    "中国银行": ("601988.SS", "CN_SH", "Bank of China"),
    "中信证券": ("600030.SS", "CN_SH", "CITIC Securities"),
    "比亚迪a": ("002594.SZ", "CN_SZ", "BYD"),
    "紫金矿业": ("601899.SS", "CN_SH", "Zijin Mining"),
    "恒瑞医药": ("600276.SS", "CN_SH", "Hengrui Medicine"),
    "药明康德": ("603259.SS", "CN_SH", "WuXi AppTec"),
    "长江电力": ("600900.SS", "CN_SH", "CYPC"),
    "泸州老窖": ("000568.SZ", "CN_SZ", "Luzhou Laojiao"),
    "片仔癀": ("600436.SS", "CN_SH", "Pien Tze Huang"),
    "伊利股份": ("600887.SS", "CN_SH", "Yili"),

    # ── 美股热门 ──
    "苹果": ("AAPL", "US", "Apple"),
    "apple": ("AAPL", "US", "Apple"),
    "微软": ("MSFT", "US", "Microsoft"),
    "microsoft": ("MSFT", "US", "Microsoft"),
    "谷歌": ("GOOGL", "US", "Alphabet"),
    "google": ("GOOGL", "US", "Alphabet"),
    "alphabet": ("GOOGL", "US", "Alphabet"),
    "亚马逊": ("AMZN", "US", "Amazon"),
    "amazon": ("AMZN", "US", "Amazon"),
    "特斯拉": ("TSLA", "US", "Tesla"),
    "tesla": ("TSLA", "US", "Tesla"),
    "英伟达": ("NVDA", "US", "NVIDIA"),
    "nvidia": ("NVDA", "US", "NVIDIA"),
    "meta": ("META", "US", "Meta Platforms"),
    "脸书": ("META", "US", "Meta Platforms"),
    "facebook": ("META", "US", "Meta Platforms"),
    "台积电": ("TSM", "US", "TSMC"),
    "tsmc": ("TSM", "US", "TSMC"),
    "伯克希尔": ("BRK-B", "US", "Berkshire Hathaway"),
    "berkshire": ("BRK-B", "US", "Berkshire Hathaway"),
    "巴菲特": ("BRK-B", "US", "Berkshire Hathaway"),
    "摩根大通": ("JPM", "US", "JPMorgan Chase"),
    "jpmorgan": ("JPM", "US", "JPMorgan Chase"),
    "高盛": ("GS", "US", "Goldman Sachs"),
    "可口可乐": ("KO", "US", "Coca-Cola"),
    "cocacola": ("KO", "US", "Coca-Cola"),
    "强生": ("JNJ", "US", "Johnson & Johnson"),
    "宝洁": ("PG", "US", "Procter & Gamble"),
    "迪士尼": ("DIS", "US", "Disney"),
    "disney": ("DIS", "US", "Disney"),
    "奈飞": ("NFLX", "US", "Netflix"),
    "netflix": ("NFLX", "US", "Netflix"),
    "amd": ("AMD", "US", "AMD"),
    "intel": ("INTC", "US", "Intel"),
    "英特尔": ("INTC", "US", "Intel"),
}

# ── 港股数字代码 → 名称映射（常用的，补齐） ──
HK_NUMBER_MAP: Dict[str, Tuple[str, str]] = {
    # (canonical_with_suffix, company_name)
    "700": ("0700.HK", "Tencent Holdings"),
    "0700": ("0700.HK", "Tencent Holdings"),
    "9988": ("9988.HK", "Alibaba Group"),
    "3690": ("3690.HK", "Meituan"),
    "9618": ("9618.HK", "JD.com"),
    "1810": ("1810.HK", "Xiaomi"),
    "9888": ("9888.HK", "Baidu"),
    "9999": ("9999.HK", "NetEase"),
    "1211": ("1211.HK", "BYD"),
    "5": ("0005.HK", "HSBC Holdings"),
    "0005": ("0005.HK", "HSBC Holdings"),
    "1299": ("1299.HK", "AIA Group"),
    "388": ("0388.HK", "HKEX"),
    "0388": ("0388.HK", "HKEX"),
    "941": ("0941.HK", "China Mobile"),
    "0941": ("0941.HK", "China Mobile"),
    "939": ("0939.HK", "CCB"),
    "0939": ("0939.HK", "CCB"),
    "1398": ("1398.HK", "ICBC"),
    "2318": ("2318.HK", "Ping An"),
    "1024": ("1024.HK", "Kuaishou"),
    "9626": ("9626.HK", "Bilibili"),
    "2015": ("2015.HK", "Li Auto"),
    "9866": ("9866.HK", "NIO"),
    "981": ("0981.HK", "SMIC"),
    "0981": ("0981.HK", "SMIC"),
    "992": ("0992.HK", "Lenovo"),
    "0992": ("0992.HK", "Lenovo"),
    "2382": ("2382.HK", "Sunny Optical"),
    "1177": ("1177.HK", "Sino Biopharm"),
    "2269": ("2269.HK", "WuXi Biologics"),
    "6098": ("6098.HK", "Country Garden Services"),
    "1833": ("1833.HK", "Ping An Healthcare"),
    "772": ("0772.HK", "China Literature"),
    "0772": ("0772.HK", "China Literature"),
    "241": ("0241.HK", "Alibaba Health"),
    "0241": ("0241.HK", "Alibaba Health"),
    "3988": ("3988.HK", "Bank of China HK"),
    "2628": ("2628.HK", "China Life"),
    "883": ("0883.HK", "CNOOC"),
    "0883": ("0883.HK", "CNOOC"),
    "1": ("0001.HK", "CK Hutchison"),
    "0001": ("0001.HK", "CK Hutchison"),
    "16": ("0016.HK", "SHK Properties"),
    "0016": ("0016.HK", "SHK Properties"),
    "27": ("0027.HK", "Galaxy Entertainment"),
    "0027": ("0027.HK", "Galaxy Entertainment"),
    "2020": ("2020.HK", "ANTA Sports"),
    "1928": ("1928.HK", "Sands China"),
}


# ─── 核心解析函数 ───

def resolve_symbol(raw_input: str) -> ResolvedSymbol:
    """将用户输入解析为标准化股票代码。

    支持的输入格式：
    - 标准美股: AAPL, MSFT, BRK-B
    - 带后缀: 0700.HK, 600519.SS, 9988.HK
    - 纯数字港股: 700, 0700, 9988, 5
    - 6位数字A股: 600519, 000858, 300750
    - 中文名: 腾讯, 茅台, 英伟达
    - 英文别名: tencent, apple, nvidia
    """
    raw = raw_input.strip()
    if not raw:
        return ResolvedSymbol(original_input=raw, canonical=raw, market="UNKNOWN")

    raw_lower = raw.lower()
    raw_upper = raw.upper()

    # 1) 别名映射（中文/英文名 → 精确匹配）
    if raw_lower in ALIAS_MAP:
        canonical, market, name_en = ALIAS_MAP[raw_lower]
        return ResolvedSymbol(
            original_input=raw, canonical=canonical, market=market,
            name=name_en, name_cn=raw if _is_chinese(raw) else "",
            confidence=1.0, source="alias",
        )

    # 2) 已带后缀 — 直接使用
    suffix_match = re.match(r'^([A-Za-z0-9]+)\.([A-Z]{1,4})$', raw_upper)
    if suffix_match:
        base, suffix = suffix_match.groups()
        market = _suffix_to_market(suffix)
        return ResolvedSymbol(
            original_input=raw, canonical=raw_upper, market=market,
            confidence=1.0, source="rule",
        )

    # 3) 纯数字 — 判断市场
    if raw.isdigit():
        return _resolve_numeric(raw, raw_upper)

    # 4) 字母+数字混合且无后缀 — 可能是美股或需要带后缀
    #    如 BRK-B, BABA 等
    if re.match(r'^[A-Za-z][A-Za-z0-9\-\.]*$', raw):
        return ResolvedSymbol(
            original_input=raw, canonical=raw_upper, market="US",
            confidence=0.9, source="rule",
        )

    # 5) 中文 — 尝试模糊搜索
    if _is_chinese(raw):
        result = _fuzzy_search_alias(raw)
        if result:
            return result

    # 6) 无法识别 — 返回原样（当作美股处理）
    logger.warning(f"[symbol_resolver] 无法识别 '{raw}'，按美股代码处理")
    return ResolvedSymbol(
        original_input=raw, canonical=raw_upper, market="US",
        confidence=0.5, source="fallback",
    )


def resolve_symbols(raw_list: List[str]) -> List[ResolvedSymbol]:
    """批量解析股票代码"""
    return [resolve_symbol(s) for s in raw_list]


def resolve_for_provider(raw_input: str, provider: str = "yfinance") -> str:
    """一步到位：解析并返回指定 provider 需要的格式。

    Args:
        raw_input: 用户输入
        provider: "yfinance" / "fmp" / "finnhub" / "bloomberg"

    Returns:
        对应 API 格式的 symbol 字符串
    """
    resolved = resolve_symbol(raw_input)
    fmt_map = {
        "yfinance": resolved.yfinance,
        "yahoo": resolved.yfinance,
        "yahoo_direct": resolved.yfinance,
        "fmp": resolved.fmp,
        "finnhub": resolved.finnhub,
        "bloomberg": resolved.bloomberg,
    }
    return fmt_map.get(provider.lower(), resolved.canonical)


def search_symbols(query: str, limit: int = 10) -> List[ResolvedSymbol]:
    """模糊搜索股票代码/名称，返回候选列表（用于前端自动补全）。"""
    query_lower = query.strip().lower()
    if not query_lower:
        return []

    results = []

    # 搜索别名
    for alias, (canonical, market, name_en) in ALIAS_MAP.items():
        if query_lower in alias or alias in query_lower:
            results.append(ResolvedSymbol(
                original_input=query, canonical=canonical, market=market,
                name=name_en, confidence=1.0 if alias == query_lower else 0.8,
                source="alias",
            ))

    # 搜索港股数字映射
    if query.isdigit():
        for num, (canonical, name) in HK_NUMBER_MAP.items():
            if num.startswith(query) or query.startswith(num):
                results.append(ResolvedSymbol(
                    original_input=query, canonical=canonical, market="HK",
                    name=name, confidence=0.9, source="alias",
                ))

    # 去重（按 canonical）
    seen = set()
    deduped = []
    for r in results:
        if r.canonical not in seen:
            seen.add(r.canonical)
            deduped.append(r)

    # 按置信度排序
    deduped.sort(key=lambda x: x.confidence, reverse=True)
    return deduped[:limit]


# ─── 内部辅助函数 ───

def _resolve_numeric(raw: str, raw_upper: str) -> ResolvedSymbol:
    """解析纯数字输入"""
    num = int(raw)
    digits = raw.lstrip("0") or "0"

    # 先查港股映射表
    if raw in HK_NUMBER_MAP or digits in HK_NUMBER_MAP:
        key = raw if raw in HK_NUMBER_MAP else digits
        canonical, name = HK_NUMBER_MAP[key]
        return ResolvedSymbol(
            original_input=raw, canonical=canonical, market="HK",
            name=name, currency="HKD", confidence=1.0, source="alias",
        )

    # 6位数字 → A股
    if len(raw) == 6:
        return _resolve_a_share(raw)

    # 1-4位数字 → 港股（通用规则）
    if 1 <= len(digits) <= 4:
        padded = digits.zfill(4) + ".HK"
        return ResolvedSymbol(
            original_input=raw, canonical=padded, market="HK",
            currency="HKD", confidence=0.8, source="rule",
        )

    # 5位数字 → 也可能是港股（如 09988 → 9988.HK）
    if len(raw) == 5 and raw.startswith("0"):
        canonical = raw[1:] + ".HK"
        return ResolvedSymbol(
            original_input=raw, canonical=canonical, market="HK",
            currency="HKD", confidence=0.7, source="rule",
        )

    # 其他 → 无法判断，按美股
    return ResolvedSymbol(
        original_input=raw, canonical=raw_upper, market="US",
        confidence=0.3, source="fallback",
    )


def _resolve_a_share(code: str) -> ResolvedSymbol:
    """解析6位A股代码"""
    prefix = code[:3]
    # 上交所: 600xxx, 601xxx, 603xxx, 605xxx, 688xxx(科创板)
    if prefix in ("600", "601", "603", "605", "688"):
        return ResolvedSymbol(
            original_input=code, canonical=f"{code}.SS", market="CN_SH",
            currency="CNY", confidence=0.95, source="rule",
        )
    # 深交所: 000xxx, 001xxx(主板), 002xxx(中小板), 003xxx
    # 300xxx, 301xxx(创业板)
    if prefix in ("000", "001", "002", "003", "300", "301"):
        return ResolvedSymbol(
            original_input=code, canonical=f"{code}.SZ", market="CN_SZ",
            currency="CNY", confidence=0.95, source="rule",
        )
    # 北交所: 8xxxxx, 4xxxxx
    if code[0] in ("8", "4"):
        return ResolvedSymbol(
            original_input=code, canonical=f"{code}.BJ", market="CN_BJ",
            currency="CNY", confidence=0.85, source="rule",
        )
    # 未知6位数字
    return ResolvedSymbol(
        original_input=code, canonical=f"{code}.SS", market="CN_SH",
        currency="CNY", confidence=0.5, source="fallback",
    )


def _suffix_to_market(suffix: str) -> str:
    """后缀 → 市场标识"""
    _map = {
        "HK": "HK", "SS": "CN_SH", "SH": "CN_SH",
        "SZ": "CN_SZ", "BJ": "CN_BJ",
        "T": "JP", "L": "UK", "TO": "CA",
        "AX": "AU", "SI": "SG", "KS": "KR",
        "TW": "TW", "NS": "IN", "BO": "IN",
    }
    return _map.get(suffix, "US")


def _is_chinese(s: str) -> bool:
    """检查字符串是否包含中文"""
    return bool(re.search(r'[\u4e00-\u9fff]', s))


def _fuzzy_search_alias(query: str) -> Optional[ResolvedSymbol]:
    """在别名表中模糊搜索中文名"""
    query_lower = query.lower()
    best_match = None
    best_score = 0

    for alias, (canonical, market, name_en) in ALIAS_MAP.items():
        if not _is_chinese(alias):
            continue
        # 完全包含
        if query_lower in alias or alias in query_lower:
            score = len(alias) / max(len(query_lower), 1)
            if alias == query_lower:
                score = 2.0  # 精确匹配最高
            if score > best_score:
                best_score = score
                best_match = (canonical, market, name_en, alias)

    if best_match:
        canonical, market, name_en, matched_alias = best_match
        return ResolvedSymbol(
            original_input=query, canonical=canonical, market=market,
            name=name_en, name_cn=matched_alias,
            confidence=min(best_score, 1.0), source="alias",
        )
    return None
