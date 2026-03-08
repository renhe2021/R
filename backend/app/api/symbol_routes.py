"""Symbol search API — fuzzy search for stock tickers/names."""

import logging
import re
from typing import Optional

from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/symbol", tags=["symbol"])

# ── Built-in symbol dictionary for fast local search ──
# Format: (symbol, name_en, name_cn, market, exchange)
_BUILTIN_SYMBOLS: list[tuple[str, str, str, str, str]] = [
    # US — Mega Caps
    ("AAPL", "Apple Inc.", "苹果", "US", "NASDAQ"),
    ("MSFT", "Microsoft Corporation", "微软", "US", "NASDAQ"),
    ("GOOGL", "Alphabet Inc. Class A", "谷歌A", "US", "NASDAQ"),
    ("GOOG", "Alphabet Inc. Class C", "谷歌C", "US", "NASDAQ"),
    ("AMZN", "Amazon.com Inc.", "亚马逊", "US", "NASDAQ"),
    ("NVDA", "NVIDIA Corporation", "英伟达", "US", "NASDAQ"),
    ("META", "Meta Platforms Inc.", "Meta", "US", "NASDAQ"),
    ("TSLA", "Tesla Inc.", "特斯拉", "US", "NASDAQ"),
    ("BRK-A", "Berkshire Hathaway Inc. Class A", "伯克希尔A", "US", "NYSE"),
    ("BRK-B", "Berkshire Hathaway Inc. Class B", "伯克希尔B", "US", "NYSE"),
    ("AVGO", "Broadcom Inc.", "博通", "US", "NASDAQ"),
    ("LLY", "Eli Lilly and Company", "礼来", "US", "NYSE"),
    ("JPM", "JPMorgan Chase & Co.", "摩根大通", "US", "NYSE"),
    ("V", "Visa Inc.", "Visa", "US", "NYSE"),
    ("UNH", "UnitedHealth Group Inc.", "联合健康", "US", "NYSE"),
    ("MA", "Mastercard Incorporated", "万事达", "US", "NYSE"),
    ("XOM", "Exxon Mobil Corporation", "埃克森美孚", "US", "NYSE"),
    ("JNJ", "Johnson & Johnson", "强生", "US", "NYSE"),
    ("PG", "Procter & Gamble Co.", "宝洁", "US", "NYSE"),
    ("HD", "The Home Depot Inc.", "家得宝", "US", "NYSE"),
    ("COST", "Costco Wholesale Corporation", "好市多", "US", "NASDAQ"),
    ("ABBV", "AbbVie Inc.", "艾伯维", "US", "NYSE"),
    ("MRK", "Merck & Co. Inc.", "默沙东", "US", "NYSE"),
    ("KO", "The Coca-Cola Company", "可口可乐", "US", "NYSE"),
    ("PEP", "PepsiCo Inc.", "百事可乐", "US", "NASDAQ"),
    ("CRM", "Salesforce Inc.", "赛富时", "US", "NYSE"),
    ("ADBE", "Adobe Inc.", "Adobe", "US", "NASDAQ"),
    ("WMT", "Walmart Inc.", "沃尔玛", "US", "NYSE"),
    ("BAC", "Bank of America Corp.", "美国银行", "US", "NYSE"),
    ("NFLX", "Netflix Inc.", "奈飞", "US", "NASDAQ"),
    ("AMD", "Advanced Micro Devices Inc.", "AMD", "US", "NASDAQ"),
    ("TMO", "Thermo Fisher Scientific Inc.", "赛默飞", "US", "NYSE"),
    ("CSCO", "Cisco Systems Inc.", "思科", "US", "NASDAQ"),
    ("DIS", "The Walt Disney Company", "迪士尼", "US", "NYSE"),
    ("INTC", "Intel Corporation", "英特尔", "US", "NASDAQ"),
    ("NKE", "NIKE Inc.", "耐克", "US", "NYSE"),
    ("T", "AT&T Inc.", "AT&T", "US", "NYSE"),
    ("VZ", "Verizon Communications Inc.", "威瑞森", "US", "NYSE"),
    ("BA", "The Boeing Company", "波音", "US", "NYSE"),
    ("CAT", "Caterpillar Inc.", "卡特彼勒", "US", "NYSE"),
    ("IBM", "International Business Machines", "IBM", "US", "NYSE"),
    ("GS", "Goldman Sachs Group Inc.", "高盛", "US", "NYSE"),
    ("AXP", "American Express Company", "美国运通", "US", "NYSE"),
    ("MMM", "3M Company", "3M", "US", "NYSE"),
    ("GE", "General Electric Company", "通用电气", "US", "NYSE"),
    ("HON", "Honeywell International", "霍尼韦尔", "US", "NASDAQ"),
    ("UPS", "United Parcel Service", "UPS", "US", "NYSE"),
    ("MCD", "McDonald's Corporation", "麦当劳", "US", "NYSE"),
    ("AMGN", "Amgen Inc.", "安进", "US", "NASDAQ"),
    ("CVX", "Chevron Corporation", "雪佛龙", "US", "NYSE"),
    ("WFC", "Wells Fargo & Company", "富国银行", "US", "NYSE"),
    ("C", "Citigroup Inc.", "花旗集团", "US", "NYSE"),
    ("TRV", "The Travelers Companies", "旅行者保险", "US", "NYSE"),
    ("DOW", "Dow Inc.", "陶氏", "US", "NYSE"),
    ("WBA", "Walgreens Boots Alliance", "沃博联", "US", "NASDAQ"),
    ("ABT", "Abbott Laboratories", "雅培", "US", "NYSE"),
    ("O", "Realty Income Corporation", "Realty Income", "US", "NYSE"),
    ("MO", "Altria Group Inc.", "奥驰亚", "US", "NYSE"),
    ("EMR", "Emerson Electric Co.", "艾默生", "US", "NYSE"),
    ("QCOM", "QUALCOMM Incorporated", "高通", "US", "NASDAQ"),
    ("TXN", "Texas Instruments Inc.", "德州仪器", "US", "NASDAQ"),
    ("SBUX", "Starbucks Corporation", "星巴克", "US", "NASDAQ"),
    ("PYPL", "PayPal Holdings Inc.", "PayPal", "US", "NASDAQ"),
    ("SQ", "Block Inc.", "Block", "US", "NYSE"),
    ("SHOP", "Shopify Inc.", "Shopify", "US", "NYSE"),
    ("SNAP", "Snap Inc.", "Snap", "US", "NYSE"),
    ("UBER", "Uber Technologies Inc.", "优步", "US", "NYSE"),
    ("ABNB", "Airbnb Inc.", "爱彼迎", "US", "NASDAQ"),
    ("PLTR", "Palantir Technologies", "Palantir", "US", "NYSE"),
    ("COIN", "Coinbase Global Inc.", "Coinbase", "US", "NASDAQ"),
    ("ARM", "Arm Holdings plc", "ARM", "US", "NASDAQ"),
    ("PANW", "Palo Alto Networks", "Palo Alto", "US", "NASDAQ"),
    ("SNOW", "Snowflake Inc.", "Snowflake", "US", "NYSE"),
    ("NET", "Cloudflare Inc.", "Cloudflare", "US", "NYSE"),
    ("CRWD", "CrowdStrike Holdings", "CrowdStrike", "US", "NASDAQ"),
    ("DDOG", "Datadog Inc.", "Datadog", "US", "NASDAQ"),
    ("ZS", "Zscaler Inc.", "Zscaler", "US", "NASDAQ"),
    ("NOW", "ServiceNow Inc.", "ServiceNow", "US", "NYSE"),
    ("SPOT", "Spotify Technology", "Spotify", "US", "NYSE"),
    ("SE", "Sea Limited", "Sea", "US", "NYSE"),
    ("BABA", "Alibaba Group Holding", "阿里巴巴", "US", "NYSE"),
    ("PDD", "PDD Holdings Inc.", "拼多多", "US", "NASDAQ"),
    ("JD", "JD.com Inc.", "京东", "US", "NASDAQ"),
    ("BIDU", "Baidu Inc.", "百度", "US", "NASDAQ"),
    ("NIO", "NIO Inc.", "蔚来", "US", "NYSE"),
    ("XPEV", "XPeng Inc.", "小鹏汽车", "US", "NYSE"),
    ("LI", "Li Auto Inc.", "理想汽车", "US", "NASDAQ"),
    ("BILI", "Bilibili Inc.", "哔哩哔哩", "US", "NASDAQ"),
    ("TME", "Tencent Music Entertainment", "腾讯音乐", "US", "NYSE"),
    ("ZTO", "ZTO Express", "中通快递", "US", "NYSE"),
    ("FUTU", "Futu Holdings Limited", "富途", "US", "NASDAQ"),
    ("TAL", "TAL Education Group", "好未来", "US", "NYSE"),
    ("VNET", "VNET Group Inc.", "世纪互联", "US", "NASDAQ"),

    # HK — Major stocks
    ("0700.HK", "Tencent Holdings Limited", "腾讯控股", "HK", "HKEX"),
    ("9988.HK", "Alibaba Group Holding", "阿里巴巴", "HK", "HKEX"),
    ("0005.HK", "HSBC Holdings plc", "汇丰控股", "HK", "HKEX"),
    ("0941.HK", "China Mobile Limited", "中国移动", "HK", "HKEX"),
    ("1299.HK", "AIA Group Limited", "友邦保险", "HK", "HKEX"),
    ("0388.HK", "Hong Kong Exchanges", "香港交易所", "HK", "HKEX"),
    ("0001.HK", "CK Hutchison Holdings", "长江和记", "HK", "HKEX"),
    ("0016.HK", "Sun Hung Kai Properties", "新鸿基地产", "HK", "HKEX"),
    ("0002.HK", "CLP Holdings Limited", "中电控股", "HK", "HKEX"),
    ("2318.HK", "Ping An Insurance", "中国平安", "HK", "HKEX"),
    ("3690.HK", "Meituan", "美团", "HK", "HKEX"),
    ("9618.HK", "JD.com Inc.", "京东集团", "HK", "HKEX"),
    ("9999.HK", "NetEase Inc.", "网易", "HK", "HKEX"),
    ("1810.HK", "Xiaomi Corporation", "小米集团", "HK", "HKEX"),
    ("2020.HK", "ANTA Sports Products", "安踏体育", "HK", "HKEX"),
    ("0027.HK", "Galaxy Entertainment", "银河娱乐", "HK", "HKEX"),
    ("1928.HK", "Sands China Ltd.", "金沙中国", "HK", "HKEX"),
    ("2688.HK", "ENN Energy Holdings", "新奥能源", "HK", "HKEX"),
    ("0011.HK", "Hang Seng Bank Limited", "恒生银行", "HK", "HKEX"),
    ("0003.HK", "The Hong Kong and China Gas", "香港中华煤气", "HK", "HKEX"),
    ("0006.HK", "Power Assets Holdings", "电能实业", "HK", "HKEX"),
    ("0012.HK", "Henderson Land Development", "恒基兆业地产", "HK", "HKEX"),
    ("0017.HK", "New World Development", "新世界发展", "HK", "HKEX"),
    ("0066.HK", "MTR Corporation Limited", "港铁公司", "HK", "HKEX"),
    ("0101.HK", "Hang Lung Properties", "恒隆地产", "HK", "HKEX"),
    ("0175.HK", "Geely Automobile Holdings", "吉利汽车", "HK", "HKEX"),
    ("0241.HK", "Alibaba Health Information", "阿里健康", "HK", "HKEX"),
    ("0267.HK", "CITIC Limited", "中信股份", "HK", "HKEX"),
    ("0288.HK", "WH Group Limited", "万洲国际", "HK", "HKEX"),
    ("0386.HK", "China Petroleum & Chemical", "中国石化", "HK", "HKEX"),
    ("0688.HK", "China Overseas Land", "中国海外发展", "HK", "HKEX"),
    ("0762.HK", "China Unicom (Hong Kong)", "中国联通", "HK", "HKEX"),
    ("0857.HK", "PetroChina Company Limited", "中国石油", "HK", "HKEX"),
    ("0883.HK", "CNOOC Limited", "中国海洋石油", "HK", "HKEX"),
    ("0939.HK", "China Construction Bank", "建设银行", "HK", "HKEX"),
    ("1038.HK", "CK Infrastructure Holdings", "长江基建", "HK", "HKEX"),
    ("1088.HK", "China Shenhua Energy", "中国神华", "HK", "HKEX"),
    ("1109.HK", "China Resources Land", "华润置地", "HK", "HKEX"),
    ("1177.HK", "Sino Biopharmaceutical", "中国生物制药", "HK", "HKEX"),
    ("1211.HK", "BYD Company Limited", "比亚迪", "HK", "HKEX"),
    ("1288.HK", "Agricultural Bank of China", "农业银行", "HK", "HKEX"),
    ("1398.HK", "Industrial & Commercial Bank", "工商银行", "HK", "HKEX"),
    ("1876.HK", "Budweiser Brewing Co APAC", "百威亚太", "HK", "HKEX"),
    ("2007.HK", "Country Garden Holdings", "碧桂园", "HK", "HKEX"),
    ("2269.HK", "WuXi Biologics", "药明生物", "HK", "HKEX"),
    ("2313.HK", "Shenzhou International", "申洲国际", "HK", "HKEX"),
    ("2328.HK", "PICC Property and Casualty", "中国财险", "HK", "HKEX"),
    ("2382.HK", "Sunny Optical Technology", "舜宇光学", "HK", "HKEX"),
    ("2628.HK", "China Life Insurance", "中国人寿", "HK", "HKEX"),
    ("3328.HK", "Bank of Communications", "交通银行", "HK", "HKEX"),
    ("3988.HK", "Bank of China Limited", "中国银行", "HK", "HKEX"),
    ("6098.HK", "Country Garden Services", "碧桂园服务", "HK", "HKEX"),
    ("6862.HK", "Haidilao International", "海底捞", "HK", "HKEX"),
    ("9626.HK", "Bilibili Inc.", "哔哩哔哩", "HK", "HKEX"),
    ("9888.HK", "Baidu Inc.", "百度集团", "HK", "HKEX"),
    ("9961.HK", "Trip.com Group", "携程集团", "HK", "HKEX"),
    ("9868.HK", "XPeng Inc.", "小鹏汽车", "HK", "HKEX"),
    ("9866.HK", "NIO Inc.", "蔚来汽车", "HK", "HKEX"),
    ("2015.HK", "Li Auto Inc.", "理想汽车", "HK", "HKEX"),

    # CN-A — Shanghai/Shenzhen
    ("600519.SS", "Kweichow Moutai Co.", "贵州茅台", "CN-A", "SSE"),
    ("601318.SS", "Ping An Insurance", "中国平安", "CN-A", "SSE"),
    ("600036.SS", "China Merchants Bank", "招商银行", "CN-A", "SSE"),
    ("601012.SS", "LONGi Green Energy", "隆基绿能", "CN-A", "SSE"),
    ("600276.SS", "Jiangsu Hengrui Medicine", "恒瑞医药", "CN-A", "SSE"),
    ("601888.SS", "China Tourism Group Duty Free", "中国中免", "CN-A", "SSE"),
    ("600900.SS", "China Yangtze Power", "长江电力", "CN-A", "SSE"),
    ("600030.SS", "CITIC Securities", "中信证券", "CN-A", "SSE"),
    ("601166.SS", "Industrial Bank Co.", "兴业银行", "CN-A", "SSE"),
    ("000858.SZ", "Wuliangye Yibin Co.", "五粮液", "CN-A", "SZSE"),
    ("000333.SZ", "Midea Group Co.", "美的集团", "CN-A", "SZSE"),
    ("000651.SZ", "Gree Electric Appliances", "格力电器", "CN-A", "SZSE"),
    ("002594.SZ", "BYD Company Limited", "比亚迪", "CN-A", "SZSE"),
    ("300750.SZ", "Contemporary Amperex", "宁德时代", "CN-A", "SZSE"),
    ("002415.SZ", "Hangzhou Hikvision", "海康威视", "CN-A", "SZSE"),
    ("300059.SZ", "East Money Information", "东方财富", "CN-A", "SZSE"),
    ("002304.SZ", "Jiangsu Yanghe Brewery", "洋河股份", "CN-A", "SZSE"),
    ("000568.SZ", "Luzhou Laojiao Co.", "泸州老窖", "CN-A", "SZSE"),
    ("002714.SZ", "Muyuan Foods Co.", "牧原股份", "CN-A", "SZSE"),

    # JP — Major Japan
    ("7203.T", "Toyota Motor Corporation", "丰田汽车", "JP", "TSE"),
    ("6758.T", "Sony Group Corporation", "索尼", "JP", "TSE"),
    ("6861.T", "Keyence Corporation", "基恩士", "JP", "TSE"),
    ("9984.T", "SoftBank Group Corp.", "软银集团", "JP", "TSE"),
    ("7974.T", "Nintendo Co. Ltd.", "任天堂", "JP", "TSE"),
    ("8306.T", "Mitsubishi UFJ Financial", "三菱UFJ", "JP", "TSE"),
    ("6501.T", "Hitachi Ltd.", "日立", "JP", "TSE"),
    ("6902.T", "DENSO Corporation", "电装", "JP", "TSE"),
    ("9432.T", "Nippon Telegraph & Telephone", "日本电信电话", "JP", "TSE"),

    # ETFs
    ("SPY", "SPDR S&P 500 ETF Trust", "标普500ETF", "US", "NYSE"),
    ("QQQ", "Invesco QQQ Trust", "纳指100ETF", "US", "NASDAQ"),
    ("IWM", "iShares Russell 2000 ETF", "罗素2000ETF", "US", "NYSE"),
    ("VTI", "Vanguard Total Stock Market", "全美股票ETF", "US", "NYSE"),
    ("VOO", "Vanguard S&P 500 ETF", "标普500ETF", "US", "NYSE"),
    ("ARKK", "ARK Innovation ETF", "ARK创新ETF", "US", "NYSE"),
    ("GLD", "SPDR Gold Shares", "黄金ETF", "US", "NYSE"),
    ("TLT", "iShares 20+ Year Treasury", "长期国债ETF", "US", "NASDAQ"),
    ("2800.HK", "Tracker Fund of Hong Kong", "盈富基金", "HK", "HKEX"),
    ("2828.HK", "Hang Seng China Enterprises", "恒生中国企业", "HK", "HKEX"),

    # UK
    ("SHEL", "Shell plc", "壳牌", "US", "NYSE"),
    ("BP", "BP p.l.c.", "英国石油", "US", "NYSE"),
    ("AZN", "AstraZeneca PLC", "阿斯利康", "US", "NASDAQ"),
    ("GSK", "GSK plc", "葛兰素史克", "US", "NYSE"),
]


def _normalize(s: str) -> str:
    """Normalize string for matching: lowercase, strip dots/dashes/spaces."""
    return re.sub(r'[\s.\-]', '', s.lower())


def _search_local(query: str, limit: int = 12) -> list[dict]:
    """Search the builtin symbol dictionary."""
    q = _normalize(query)
    q_lower = query.lower().strip()

    results = []
    for sym, name_en, name_cn, market, exchange in _BUILTIN_SYMBOLS:
        sym_norm = _normalize(sym)
        name_norm = name_en.lower()

        # Scoring: exact symbol match > prefix > contains
        score = 0
        if sym_norm == q or sym.lower() == q_lower:
            score = 100
        elif sym_norm.startswith(q):
            score = 80
        elif q in sym_norm:
            score = 60
        elif q in name_norm:
            score = 40
        elif q in name_cn:
            score = 40
        else:
            continue

        results.append({
            "symbol": sym,
            "name": name_en,
            "name_cn": name_cn,
            "market": market,
            "exchange": exchange,
            "_score": score,
        })

    # Sort by score desc, then symbol length (shorter = better)
    results.sort(key=lambda r: (-r["_score"], len(r["symbol"])))

    # Remove internal score
    for r in results:
        del r["_score"]

    return results[:limit]


async def _search_yfinance(query: str, limit: int = 8) -> list[dict]:
    """Fallback: use yfinance search API for symbols not in local dict."""
    try:
        import asyncio
        import yfinance as yf

        def _do_search():
            try:
                # yfinance >= 0.2.31 has a search method
                results = []
                tickers = yf.Tickers(query)
                # Actually, yfinance doesn't have a great search API
                # Use the download to validate a single ticker
                t = yf.Ticker(query)
                info = t.info or {}
                if info.get("symbol") or info.get("shortName"):
                    results.append({
                        "symbol": info.get("symbol", query.upper()),
                        "name": info.get("shortName", info.get("longName", "")),
                        "name_cn": "",
                        "market": _guess_market(info.get("exchange", ""), info.get("symbol", query)),
                        "exchange": info.get("exchange", ""),
                    })
                return results
            except Exception:
                return []

        return await asyncio.get_event_loop().run_in_executor(None, _do_search)
    except Exception as e:
        logger.debug(f"yfinance search failed: {e}")
        return []


def _guess_market(exchange: str, symbol: str) -> str:
    """Guess market from exchange name or symbol."""
    exchange_lower = exchange.lower()
    if "hk" in exchange_lower or ".HK" in symbol:
        return "HK"
    if "ss" in exchange_lower or "sz" in exchange_lower or ".SS" in symbol or ".SZ" in symbol:
        return "CN-A"
    if "tse" in exchange_lower or "jpx" in exchange_lower or ".T" in symbol:
        return "JP"
    return "US"


@router.get("/search")
async def search_symbols(
    q: str = Query(..., min_length=1, max_length=50, description="Search query"),
    limit: int = Query(12, ge=1, le=50),
):
    """Fuzzy search for stock symbols — searches local dictionary first,
    falls back to yfinance for unknown symbols."""

    # 1. Local dictionary search (fast)
    local_results = _search_local(q, limit=limit)

    # 2. If no local results and query looks like a ticker, try yfinance
    if len(local_results) < 3 and len(q) >= 1:
        try:
            yf_results = await _search_yfinance(q, limit=limit - len(local_results))
            # Deduplicate
            existing_symbols = {r["symbol"] for r in local_results}
            for r in yf_results:
                if r["symbol"] not in existing_symbols:
                    local_results.append(r)
        except Exception:
            pass

    return {"results": local_results[:limit]}
