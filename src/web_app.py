"""Web UI - 投资选股分析面板"""

import dataclasses
import json
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from .config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder=None)
CORS(app)

# 全局配置
_config = None


def get_config():
    global _config
    if _config is None:
        _config = load_config()
    return _config


# ─── 静态文件服务 ─────────────────────────────────────────────
STATIC_DIR = Path(__file__).parent.parent / "web"


@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/assets/<path:filename>")
def static_assets(filename):
    return send_from_directory(STATIC_DIR / "assets", filename)


# ─── API: 股票分析 ─────────────────────────────────────────────
@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """分析股票 — 先从多数据源下载 → 交叉验证 → 合并 → 再规则评估"""
    try:
        data = request.get_json()
        symbol = data.get("symbol", "").strip().upper()
        data_source = data.get("data_source", "yfinance")
        book_name = data.get("book", None)

        if not symbol:
            return jsonify({"error": "请输入股票代码"}), 400

        config = get_config()

        # ============================================================
        # 阶段 1: 从多个数据源并行下载数据
        # ============================================================
        from .data_providers import get_data_provider, get_all_available_providers
        from .analyzer import StockData

        # 收集所有可用 provider 的 API key
        provider_kwargs = {}
        fmp_key = _get_api_key(config, "fmp")
        fh_key = _get_api_key(config, "finnhub")
        if fmp_key:
            provider_kwargs["fmp_api_key"] = fmp_key
        if fh_key:
            provider_kwargs["finnhub_api_key"] = fh_key

        all_providers = get_all_available_providers(**provider_kwargs)
        logger.info(f"[analyze] {symbol}: 找到 {len(all_providers)} 个可用数据源: {[p.name for p in all_providers]}")

        if not all_providers:
            return jsonify({"error": "没有可用的数据源"}), 500

        # 并行从所有数据源获取数据
        source_results = {}
        with ThreadPoolExecutor(max_workers=min(len(all_providers), 4)) as executor:
            futures = {
                executor.submit(_fetch_from_provider, dp, symbol): dp.name
                for dp in all_providers
            }
            for future in as_completed(futures):
                src_name = futures[future]
                try:
                    stock_data, quality = future.result()
                    source_results[src_name] = {
                        "stock": stock_data,
                        "quality": quality,
                        "status": "ok" if stock_data.is_valid() else "partial",
                    }
                    logger.info(f"[analyze] {src_name}: {'有效' if stock_data.is_valid() else '部分'} (核心覆盖 {quality.get('coverage', {}).get('core', {}).get('pct', 0)}%)")
                except Exception as e:
                    source_results[src_name] = {
                        "stock": None,
                        "quality": {},
                        "status": "error",
                        "error": str(e),
                    }
                    logger.warning(f"[analyze] {src_name}: 获取失败 - {e}")

        # 有效的数据源
        valid_sources = {k: v for k, v in source_results.items() if v["status"] in ("ok", "partial") and v["stock"] is not None}

        if not valid_sources:
            return jsonify({
                "error": "所有数据源均无法获取有效数据，请检查股票代码",
                "sources_tried": list(source_results.keys()),
                "source_errors": {k: v.get("error", v["status"]) for k, v in source_results.items()},
            }), 422

        # ============================================================
        # 阶段 2: 交叉验证
        # ============================================================
        cross_validation = None
        if len(valid_sources) >= 2:
            cross_validation = _cross_validate_data(valid_sources)
            logger.info(f"[analyze] 交叉验证完成: {cross_validation['summary']}")
        else:
            logger.warning(f"[analyze] 仅有 {len(valid_sources)} 个数据源可用，无法交叉验证")

        # ============================================================
        # 阶段 3: 智能合并 — 多源数据取共识值
        # ============================================================
        if len(valid_sources) >= 2:
            merged_stock = _merge_multi_source_data(symbol, valid_sources, cross_validation)
            merge_method = "multi_source_consensus"
        else:
            # 只有一个源，直接用
            src_name = list(valid_sources.keys())[0]
            merged_stock = valid_sources[src_name]["stock"]
            merge_method = f"single_source:{src_name}"

        # 再次验证合并后的数据
        if not merged_stock.is_valid():
            coverage = merged_stock.data_coverage()
            return jsonify({
                "error": f"合并后数据仍不完整 (核心覆盖率 {coverage['core']['pct']}%)，请检查股票代码",
                "data_quality": getattr(merged_stock, '_data_quality', {}),
                "sources_used": list(valid_sources.keys()),
            }), 422

        # ============================================================
        # 阶段 4: 加载规则并评估
        # ============================================================
        from .analyzer import load_knowledge_rules, evaluate_rules
        rules = load_knowledge_rules(config.storage.knowledge_dir, book_name)
        results = evaluate_rules(merged_stock, rules)

        # ============================================================
        # 阶段 5: 构建响应
        # ============================================================
        passed = [r for r in results if r.passed is True]
        failed = [r for r in results if r.passed is False]
        unknown = [r for r in results if r.passed is None]

        stock_dict = merged_stock.__dict__.copy()
        for k, v in list(stock_dict.items()):
            if k.startswith("_"):
                del stock_dict[k]
            elif v is None or v == "" or v == []:
                del stock_dict[k]

        # 数据质量汇总
        data_quality_summary = {
            "merge_method": merge_method,
            "sources_used": list(valid_sources.keys()),
            "sources_tried": list(source_results.keys()),
            "per_source": {},
        }
        for src_name, src_info in source_results.items():
            if src_info["status"] == "error":
                data_quality_summary["per_source"][src_name] = {
                    "status": "error",
                    "error": src_info.get("error", "unknown"),
                }
            else:
                data_quality_summary["per_source"][src_name] = {
                    "status": src_info["status"],
                    "coverage": src_info["quality"].get("coverage", {}),
                    "warnings": src_info["quality"].get("warnings", []),
                }

        response = {
            "stock": stock_dict,
            "data_quality": data_quality_summary,
            "rules": {
                "total": len(results),
                "passed": len(passed),
                "failed": len(failed),
                "unknown": len(unknown),
                "pass_rate": round(len(passed) / max(len(passed) + len(failed), 1) * 100, 1),
            },
            "passed_rules": [
                {
                    "description": r.description,
                    "expression": r.expression,
                    "reason": r.reason,
                    "values": r.values_used,
                }
                for r in passed
            ],
            "failed_rules": [
                {
                    "description": r.description,
                    "expression": r.expression,
                    "reason": r.reason,
                    "values": r.values_used,
                }
                for r in failed
            ],
            "unknown_rules": [
                {
                    "description": r.description,
                    "expression": r.expression,
                    "reason": r.reason,
                }
                for r in unknown[:20]
            ],
        }

        if cross_validation:
            response["cross_validation"] = cross_validation

        return jsonify(response)

    except Exception as e:
        logger.error(f"分析失败: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


def _get_api_key(config, provider_name: str) -> str:
    """从配置中安全提取 API key"""
    try:
        if hasattr(config, 'data') and hasattr(config.data, provider_name):
            return getattr(getattr(config.data, provider_name), 'api_key', '') or ''
        elif isinstance(getattr(config, 'data', None), dict):
            return config.data.get(provider_name, {}).get('api_key', '')
    except Exception:
        pass
    return ""


def _fetch_from_provider(dp, symbol: str):
    """从单个 provider 获取数据 + 验证（用于线程池）"""
    stock = dp.fetch_with_validation(symbol)
    quality = getattr(stock, '_data_quality', {})
    return stock, quality


def _cross_validate_data(valid_sources: dict) -> dict:
    """交叉验证多个数据源的数据，返回验证报告"""
    key_fields = [
        ("price", "股价", "$"),
        ("pe", "PE (TTM)", ""),
        ("forward_pe", "Forward PE", ""),
        ("pb", "PB", ""),
        ("ps", "PS", ""),
        ("eps", "EPS", "$"),
        ("roe", "ROE", "%"),
        ("revenue", "营收", "$"),
        ("net_income", "净利润", "$"),
        ("current_ratio", "流动比率", ""),
        ("debt_to_equity", "负债权益比", ""),
        ("market_cap", "市值", "$"),
        ("dividend_yield", "股息率", "%"),
        ("book_value", "每股账面值", "$"),
        ("profit_margin", "净利润率", "%"),
        ("operating_margin", "营业利润率", "%"),
        ("free_cash_flow", "自由现金流", "$"),
        ("graham_number", "Graham Number", "$"),
        ("intrinsic_value", "内在价值", "$"),
        ("margin_of_safety", "安全边际", "%"),
        ("rsi_14d", "RSI(14)", ""),
        ("ma_200d", "MA(200)", "$"),
    ]

    source_names = list(valid_sources.keys())
    fields = []

    for field_key, label, unit in key_fields:
        vals = {}
        for src_name, src_info in valid_sources.items():
            stock = src_info["stock"]
            v = getattr(stock, field_key, None)
            if v is not None:
                vals[src_name] = v

        if len(vals) >= 2:
            values = list(vals.values())
            avg = sum(values) / len(values)
            max_diff_pct = 0.0
            if avg != 0:
                max_diff_pct = max(abs(v - avg) / abs(avg) * 100 for v in values)

            if max_diff_pct < 2:
                status = "match"
            elif max_diff_pct < 10:
                status = "close"
            elif max_diff_pct < 25:
                status = "diverge"
            else:
                status = "conflict"

            fields.append({
                "key": field_key,
                "label": label,
                "unit": unit,
                "values": vals,
                "avg": avg,
                "max_diff_pct": round(max_diff_pct, 1),
                "status": status,
            })

    match_count = sum(1 for f in fields if f["status"] == "match")
    close_count = sum(1 for f in fields if f["status"] == "close")
    diverge_count = sum(1 for f in fields if f["status"] == "diverge")
    conflict_count = sum(1 for f in fields if f["status"] == "conflict")

    return {
        "fields": fields,
        "source_names": source_names,
        "stats": {
            "total": len(fields),
            "match": match_count,
            "close": close_count,
            "diverge": diverge_count,
            "conflict": conflict_count,
        },
        "summary": f"共比较 {len(fields)} 项: {match_count} 一致, {close_count} 接近, {diverge_count} 有差异, {conflict_count} 冲突",
    }


def _merge_multi_source_data(symbol: str, valid_sources: dict, cross_validation: Optional[dict]) -> 'StockData':
    """智能合并多个数据源的数据
    
    策略:
    1. 对于一致(match)或接近(close)的字段: 取各源的中位数
    2. 对于有差异(diverge)的字段: 取主源(yfinance)的值，标记警告
    3. 对于冲突(conflict)的字段: 取覆盖率最高的源的值，标记警告
    4. 对于只有一个源有的字段: 直接用该源的值
    """
    from .analyzer import StockData

    merged = StockData(symbol=symbol)

    # 确定主数据源（优先 yfinance，因为数据最全）
    primary_source = "yfinance" if "yfinance" in valid_sources else list(valid_sources.keys())[0]

    # 按覆盖率排序
    source_by_coverage = sorted(
        valid_sources.items(),
        key=lambda x: x[1]["quality"].get("coverage", {}).get("overall", {}).get("pct", 0),
        reverse=True,
    )

    # 建立冲突字段映射（从交叉验证结果）
    conflict_fields = set()
    diverge_fields = set()
    if cross_validation:
        for f in cross_validation.get("fields", []):
            if f["status"] == "conflict":
                conflict_fields.add(f["key"])
            elif f["status"] == "diverge":
                diverge_fields.add(f["key"])

    # 获取所有数值字段名
    all_fields = [f.name for f in dataclasses.fields(StockData)
                  if f.name not in ("symbol", "_data_quality") and not f.name.startswith("_")]

    merge_log = []

    for field_name in all_fields:
        # 收集所有源的值
        source_vals = {}
        for src_name, src_info in valid_sources.items():
            val = getattr(src_info["stock"], field_name, None)
            if val is not None and val != "" and val != []:
                source_vals[src_name] = val

        if not source_vals:
            continue  # 所有源都没有这个字段

        if len(source_vals) == 1:
            # 只有一个源有数据，直接用
            src_name, val = list(source_vals.items())[0]
            setattr(merged, field_name, val)
            continue

        # 多个源都有数据
        if field_name in ("name", "sector", "industry", "sp_rating", "moody_rating",
                          "sp_quality_ranking", "eps_history", "dividend_history"):
            # 字符串/列表字段: 用主源
            val = source_vals.get(primary_source) or list(source_vals.values())[0]
            setattr(merged, field_name, val)
            continue

        # 数值字段
        numeric_vals = []
        for v in source_vals.values():
            if isinstance(v, (int, float)):
                numeric_vals.append(v)

        if not numeric_vals:
            setattr(merged, field_name, list(source_vals.values())[0])
            continue

        if field_name in conflict_fields:
            # 冲突: 用覆盖率最高的源
            for src_name, _ in source_by_coverage:
                if src_name in source_vals:
                    setattr(merged, field_name, source_vals[src_name])
                    merge_log.append(f"[conflict] {field_name}: 用 {src_name} 的值 ({source_vals})")
                    break
        elif field_name in diverge_fields:
            # 有差异: 用主源
            val = source_vals.get(primary_source) or numeric_vals[0]
            setattr(merged, field_name, val)
            merge_log.append(f"[diverge] {field_name}: 用 {primary_source} 的值")
        else:
            # 一致或接近: 取中位数
            numeric_vals.sort()
            mid = len(numeric_vals) // 2
            if len(numeric_vals) % 2 == 0:
                median = (numeric_vals[mid - 1] + numeric_vals[mid]) / 2
            else:
                median = numeric_vals[mid]
            setattr(merged, field_name, median)

    # 记录合并质量信息
    merged._data_quality = {
        "merge_method": "multi_source_consensus",
        "sources": list(valid_sources.keys()),
        "primary_source": primary_source,
        "coverage": merged.data_coverage(),
        "is_valid": merged.is_valid(),
        "merge_log": merge_log[:20],  # 只记前20条
        "warnings": [],
    }

    if conflict_fields:
        merged._data_quality["warnings"].append(
            f"以下字段在数据源间有冲突: {', '.join(conflict_fields)}"
        )
    if diverge_fields:
        merged._data_quality["warnings"].append(
            f"以下字段在数据源间有差异: {', '.join(diverge_fields)}"
        )

    logger.info(f"[merge] {symbol}: 从 {list(valid_sources.keys())} 合并完成, "
                f"覆盖 {merged.data_coverage()['overall']['pct']}%, "
                f"{len(conflict_fields)} 冲突, {len(diverge_fields)} 差异")

    return merged


# ─── API: 多数据源交叉验证 ─────────────────────────────────────
@app.route("/api/cross-validate", methods=["POST"])
def api_cross_validate():
    """用多个数据源拉取同一只股票，交叉对比数据"""
    try:
        data = request.get_json()
        symbol = data.get("symbol", "").strip().upper()
        sources = data.get("sources", [])  # 用户指定的数据源列表

        if not symbol:
            return jsonify({"error": "请输入股票代码"}), 400

        config = get_config()
        from .data_providers import get_data_provider

        # 如果没指定数据源，默认 yfinance + yahoo_direct + 有key的都加上
        if not sources:
            sources = ["yfinance", "yahoo_direct"]
            fmp_key = _get_api_key(config, "fmp")
            if fmp_key:
                sources.append("fmp")
            fh_key = _get_api_key(config, "finnhub")
            if fh_key:
                sources.append("finnhub")

        results = {}
        for src in sources:
            try:
                kwargs = {}
                if src == "fmp":
                    kwargs['api_key'] = _get_api_key(config, "fmp")
                elif src == "finnhub":
                    kwargs['api_key'] = _get_api_key(config, "finnhub")

                dp = get_data_provider(src, **kwargs)
                stock = dp.fetch_with_validation(symbol)
                quality = getattr(stock, '_data_quality', {})
                stock_dict = stock.__dict__.copy()
                # 只去掉 None / 空值，保留 0
                for k, v in list(stock_dict.items()):
                    if k.startswith("_"):
                        del stock_dict[k]
                    elif v is None or v == "" or v == []:
                        del stock_dict[k]
                results[src] = {
                    "status": "ok" if stock.is_valid() else "partial",
                    "data": stock_dict,
                    "data_quality": quality,
                }
            except Exception as e:
                results[src] = {"status": "error", "error": str(e)}

        # 计算差异
        comparison = _compute_comparison(results)

        return jsonify({
            "symbol": symbol,
            "sources": results,
            "comparison": comparison,
        })

    except Exception as e:
        logger.error(f"交叉验证失败: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


def _compute_comparison(results: dict) -> dict:
    """对比多个数据源的数值差异"""
    # 关键指标列表
    key_fields = [
        ("price", "股价", "$"),
        ("pe", "PE (TTM)", ""),
        ("forward_pe", "Forward PE", ""),
        ("pb", "PB", ""),
        ("ps", "PS", ""),
        ("eps", "EPS", "$"),
        ("roe", "ROE", "%"),
        ("revenue", "营收", "$"),
        ("net_income", "净利润", "$"),
        ("current_ratio", "流动比率", ""),
        ("debt_to_equity", "负债权益比", ""),
        ("market_cap", "市值", "$"),
        ("dividend_yield", "股息率", "%"),
        ("book_value", "每股账面值", "$"),
        ("profit_margin", "净利润率", "%"),
        ("operating_margin", "营业利润率", "%"),
        ("interest_coverage_ratio", "利息覆盖率", "x"),
        ("free_cash_flow", "自由现金流", "$"),
        ("graham_number", "Graham Number", "$"),
        ("intrinsic_value", "内在价值", "$"),
        ("margin_of_safety", "安全边际", "%"),
        ("rsi_14d", "RSI(14)", ""),
        ("ma_200d", "MA(200)", "$"),
        ("earnings_growth_10y", "EPS 10Y CAGR", "%"),
        ("avg_eps_10y", "10年平均EPS", "$"),
        ("consecutive_dividend_years", "连续分红年数", "年"),
    ]

    ok_sources = {k: v["data"] for k, v in results.items() if v.get("status") in ("ok", "partial")}
    if len(ok_sources) < 2:
        return {"fields": [], "summary": "需要至少2个数据源才能交叉验证"}

    source_names = list(ok_sources.keys())
    fields = []

    for field_key, label, unit in key_fields:
        vals = {}
        for src, data in ok_sources.items():
            v = data.get(field_key)
            # None = 数据不存在；0/0.0 = 有效数据
            if v is not None:
                vals[src] = v

        if len(vals) >= 2:
            # 计算差异百分比
            values = list(vals.values())
            avg = sum(values) / len(values)
            max_diff_pct = 0.0
            if avg != 0:
                max_diff_pct = max(abs(v - avg) / abs(avg) * 100 for v in values)

            # 判断一致性
            if max_diff_pct < 2:
                status = "match"
            elif max_diff_pct < 10:
                status = "close"
            elif max_diff_pct < 25:
                status = "diverge"
            else:
                status = "conflict"

            fields.append({
                "key": field_key,
                "label": label,
                "unit": unit,
                "values": vals,
                "avg": avg,
                "max_diff_pct": round(max_diff_pct, 1),
                "status": status,
            })

    # 统计
    match_count = sum(1 for f in fields if f["status"] == "match")
    close_count = sum(1 for f in fields if f["status"] == "close")
    diverge_count = sum(1 for f in fields if f["status"] == "diverge")
    conflict_count = sum(1 for f in fields if f["status"] == "conflict")

    return {
        "fields": fields,
        "source_names": source_names,
        "stats": {
            "total": len(fields),
            "match": match_count,
            "close": close_count,
            "diverge": diverge_count,
            "conflict": conflict_count,
        },
        "data_quality": {
            src: results[src].get("data_quality", {})
            for src in source_names if src in results
        },
        "summary": f"共比较 {len(fields)} 项: {match_count} 一致, {close_count} 接近, {diverge_count} 有差异, {conflict_count} 冲突",
    }


# ─── API: 可用数据源 ───────────────────────────────────────────
@app.route("/api/data-sources")
def api_data_sources():
    """返回可用的数据源列表"""
    config = get_config()
    sources = [
        {"id": "yfinance", "name": "yfinance (免费)", "available": True},
        {"id": "yahoo_direct", "name": "Yahoo Direct HTTP (免费/交叉验证)", "available": True},
    ]
    # 检查 FMP
    fmp_key = _get_api_key(config, "fmp")
    sources.append({
        "id": "fmp",
        "name": "FMP (Financial Modeling Prep)",
        "available": bool(fmp_key),
        "note": "" if fmp_key else "需在 config.yaml 配置 api_key",
    })
    # 检查 Finnhub
    fh_key = _get_api_key(config, "finnhub")
    sources.append({
        "id": "finnhub",
        "name": "Finnhub (60次/分钟免费)",
        "available": bool(fh_key),
        "note": "" if fh_key else "需在 config.yaml 配置 api_key",
    })
    sources.append({"id": "bloomberg", "name": "Bloomberg Terminal", "available": False, "note": "需安装 blpapi"})
    sources.append({"id": "auto", "name": "自动选择", "available": True})
    return jsonify({"sources": sources})


# ─── API: LLM 深度分析 ─────────────────────────────────────────
@app.route("/api/llm-analysis", methods=["POST"])
def api_llm_analysis():
    """用 LLM 生成深度分析报告"""
    try:
        data = request.get_json()
        stock_data = data.get("stock", {})
        passed_rules = data.get("passed_rules", [])
        failed_rules = data.get("failed_rules", [])

        config = get_config()
        from .llm import get_llm_provider
        llm_config = config.llm.model_dump()
        llm_provider = get_llm_provider(config.llm.default_provider, llm_config)

        name = stock_data.get("name", "")
        symbol = stock_data.get("symbol", "")

        prompt = f"""你是一位资深投资分析师。请基于以下数据和规则评估结果，为 {name} ({symbol}) 生成一份简洁的投资分析报告。

## 股票基础数据
- 行业: {stock_data.get('sector', 'N/A')} / {stock_data.get('industry', 'N/A')}
- 股价: ${stock_data.get('price', 0):.2f}, 市值: ${stock_data.get('market_cap', 0)/1e9:.1f}B
- PE: {stock_data.get('pe', 0):.1f}, Forward PE: {stock_data.get('forward_pe', 0):.1f}, PB: {stock_data.get('pb', 0):.1f}
- ROE: {stock_data.get('roe', 0)*100:.1f}%, EPS: ${stock_data.get('eps', 0):.2f}
- 负债权益比: {stock_data.get('debt_to_equity', 0):.2f}, 流动比率: {stock_data.get('current_ratio', 0):.2f}
- 股息率: {stock_data.get('dividend_yield', 0):.2f}%

## 规则评估结果
- 通过 {len(passed_rules)} 条，不通过 {len(failed_rules)} 条

### 通过的规则 (前10条):
{chr(10).join(f"- [{r.get('expression', '')}] {r.get('description', '')[:60]}" for r in passed_rules[:10])}

### 不通过的规则 (前10条):
{chr(10).join(f"- [{r.get('expression', '')}] {r.get('description', '')[:60]}" for r in failed_rules[:10])}

请用中文输出，包含：
1. 一句话总结（买入/持有/观望/回避）
2. 核心优势（基于通过的规则）
3. 主要风险（基于不通过的规则）
4. 从价值投资角度的建议
"""
        analysis = llm_provider.chat(messages=[{"role": "user", "content": prompt}])
        return jsonify({"analysis": analysis})

    except Exception as e:
        logger.error(f"LLM 分析失败: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


# ─── API: 书籍列表 ─────────────────────────────────────────────
@app.route("/api/books")
def api_books():
    """获取已导入书籍列表"""
    try:
        config = get_config()
        books = []

        # 从知识库目录获取书籍信息
        knowledge_dir = Path(config.storage.knowledge_dir)
        if knowledge_dir.exists():
            for d in sorted(knowledge_dir.iterdir()):
                if d.is_dir():
                    kfile = d / "knowledge.json"
                    info = {"name": d.name, "has_knowledge": kfile.exists(), "rule_count": 0}
                    if kfile.exists():
                        try:
                            kdata = json.loads(kfile.read_text(encoding="utf-8"))
                            info["rule_count"] = len(kdata.get("rules", []))
                            info["indicator_count"] = len(kdata.get("indicators", []))
                            info["summary"] = kdata.get("summary", "")[:200]
                        except Exception:
                            pass
                    books.append(info)

        return jsonify({"books": books})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── API: 语义搜索 ─────────────────────────────────────────────
@app.route("/api/search", methods=["POST"])
def api_search():
    """语义检索"""
    try:
        data = request.get_json()
        query = data.get("query", "").strip()
        book = data.get("book", None)
        top_k = data.get("top_k", 5)

        if not query:
            return jsonify({"error": "请输入搜索内容"}), 400

        config = get_config()
        from .search import KnowledgeSearcher
        from .llm import get_llm_provider

        llm_provider = None
        try:
            llm_config = config.llm.model_dump()
            llm_provider = get_llm_provider(config.llm.default_provider, llm_config)
        except Exception:
            pass

        searcher = KnowledgeSearcher(config.storage.vectordb_dir, llm_provider)
        books = [book] if book else searcher.list_books()

        all_results = []
        for b in books:
            results = searcher.search(query, b, top_k=top_k)
            all_results.extend(results)

        all_results.sort(key=lambda r: r.score, reverse=True)
        all_results = all_results[:top_k]

        return jsonify({
            "results": [
                {
                    "content": r.content,
                    "chapter": r.chapter_title,
                    "book": r.book_title,
                    "score": round(r.score, 3),
                }
                for r in all_results
            ]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── API: 知识库规则详情 ──────────────────────────────────────
@app.route("/api/rules/<book_name>")
def api_rules(book_name):
    """获取某本书的规则详情"""
    try:
        config = get_config()
        kfile = Path(config.storage.knowledge_dir) / book_name / "knowledge.json"
        if not kfile.exists():
            return jsonify({"error": "书籍知识未找到"}), 404

        kdata = json.loads(kfile.read_text(encoding="utf-8"))
        return jsonify(kdata)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def main():
    """启动 Web UI"""
    import argparse
    parser = argparse.ArgumentParser(description="Book KB Web UI")
    parser.add_argument("--host", default="127.0.0.1", help="Host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5000, help="Port (default: 5000)")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    args = parser.parse_args()

    print(f"\n  [*] Investment Analysis Panel")
    print(f"  http://{args.host}:{args.port}\n")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
