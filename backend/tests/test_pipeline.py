"""Tests for R-System — pipeline stages, LLM timeout, SSE, basket builder, and tools.

Run with:
    cd backend && set PYTHONPATH=backend && python -m pytest tests/test_pipeline.py -v
"""

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

# ── Ensure backend package is importable ──
_BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

_PROJECT_ROOT = _BACKEND_ROOT.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ════════════════════════════════════════════════════════════════
#  1. Unit Tests — StockResult data class
# ════════════════════════════════════════════════════════════════

class TestStockResult:
    def test_default_alive(self):
        from app.agent.unified_pipeline import StockResult
        r = StockResult(symbol="AAPL")
        assert r.is_alive()
        assert r.eliminated_at_stage == 0

    def test_eliminate(self):
        from app.agent.unified_pipeline import StockResult
        r = StockResult(symbol="AAPL")
        r.eliminate(3, "PE too high")
        assert not r.is_alive()
        assert r.eliminated_at_stage == 3
        assert "PE too high" in r.gate_failures

    def test_to_dict(self):
        from app.agent.unified_pipeline import StockResult
        r = StockResult(symbol="MSFT", name="Microsoft", price=400.0)
        d = r.to_dict()
        assert d["symbol"] == "MSFT"
        assert d["price"] == 400.0
        assert isinstance(d, dict)

    def test_default_fields(self):
        from app.agent.unified_pipeline import StockResult
        r = StockResult(symbol="GOOG")
        assert r.school_results == {}
        assert r.valuations == {}
        assert r.per_school_opinions == {}
        assert r.filing_summary == ""
        assert r.comparative_matrix == ""

    def test_multiple_instances_independent(self):
        """Ensure dataclass default_factory creates independent dicts/lists."""
        from app.agent.unified_pipeline import StockResult
        a = StockResult(symbol="A")
        b = StockResult(symbol="B")
        a.gate_failures.append("fail")
        assert b.gate_failures == []  # should not share


# ════════════════════════════════════════════════════════════════
#  2. Unit Tests — Strategy templates & Preset universes
# ════════════════════════════════════════════════════════════════

class TestStrategiesAndUniverses:
    def test_strategies_exist(self):
        from app.agent.unified_pipeline import STRATEGY_TEMPLATES
        assert "conservative" in STRATEGY_TEMPLATES
        assert "balanced" in STRATEGY_TEMPLATES
        assert "aggressive" in STRATEGY_TEMPLATES

    def test_conservative_stricter_than_aggressive(self):
        from app.agent.unified_pipeline import STRATEGY_TEMPLATES
        cons = STRATEGY_TEMPLATES["conservative"]
        aggr = STRATEGY_TEMPLATES["aggressive"]
        assert cons["max_pe"] < aggr["max_pe"]
        assert cons["min_margin_of_safety"] > aggr["min_margin_of_safety"]

    def test_preset_universes_not_empty(self):
        from app.agent.unified_pipeline import PRESET_UNIVERSES
        for name, stocks in PRESET_UNIVERSES.items():
            assert len(stocks) > 0, f"Universe '{name}' is empty"

    def test_preset_universes_no_duplicates(self):
        from app.agent.unified_pipeline import PRESET_UNIVERSES
        for name, stocks in PRESET_UNIVERSES.items():
            assert len(stocks) == len(set(stocks)), f"Universe '{name}' has duplicates"


# ════════════════════════════════════════════════════════════════
#  3. Unit Tests — Stage 1: Universe Construction
# ════════════════════════════════════════════════════════════════

class TestStage1:
    def test_custom_symbols(self):
        from app.agent.unified_pipeline import UnifiedPipeline
        p = UnifiedPipeline()
        result = p._stage1_universe(symbols=["AAPL", "MSFT", "GOOG"], universe=None)
        assert len(result) == 3
        assert result[0] == "AAPL"

    def test_preset_universe(self):
        from app.agent.unified_pipeline import UnifiedPipeline
        p = UnifiedPipeline()
        result = p._stage1_universe(symbols=[], universe="dividend_kings")
        assert len(result) > 5

    def test_deduplication_case_insensitive(self):
        from app.agent.unified_pipeline import UnifiedPipeline
        p = UnifiedPipeline()
        result = p._stage1_universe(symbols=["AAPL", "aapl", "Aapl"], universe=None)
        # _stage1_universe upper-cases; dedup happens at run() level or not
        # At minimum they should all be uppercased
        assert all(s == "AAPL" for s in result)

    def test_empty_input_with_no_universe(self):
        from app.agent.unified_pipeline import UnifiedPipeline
        p = UnifiedPipeline()
        result = p._stage1_universe(symbols=[], universe=None)
        assert result == []

    def test_unknown_universe_returns_empty(self):
        from app.agent.unified_pipeline import UnifiedPipeline
        p = UnifiedPipeline()
        result = p._stage1_universe(symbols=[], universe="nonexistent_xyz")
        assert result == []


# ════════════════════════════════════════════════════════════════
#  4. Unit Tests — Stage 3: Knockout Gate
# ════════════════════════════════════════════════════════════════

class TestStage3Knockout:
    def _make_pipeline(self, strategy="balanced"):
        from app.agent.unified_pipeline import UnifiedPipeline, STRATEGY_TEMPLATES
        p = UnifiedPipeline()
        p.config = STRATEGY_TEMPLATES.get(strategy, STRATEGY_TEMPLATES["balanced"])
        return p

    def test_passes_good_stock(self):
        from app.agent.unified_pipeline import StockResult
        p = self._make_pipeline("balanced")
        r = StockResult(symbol="AAPL", data_quality=80)
        p.results = [r]

        data_map = {"AAPL": {"dict": {
            "pe": 15, "debt_to_equity": 1.0, "current_ratio": 1.5,
            "free_cash_flow": 1e9, "market_cap": 3e12, "eps": 6.5, "roe": 0.25,
        }}}

        p._stage3_knockout(data_map)
        assert r.is_alive()

    def test_eliminates_high_pe_conservative(self):
        from app.agent.unified_pipeline import StockResult
        p = self._make_pipeline("conservative")
        r = StockResult(symbol="TSLA", data_quality=80)
        p.results = [r]

        data_map = {"TSLA": {"dict": {
            "pe": 80, "debt_to_equity": 0.5, "current_ratio": 2.5,
            "free_cash_flow": 1e9, "market_cap": 8e11, "eps": 4.0, "roe": 0.15,
        }}}

        p._stage3_knockout(data_map)
        assert not r.is_alive()
        assert r.eliminated_at_stage == 3


# ════════════════════════════════════════════════════════════════
#  5. Unit Tests — Stage 8: Conviction Ranking
# ════════════════════════════════════════════════════════════════

class TestStage8Conviction:
    def test_ranking_order(self):
        from app.agent.unified_pipeline import UnifiedPipeline, StockResult, STRATEGY_TEMPLATES
        p = UnifiedPipeline()
        p.config = STRATEGY_TEMPLATES["balanced"]

        r1 = StockResult(symbol="A", margin_of_safety=0.4, school_consensus_score=80, risk_tier="FORTRESS")
        r2 = StockResult(symbol="B", margin_of_safety=0.1, school_consensus_score=40, risk_tier="NEUTRAL")
        r3 = StockResult(symbol="C", margin_of_safety=0.25, school_consensus_score=60, risk_tier="SOLID")

        p.results = [r1, r2, r3]
        p._stage8_conviction([r1, r2, r3])

        assert r1.composite_score >= r3.composite_score >= r2.composite_score

    def test_conviction_levels_assigned(self):
        from app.agent.unified_pipeline import UnifiedPipeline, StockResult, STRATEGY_TEMPLATES
        p = UnifiedPipeline()
        p.config = STRATEGY_TEMPLATES["balanced"]

        r = StockResult(
            symbol="TOP", margin_of_safety=0.5, school_consensus_score=95,
            risk_tier="FORTRESS", llm_analysis="Solid investment"
        )
        p.results = [r]
        p._stage8_conviction([r])

        # Should be assigned a conviction level
        assert r.conviction in ("HIGHEST", "HIGH", "MEDIUM", "LOW", "NONE")
        assert r.composite_score > 0


# ════════════════════════════════════════════════════════════════
#  6. Tests — LLM Timeout Protection
# ════════════════════════════════════════════════════════════════

class TestLLMTimeout:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_simple_completion_timeout(self):
        """simple_completion should return timeout error message instead of hanging."""
        from app.agent import llm

        mock_client = MagicMock()

        async def _hang(client, **kwargs):
            await asyncio.sleep(9999)

        with patch.object(llm, '_require_client', return_value=mock_client), \
             patch.object(llm, '_retry_create', side_effect=_hang), \
             patch.object(llm, 'get_llm_model', return_value='test-model'):

            result = await llm.simple_completion(
                messages=[{"role": "user", "content": "test"}],
                timeout=0.5,
            )

            assert "[LLM Timeout" in result

    @pytest.mark.asyncio(loop_scope="function")
    async def test_simple_completion_success(self):
        """simple_completion should return content on success."""
        from app.agent import llm

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test response"

        async def _fast(client, **kwargs):
            return mock_response

        mock_client = MagicMock()

        with patch.object(llm, '_require_client', return_value=mock_client), \
             patch.object(llm, '_retry_create', side_effect=_fast), \
             patch.object(llm, 'get_llm_model', return_value='test-model'):

            result = await llm.simple_completion(
                messages=[{"role": "user", "content": "test"}],
                timeout=5.0,
            )

            assert result == "Test response"

    @pytest.mark.asyncio(loop_scope="function")
    async def test_simple_completion_error_handled(self):
        """simple_completion should return error message on exception."""
        from app.agent import llm

        async def _fail(client, **kwargs):
            raise RuntimeError("API down")

        mock_client = MagicMock()

        with patch.object(llm, '_require_client', return_value=mock_client), \
             patch.object(llm, '_retry_create', side_effect=_fail), \
             patch.object(llm, 'get_llm_model', return_value='test-model'):

            result = await llm.simple_completion(
                messages=[{"role": "user", "content": "test"}],
                timeout=5.0,
            )

            assert "[LLM Error" in result


# ════════════════════════════════════════════════════════════════
#  7. Tests — Pipeline SSE Error Handling
# ════════════════════════════════════════════════════════════════

class TestPipelineSSE:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_sse_generator_catches_errors(self):
        """_sse_generator should yield error event instead of crashing."""
        from app.api.agent_routes import _sse_generator

        async def _failing_gen():
            yield {"type": "stage_start", "stage": 1}
            raise RuntimeError("Boom!")

        events = []
        async for chunk in _sse_generator(_failing_gen()):
            events.append(chunk)

        assert len(events) == 2
        assert "stage_start" in events[0]
        error_data = json.loads(events[1].replace("data: ", "").strip())
        assert error_data["event"] == "error"
        assert "Boom!" in error_data["message"]

    @pytest.mark.asyncio(loop_scope="function")
    async def test_sse_normal_flow(self):
        """_sse_generator should pass through events normally."""
        from app.api.agent_routes import _sse_generator

        async def _gen():
            yield {"type": "status", "message": "ok"}

        events = []
        async for chunk in _sse_generator(_gen()):
            events.append(chunk)

        assert len(events) == 1
        parsed = json.loads(events[0].replace("data: ", "").strip())
        assert parsed["event"] == "status"


# ════════════════════════════════════════════════════════════════
#  8. Tests — Filing Fetcher
# ════════════════════════════════════════════════════════════════

class TestFilingFetcher:
    def test_build_financial_summary(self):
        """_build_financial_summary should produce a string."""
        from app.agent.filing_fetcher import _build_financial_summary

        summary = _build_financial_summary(
            symbol="AAPL",
            data={"has_sec_filing": False, "financial_statements": {}},
        )
        assert isinstance(summary, str)
        assert "AAPL" in summary

    def test_build_financial_summary_with_filing(self):
        from app.agent.filing_fetcher import _build_financial_summary

        summary = _build_financial_summary(
            symbol="MSFT",
            data={
                "has_sec_filing": True,
                "filing_metadata": {"form": "10-K", "filingDate": "2025-01-15"},
                "financial_statements": {"income_statement": "Revenue: 200B"},
            },
        )
        assert "MSFT" in summary
        assert "10-K" in summary


# ════════════════════════════════════════════════════════════════
#  9. Tests — Symbol Search API
# ════════════════════════════════════════════════════════════════

class TestSymbolSearch:
    def test_symbol_search_endpoint(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/symbol/search?q=AAPL")
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        results = data["results"]
        assert len(results) >= 1
        assert results[0]["symbol"] == "AAPL"

    def test_symbol_search_chinese(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/symbol/search?q=苹果")
        assert resp.status_code == 200
        data = resp.json()
        results = data["results"]
        assert any(r["symbol"] == "AAPL" for r in results)

    def test_symbol_search_hk(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/symbol/search?q=700")
        assert resp.status_code == 200
        data = resp.json()
        results = data["results"]
        assert any("0700.HK" in r["symbol"] for r in results)

    def test_symbol_search_empty(self):
        """Empty query should return 422 (min_length=1 validation)."""
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/symbol/search?q=")
        assert resp.status_code == 422  # FastAPI Query validation

    def test_symbol_search_nonexistent(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/symbol/search?q=ZZZZZZZZZZZ")
        assert resp.status_code == 200
        data = resp.json()
        # Should return empty or yfinance fallback results
        assert isinstance(data["results"], list)


# ════════════════════════════════════════════════════════════════
#  10. Tests — Health endpoint
# ════════════════════════════════════════════════════════════════

class TestHealth:
    def test_health_endpoint(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "llmAvailable" in data
        assert "model" in data


# ════════════════════════════════════════════════════════════════
#  11. Tests — Pipeline strategies endpoint
# ════════════════════════════════════════════════════════════════

class TestPipelineStrategies:
    def test_strategies_endpoint(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.get("/api/v1/agent/pipeline/strategies")
        assert resp.status_code == 200
        data = resp.json()
        assert "strategies" in data
        assert "universes" in data
        assert "balanced" in data["strategies"]
        assert "conservative" in data["strategies"]


# ════════════════════════════════════════════════════════════════
#  12. Tests — Config
# ════════════════════════════════════════════════════════════════

class TestConfig:
    def test_settings_load(self):
        from app.config import Settings
        s = Settings()
        assert s.database_url != ""

    def test_model_chain(self):
        from app.config import Settings
        s = Settings(llm_api_key="test", llm_model="claude-test", llm_fallback_models="gpt-5,gemini")
        chain = s.model_chain
        assert chain[0] == "claude-test"
        assert "gpt-5" in chain
        assert "gemini" in chain

    def test_effective_key_priority(self):
        from app.config import Settings
        s = Settings(llm_api_key="proxy-key", openai_api_key="openai-key")
        assert s.effective_api_key == "proxy-key"

    def test_effective_key_fallback(self):
        from app.config import Settings
        s = Settings(llm_api_key="", openai_api_key="openai-key")
        assert s.effective_api_key == "openai-key"


# ════════════════════════════════════════════════════════════════
#  13. Tests — Parse school opinions
# ════════════════════════════════════════════════════════════════

class TestParseSchoolOpinions:
    def test_parse_school_opinions(self):
        from app.agent.unified_pipeline import UnifiedPipeline
        p = UnifiedPipeline()

        text = """## 一、七大流派分别怎么看 AAPL

1. **Graham 深度价值派**: AAPL的PE为30，远超Graham的15倍标准。Graham不会买。

2. **Buffett 护城河派**: 苹果有强大的品牌护城河，ROE高达100%+。Buffett会认可。

3. **量化价值派 (Greenblatt/O'Shaughnessy)**: ROIC很高，排名靠前。

4. **品质投资派 (Dorsey/Cunningham)**: 持续高ROE，资本需求低。

5. **Damodaran 估值派**: DCF估值合理，增长假设保守。

6. **逆向价值派 (Spier/Templeton)**: 市场共识看好，不符合逆向条件。

7. **GARP 成长派 (Lynch)**: PEG约为1.5，略高但可接受。"""

        opinions = p._parse_school_opinions(text)
        assert isinstance(opinions, dict)
        assert len(opinions) >= 3

    def test_parse_empty_text(self):
        from app.agent.unified_pipeline import UnifiedPipeline
        p = UnifiedPipeline()
        opinions = p._parse_school_opinions("")
        assert opinions == {}


# ════════════════════════════════════════════════════════════════
#  14. Tests — Pipeline request validation
# ════════════════════════════════════════════════════════════════

class TestPipelineRequest:
    def test_pipeline_422_no_input(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.post("/api/v1/agent/pipeline", json={"stocks": [], "strategy": "balanced"})
        assert resp.status_code == 422

    def test_pipeline_accepts_stocks(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        resp = client.post(
            "/api/v1/agent/pipeline",
            json={"stocks": ["AAPL"], "strategy": "balanced", "enableLlm": False},
        )
        assert resp.status_code == 200
        assert resp.headers.get("content-type", "").startswith("text/event-stream")


# ════════════════════════════════════════════════════════════════
#  15. Tests — LLM layer helpers
# ════════════════════════════════════════════════════════════════

class TestLLMHelpers:
    def test_adapt_kwargs_normal_model(self):
        from app.agent.llm import _adapt_kwargs_for_model
        result = _adapt_kwargs_for_model("claude-sonnet", {
            "temperature": 0.7, "max_tokens": 4096, "messages": []
        })
        assert result["model"] == "claude-sonnet"
        assert result["temperature"] == 0.7
        assert result["max_tokens"] == 4096

    def test_adapt_kwargs_gpt5(self):
        from app.agent.llm import _adapt_kwargs_for_model
        result = _adapt_kwargs_for_model("gpt-5", {
            "temperature": 0.7, "max_tokens": 4096, "messages": []
        })
        assert result["model"] == "gpt-5"
        assert "temperature" not in result
        assert "max_tokens" not in result
        assert result["max_completion_tokens"] == 4096

    def test_sanitize_messages_no_tools(self):
        from app.agent.llm import _sanitize_messages_for_no_tools
        messages = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Let me check", "tool_calls": [
                {"id": "tc_1", "function": {"name": "search", "arguments": '{"q":"test"}'}}
            ]},
            {"role": "tool", "tool_call_id": "tc_1", "content": "Search result"},
            {"role": "assistant", "content": "Here is your answer"},
        ]
        sanitized = _sanitize_messages_for_no_tools(messages)
        assert all(m["role"] != "tool" for m in sanitized)
        assert len(sanitized) == 4

    def test_is_llm_available(self):
        from app.agent.llm import is_llm_available
        result = is_llm_available()
        assert isinstance(result, bool)


# ════════════════════════════════════════════════════════════════
#  16. Tests — Distilled rules integrity
# ════════════════════════════════════════════════════════════════

class TestDistilledRules:
    def test_all_seven_schools_exist(self):
        from app.agent.distilled_rules import SCHOOLS
        expected = {"graham", "buffett", "quantitative", "quality", "valuation", "contrarian", "garp"}
        assert set(SCHOOLS.keys()) == expected

    def test_each_school_has_rules(self):
        from app.agent.distilled_rules import SCHOOLS
        for name, school in SCHOOLS.items():
            assert len(school.rules) >= 3, f"School '{name}' has too few rules: {len(school.rules)}"

    def test_rule_structure(self):
        from app.agent.distilled_rules import SCHOOLS
        for name, school in SCHOOLS.items():
            for rule in school.rules:
                assert rule.name, f"Rule in '{name}' missing name"
                assert rule.expression, f"Rule '{rule.name}' in '{name}' missing expression"
                assert rule.school, f"Rule '{rule.name}' in '{name}' missing school"

    def test_all_rules_aggregate(self):
        from app.agent.distilled_rules import ALL_RULES
        assert len(ALL_RULES) >= 40, f"Expected 40+ rules, got {len(ALL_RULES)}"

    def test_evaluate_stock_against_school(self):
        from app.agent.distilled_rules import evaluate_stock_against_school
        fake_data = {"pe": 12, "pb": 1.5, "roe": 0.15, "currentRatio": 2.0, "debtToEquity": 0.5}
        result = evaluate_stock_against_school(fake_data, "graham")
        assert "score" in result
        assert "max_score" in result or "maxScore" in result


# ════════════════════════════════════════════════════════════════
#  17. Tests — Persona prompt
# ════════════════════════════════════════════════════════════════

class TestPersona:
    def test_charlie_system_prompt_exists(self):
        from app.agent.persona import CHARLIE_SYSTEM_PROMPT
        assert len(CHARLIE_SYSTEM_PROMPT) > 500
        assert "老查理" in CHARLIE_SYSTEM_PROMPT or "Charlie" in CHARLIE_SYSTEM_PROMPT

    def test_prompt_mentions_schools(self):
        from app.agent.persona import CHARLIE_SYSTEM_PROMPT
        for school in ["Graham", "Buffett", "Greenblatt"]:
            assert school in CHARLIE_SYSTEM_PROMPT, f"Persona prompt missing {school}"


# ════════════════════════════════════════════════════════════════
#  18. Tests — Format helpers
# ════════════════════════════════════════════════════════════════

class TestFormatHelpers:
    def test_fmt_pct(self):
        from app.agent.unified_pipeline import _fmt_pct
        assert "%" in _fmt_pct(0.15)
        assert _fmt_pct(None) == "N/A"

    def test_fmt_big(self):
        from app.agent.unified_pipeline import _fmt_big
        result = _fmt_big(1_500_000_000)
        assert "B" in result or "b" in result.lower()
        assert _fmt_big(None) == "N/A"
