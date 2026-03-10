"""LLM layer — Chat completions with tool-calling support.

Supports:
- 通过 fit-ai 代理访问 Claude/Gemini/GPT-5
- 直连 OpenAI API (备用)
- Streaming responses for real-time UI updates
- 模型降级链：主模型失败时自动切换到备选模型

模型优先级（可在 .env 中配置）:
1. claude-sonnet-4-5-20250929 (首选：推理强、tool calling 兼容好)
2. gemini-3.1-pro (备选：Agentic 场景强)
3. gpt-5 (备选：原生 OpenAI 格式，零兼容问题)
4. gemini-3-flash (兜底：快速、低成本)

容错机制:
- 每个模型 HTTP 400/500 自动重试(最多2次)
- 模型失败后自动降级到下一个模型
- tools 调用失败时 fallback 到无 tools 模式
"""

import asyncio
import json
import logging
import time
from typing import List, Dict, Any, Optional, AsyncGenerator

from openai import AsyncOpenAI, APIError, APIStatusError
from app.config import get_settings

logger = logging.getLogger(__name__)

# Lazy-init client
_client: Optional[AsyncOpenAI] = None

# Track which model is currently active (for logging/diagnostics)
_active_model: Optional[str] = None

# GPT-5 specific: doesn't support temperature/top_p, uses max_completion_tokens
_GPT5_MODELS = {"gpt-5", "gpt-5-mini", "gpt-5-nano"}

# ── Rate limiter: 防止并发请求风暴触发 API 频率限制 ──
# Semaphore 控制最大并发数，_last_request_time 保证请求间隔
_llm_semaphore: Optional[asyncio.Semaphore] = None
_last_request_time: float = 0.0
_request_lock: Optional[asyncio.Lock] = None

# 可通过环境变量或代码配置
LLM_MAX_CONCURRENCY = 3       # 最大同时并发 LLM 请求数
LLM_MIN_INTERVAL_MS = 200     # 两次请求之间最小间隔（毫秒）


def _get_semaphore() -> asyncio.Semaphore:
    """Lazy init semaphore (必须在 event loop 内创建)."""
    global _llm_semaphore
    if _llm_semaphore is None:
        _llm_semaphore = asyncio.Semaphore(LLM_MAX_CONCURRENCY)
    return _llm_semaphore


def _get_request_lock() -> asyncio.Lock:
    """Lazy init lock."""
    global _request_lock
    if _request_lock is None:
        _request_lock = asyncio.Lock()
    return _request_lock


async def _rate_limited_wait():
    """Ensure minimum interval between LLM requests to avoid rate limiting."""
    global _last_request_time
    lock = _get_request_lock()
    async with lock:
        now = time.monotonic()
        elapsed_ms = (now - _last_request_time) * 1000
        if elapsed_ms < LLM_MIN_INTERVAL_MS:
            wait_s = (LLM_MIN_INTERVAL_MS - elapsed_ms) / 1000
            await asyncio.sleep(wait_s)
        _last_request_time = time.monotonic()


def _get_client() -> Optional[AsyncOpenAI]:
    """Get or create the async client (代理优先，OpenAI 备用)."""
    global _client
    if _client is not None:
        return _client
    settings = get_settings()

    api_key = settings.effective_api_key
    base_url = settings.effective_base_url
    if not api_key:
        return None

    kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
        logger.info(f"LLM client: proxy mode → {base_url}")
    else:
        logger.info("LLM client: direct OpenAI mode")

    # Set explicit timeout: 120s total, 10s connect
    import httpx
    kwargs["timeout"] = httpx.Timeout(120.0, connect=10.0)

    _client = AsyncOpenAI(**kwargs)
    return _client


def is_llm_available() -> bool:
    """Check if any LLM API key is configured."""
    settings = get_settings()
    return bool(settings.effective_api_key)


def get_llm_model() -> str:
    """Get the currently active LLM model name."""
    global _active_model
    if _active_model:
        return _active_model
    settings = get_settings()
    return settings.effective_model


def get_model_chain() -> List[str]:
    """Get the full model fallback chain."""
    settings = get_settings()
    return settings.model_chain


def _adapt_kwargs_for_model(model: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Adapt API kwargs based on model-specific requirements.

    GPT-5 系列不支持 temperature/top_p。
    注意：代理 API (fit-ai proxy) 不一定支持 max_completion_tokens，
    所以我们只移除不兼容的参数，不添加新参数。
    """
    adapted = dict(kwargs)
    adapted["model"] = model

    if model in _GPT5_MODELS:
        # GPT-5: remove unsupported params
        adapted.pop("temperature", None)
        adapted.pop("top_p", None)
        # 代理 API 统一用 max_tokens；如果需要直连 OpenAI GPT-5 再改回 max_completion_tokens
    return adapted


def _require_client() -> AsyncOpenAI:
    """Get the client or raise a clear configuration error."""
    client = _get_client()
    if client is None:
        raise RuntimeError(
            "LLM 未配置。请在 backend/.env 文件中设置:\n"
            "  LLM_API_KEY=your-key\n"
            "  LLM_BASE_URL=http://your-proxy-url  (可选)\n"
            "或者:\n"
            "  OPENAI_API_KEY=sk-your-key\n"
            "然后重启后端服务。"
        )
    return client


# ─── Tool Definitions (OpenAI function-calling format) ───

TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "search_investment_books",
            "description": (
                "Search the investment book knowledge base for relevant principles, rules, "
                "valuation methods, red flags, and case studies from 14 classic investing books. "
                "Use this whenever you need to cite book-based investment wisdom or look up "
                "specific concepts like margin of safety, owner earnings, moat analysis, etc."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query — describe what investment concept, rule, or principle you're looking for",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_fundamentals",
            "description": (
                "Fetch comprehensive fundamental data for a stock: PE, PB, ROE, "
                "debt/equity, free cash flow, revenue growth, margins, and 60+ metrics."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g. AAPL, MSFT, BRK-B)",
                    }
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_valuation_analysis",
            "description": (
                "Run a full value investing analysis on a stock: 7 valuation models "
                "(Graham Number, EPV, DCF, DDM, Net-Net, Owner Earnings, PEG), "
                "moat assessment, and margin of safety calculation."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    }
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "detect_financial_shenanigans",
            "description": (
                "Run Schilit's 7-category fraud detection on a stock's financials. "
                "Includes Beneish M-Score (earnings manipulation), Altman Z-Score "
                "(bankruptcy risk), and Piotroski F-Score (financial strength). "
                "Use this to check for accounting red flags."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    }
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_news",
            "description": (
                "Fetch recent news and sentiment analysis for a stock or topic. "
                "Returns news articles with sentiment scores."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Stock symbol or topic to search news for",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of news articles to fetch (default 5)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_price_history",
            "description": (
                "Get historical price data for a stock including open, high, low, "
                "close, and volume. Useful for technical context."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of days of history (default 365)",
                        "default": 365,
                    },
                },
                "required": ["symbol"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_topic",
            "description": (
                "Use Perplexity AI for real-time research on any investment topic. "
                "Good for recent events, industry trends, competitive landscape, "
                "or any question that needs current information beyond the book knowledge."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The research question or topic",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "evaluate_stock_rules",
            "description": (
                "⭐ CORE TOOL — Evaluate a stock against 65+ quantitative investment rules "
                "distilled from 14 classic value investing books, organized into 7 investment schools: "
                "Graham Deep Value, Buffett Quality Moat, Quantitative Value, Quality Investing, "
                "Damodaran Valuation, Contrarian Value, and GARP. "
                "Returns detailed pass/fail for each school with scores and recommendations. "
                "USE THIS TOOL whenever analyzing any stock — it is your signature capability."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g. AAPL, MSFT, BRK-B)",
                    },
                    "school": {
                        "type": "string",
                        "description": (
                            "Which investment school to evaluate against. "
                            "Options: 'all' (default, all 7 schools), 'graham', 'buffett', "
                            "'quantitative', 'quality', 'valuation', 'contrarian', 'garp'"
                        ),
                        "default": "all",
                    },
                },
                "required": ["symbol"],
            },
        },
    },
]


# ─── Tool Execution ───

async def execute_tool(name: str, arguments: Dict[str, Any]) -> str:
    """Execute a tool by name and return the result as a string."""
    from app.agent.tools import (
        scan_fundamentals, scan_history, detect_shenanigans,
        run_full_valuation, search_knowledge, analyze_news,
        research_realtime, evaluate_stock_rules,
    )

    try:
        if name == "search_investment_books":
            return await search_knowledge(arguments["query"])
        elif name == "get_stock_fundamentals":
            return await scan_fundamentals(arguments["symbol"])
        elif name == "run_valuation_analysis":
            result = await run_full_valuation(arguments["symbol"])
            return json.dumps(result, default=str)[:3000]
        elif name == "detect_financial_shenanigans":
            result = await detect_shenanigans(arguments["symbol"])
            return json.dumps(result, default=str)[:3000]
        elif name == "get_stock_news":
            return await analyze_news(
                arguments["query"],
                limit=arguments.get("limit", 5),
            )
        elif name == "get_price_history":
            return await scan_history(
                arguments["symbol"],
                days=arguments.get("days", 365),
            )
        elif name == "research_topic":
            return await research_realtime(arguments["query"])
        elif name == "evaluate_stock_rules":
            return await evaluate_stock_rules(
                arguments["symbol"],
                school=arguments.get("school", "all"),
            )
        else:
            return f"Unknown tool: {name}"
    except Exception as e:
        logger.error(f"Tool execution error ({name}): {e}")
        return f"Tool error ({name}): {str(e)[:500]}"


# ─── Retry Helper ───

def _sanitize_messages_for_no_tools(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert messages containing tool calls/results to plain messages.

    When falling back to no-tools mode, we need to remove tool-specific
    message fields that the proxy doesn't understand.
    """
    sanitized = []
    tool_results = {}  # tool_call_id -> content

    # First pass: collect tool results
    for msg in messages:
        if msg.get("role") == "tool":
            tool_results[msg.get("tool_call_id", "")] = msg.get("content", "")

    # Second pass: rebuild messages
    for msg in messages:
        role = msg.get("role", "")

        if role == "tool":
            # Skip — we'll inline these into the assistant message
            continue

        if role == "assistant" and msg.get("tool_calls"):
            # Convert tool-calling assistant msg to a plain text msg
            tool_calls = msg.get("tool_calls", [])
            parts = []
            if msg.get("content"):
                parts.append(msg["content"])

            for tc in tool_calls:
                fn = tc.get("function", {}) if isinstance(tc, dict) else {}
                if not fn and hasattr(tc, "function"):
                    fn = {"name": tc.function.name, "arguments": tc.function.arguments}
                fn_name = fn.get("name", "unknown")
                fn_args = fn.get("arguments", "{}")
                tc_id = tc.get("id", "") if isinstance(tc, dict) else getattr(tc, "id", "")
                result = tool_results.get(tc_id, "")

                parts.append(f"[已调用工具 {fn_name}({fn_args})]\n结果: {result[:500]}")

            sanitized.append({"role": "assistant", "content": "\n".join(parts)})
        else:
            # Regular message — keep as-is but strip any extra fields
            clean = {"role": role, "content": msg.get("content", "")}
            sanitized.append(clean)

    return sanitized


async def _retry_create(client: AsyncOpenAI, max_retries: int = 2, **kwargs) -> Any:
    """Call client.chat.completions.create with model fallback chain.

    降级逻辑:
    1. 对当前模型重试 max_retries 次（指数退避）
    2. 如果全部失败，切换到降级链中的下一个模型
    3. 每个模型都用完后，最后尝试无 tools 模式

    并发控制:
    - Semaphore 限制最大同时并发数 (LLM_MAX_CONCURRENCY)
    - 请求间隔限制 (LLM_MIN_INTERVAL_MS) 避免触发 rate limit
    """
    global _active_model
    model_chain = get_model_chain()
    original_model = kwargs.get("model", model_chain[0])

    # Ensure the original model is at the front
    if original_model in model_chain:
        model_chain = [original_model] + [m for m in model_chain if m != original_model]

    last_error = None
    semaphore = _get_semaphore()

    for model_idx, model in enumerate(model_chain):
        adapted_kwargs = _adapt_kwargs_for_model(model, kwargs)

        for attempt in range(1, max_retries + 1):
            try:
                # Rate limiting: acquire semaphore + enforce interval
                async with semaphore:
                    await _rate_limited_wait()
                    result = await client.chat.completions.create(**adapted_kwargs)
                # Success — record the active model
                if model != original_model:
                    _active_model = model
                    logger.info(f"✅ 降级成功：{original_model} → {model}")
                return result

            except (APIError, APIStatusError) as e:
                last_error = e
                status = getattr(e, "status_code", 0) or 0
                logger.warning(
                    f"LLM [{model}] 错误 (尝试 {attempt}/{max_retries}): "
                    f"HTTP {status} — {str(e)[:200]}"
                )

                # 429 = rate limit → wait longer
                if status == 429:
                    wait = 5 * attempt
                elif status in (400, 422):
                    # Check if it's a rate limit error disguised as 400
                    err_str = str(e)
                    if "频率超出限制" in err_str or "rate" in err_str.lower():
                        wait = 3 * attempt
                        logger.info(f"  频率限制 (HTTP {status}): {wait}s 后重试...")
                        if attempt < max_retries:
                            await asyncio.sleep(wait)
                        continue
                    # Bad request — likely model doesn't support this format
                    # Try next model immediately instead of retrying same one
                    logger.info(f"  HTTP {status}: 跳过 {model}，尝试下一个模型")
                    break
                else:
                    wait = 2 ** attempt

                if attempt < max_retries:
                    logger.info(f"  {wait}s 后重试...")
                    await asyncio.sleep(wait)

            except Exception as e:
                last_error = e
                logger.warning(f"LLM [{model}] 非API错误 (尝试 {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)

        # This model exhausted retries, log and try next
        if model_idx < len(model_chain) - 1:
            next_model = model_chain[model_idx + 1]
            logger.warning(f"⬇️ 模型降级：{model} → {next_model}")

    # All models failed — last resort: no-tools fallback with the fastest model
    logger.warning("所有模型均失败。最后尝试：无 tools 模式 + 最快模型")
    fallback_model = model_chain[-1]  # Use the last (cheapest/fastest) model
    fallback_kwargs = {k: v for k, v in kwargs.items() if k not in ("tools", "tool_choice")}
    fallback_kwargs = _adapt_kwargs_for_model(fallback_model, fallback_kwargs)
    if "messages" in fallback_kwargs:
        fallback_kwargs["messages"] = _sanitize_messages_for_no_tools(fallback_kwargs["messages"])
    try:
        async with semaphore:
            await _rate_limited_wait()
            result = await client.chat.completions.create(**fallback_kwargs)
        _active_model = fallback_model
        logger.info(f"✅ 无tools兜底成功 (模型: {fallback_model})")
        return result
    except Exception as e2:
        logger.error(f"兜底也失败: {e2}")
        raise last_error from e2


# ─── Simple Completion (no tools, for evaluation) ───

async def simple_completion(
    messages: List[Dict[str, str]],
    temperature: float = 0.7,
    max_tokens: int = 4096,
    timeout: float = 120.0,
) -> str:
    """Run a simple chat completion WITHOUT tool calling.

    Use this for evaluation, scoring, or any scenario where tools
    are not needed. Supports model fallback chain.
    timeout: max seconds to wait for the LLM response (default 120s).
    """
    client = _require_client()
    model = get_llm_model()

    try:
        response = await asyncio.wait_for(
            _retry_create(
                client,
                max_retries=2,
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            ),
            timeout=timeout,
        )
        return response.choices[0].message.content or ""
    except asyncio.TimeoutError:
        logger.error(f"simple_completion timed out after {timeout}s")
        return f"[LLM Timeout: 超过{int(timeout)}秒未响应]"
    except Exception as e:
        logger.error(f"simple_completion failed: {e}")
        return f"[LLM Error: {str(e)[:200]}]"


# ─── Chat Completion (non-streaming, with tool loop) ───

async def chat_completion(
    messages: List[Dict[str, str]],
    max_tool_rounds: int = 5,
) -> str:
    """Run chat completion with automatic tool-calling loop.

    Returns the final assistant text response after all tool calls are resolved.
    Includes retry logic and fallback to no-tools mode on persistent errors.
    """
    client = _require_client()
    model = get_llm_model()
    working_messages = list(messages)

    for round_num in range(max_tool_rounds):
        response = await _retry_create(
            client,
            max_retries=2,
            model=model,
            messages=working_messages,
            tools=TOOL_DEFINITIONS,
            tool_choice="auto",
            temperature=0.7,
            max_tokens=4096,
        )

        choice = response.choices[0]
        msg = choice.message

        # If no tool calls, we're done
        if not msg.tool_calls:
            return msg.content or ""

        # Append assistant message with tool_calls
        working_messages.append(msg.model_dump())

        # Execute each tool call
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                fn_args = {}

            logger.info(f"Tool call [{round_num+1}]: {fn_name}({fn_args})")
            result = await execute_tool(fn_name, fn_args)

            working_messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    # If we exhausted tool rounds, do one final completion without tools
    response = await _retry_create(
        client,
        max_retries=2,
        model=model,
        messages=working_messages,
        temperature=0.7,
        max_tokens=4096,
    )
    return response.choices[0].message.content or ""


# ─── Streaming Chat Completion (with tool loop) ───

async def chat_completion_stream(
    messages: List[Dict[str, str]],
    max_tool_rounds: int = 5,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Run streaming chat completion with tool-calling loop.

    Yields events:
    - {"type": "tool_call", "name": "...", "arguments": {...}}
    - {"type": "tool_result", "name": "...", "result": "..."}
    - {"type": "token", "content": "..."}
    - {"type": "done"}
    """
    client = _require_client()
    model = get_llm_model()
    working_messages = list(messages)

    for round_num in range(max_tool_rounds):
        # Non-streaming call to check for tool calls
        response = await _retry_create(
            client,
            max_retries=2,
            model=model,
            messages=working_messages,
            tools=TOOL_DEFINITIONS,
            tool_choice="auto",
            temperature=0.7,
            max_tokens=4096,
        )

        choice = response.choices[0]
        msg = choice.message

        if not msg.tool_calls:
            # No tool calls — stream the final response
            # Use the active model (may have changed due to fallback)
            stream_model = _active_model or model
            stream_kwargs = _adapt_kwargs_for_model(stream_model, {
                "model": stream_model,
                "messages": working_messages,
                "temperature": 0.7,
                "max_tokens": 4096,
                "stream": True,
            })
            stream = await client.chat.completions.create(**stream_kwargs)
            async for chunk in stream:
                delta = chunk.choices[0].delta if chunk.choices else None
                if delta and delta.content:
                    yield {"type": "token", "content": delta.content}
            yield {"type": "done"}
            return

        # Has tool calls — execute them
        working_messages.append(msg.model_dump())

        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                fn_args = {}

            yield {"type": "tool_call", "name": fn_name, "arguments": fn_args}

            result = await execute_tool(fn_name, fn_args)

            working_messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

            yield {"type": "tool_result", "name": fn_name, "result": result[:200]}

    # Final streaming response after exhausting tool rounds
    stream_model = _active_model or model
    stream_kwargs = _adapt_kwargs_for_model(stream_model, {
        "model": stream_model,
        "messages": working_messages,
        "temperature": 0.7,
        "max_tokens": 4096,
        "stream": True,
    })
    stream = await client.chat.completions.create(**stream_kwargs)
    async for chunk in stream:
        delta = chunk.choices[0].delta if chunk.choices else None
        if delta and delta.content:
            yield {"type": "token", "content": delta.content}
    yield {"type": "done"}
