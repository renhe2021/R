"""LLM layer — OpenAI chat completions with tool-calling support.

Supports:
- GPT-4o / GPT-4o-mini with function calling
- Streaming responses for real-time UI updates

Requires OPENAI_API_KEY to be configured in .env.
"""

import json
import logging
from typing import List, Dict, Any, Optional, AsyncGenerator

from openai import AsyncOpenAI
from app.config import get_settings

logger = logging.getLogger(__name__)

# Lazy-init client
_client: Optional[AsyncOpenAI] = None


def _get_client() -> Optional[AsyncOpenAI]:
    """Get or create the OpenAI async client."""
    global _client
    if _client is not None:
        return _client
    settings = get_settings()
    if settings.openai_api_key:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
        return _client
    return None


def is_llm_available() -> bool:
    """Check if any LLM API key is configured."""
    settings = get_settings()
    return bool(settings.openai_api_key or settings.anthropic_api_key)


def get_llm_model() -> str:
    """Get the configured LLM model name."""
    settings = get_settings()
    return settings.openai_model or "gpt-4o"


def _require_client() -> AsyncOpenAI:
    """Get the OpenAI client or raise a clear configuration error."""
    client = _get_client()
    if client is None:
        raise RuntimeError(
            "OPENAI_API_KEY 未配置。请在 backend/.env 文件中设置:\n"
            "  OPENAI_API_KEY=sk-your-key-here\n"
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
]


# ─── Tool Execution ───

async def execute_tool(name: str, arguments: Dict[str, Any]) -> str:
    """Execute a tool by name and return the result as a string."""
    from app.agent.tools import (
        scan_fundamentals, scan_history, detect_shenanigans,
        run_full_valuation, search_knowledge, analyze_news,
        research_realtime,
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
        else:
            return f"Unknown tool: {name}"
    except Exception as e:
        logger.error(f"Tool execution error ({name}): {e}")
        return f"Tool error ({name}): {str(e)[:500]}"


# ─── Chat Completion (non-streaming, with tool loop) ───

async def chat_completion(
    messages: List[Dict[str, str]],
    max_tool_rounds: int = 5,
) -> str:
    """Run chat completion with automatic tool-calling loop.

    Returns the final assistant text response after all tool calls are resolved.
    """
    client = _require_client()
    model = get_llm_model()
    working_messages = list(messages)

    for round_num in range(max_tool_rounds):
        response = await client.chat.completions.create(
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
    response = await client.chat.completions.create(
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
        response = await client.chat.completions.create(
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
            stream = await client.chat.completions.create(
                model=model,
                messages=working_messages,
                temperature=0.7,
                max_tokens=4096,
                stream=True,
            )
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
    stream = await client.chat.completions.create(
        model=model,
        messages=working_messages,
        temperature=0.7,
        max_tokens=4096,
        stream=True,
    )
    async for chunk in stream:
        delta = chunk.choices[0].delta if chunk.choices else None
        if delta and delta.content:
            yield {"type": "token", "content": delta.content}
    yield {"type": "done"}
