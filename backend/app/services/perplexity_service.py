"""Perplexity AI service — real-time research via Perplexity Sonar API."""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class PerplexityService:
    """Wrapper for Perplexity AI research API."""

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    async def research(self, query: str, mode: str = "concise") -> Dict[str, Any]:
        """Execute a research query via Perplexity Sonar."""
        if not self.api_key:
            return {"content": "Perplexity API key not configured", "sources": []}

        try:
            import httpx

            model = "llama-3.1-sonar-large-128k-online" if mode == "deep" else "llama-3.1-sonar-small-128k-online"

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.perplexity.ai/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": "You are a financial research assistant."},
                            {"role": "user", "content": query},
                        ],
                        "max_tokens": 2000,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                citations = data.get("citations", [])
                return {"content": content, "sources": citations}

        except Exception as e:
            logger.error(f"Perplexity research failed: {e}")
            return {"content": f"Research failed: {str(e)[:200]}", "sources": []}
