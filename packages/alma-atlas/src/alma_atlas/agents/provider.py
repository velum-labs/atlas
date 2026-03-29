"""Pluggable LLM provider backends for pipeline analysis.

Providers implement the :class:`LLMProvider` interface and can be selected
via Atlas configuration.  API keys are resolved from environment variables
and are never stored in the Atlas database or logged.
"""

from __future__ import annotations

import abc
import logging
import os
from typing import TypeVar

import httpx
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class LLMProvider(abc.ABC):
    """Abstract base class for LLM provider backends."""

    @abc.abstractmethod
    async def analyze(
        self,
        system_prompt: str,
        user_prompt: str,
        response_schema: type[T],
    ) -> T:
        """Run analysis and return a validated structured response.

        Args:
            system_prompt:   Instructions for the LLM.
            user_prompt:     User-facing content (edges + file snippets).
            response_schema: Pydantic model class to validate the response.

        Returns:
            A validated instance of *response_schema*.
        """
        ...

    async def aclose(self) -> None:
        """Release any held resources (e.g. HTTP connections).

        Subclasses that hold open HTTP clients should override this.
        """
        return  # default: nothing to release


class AnthropicProvider(LLMProvider):
    """Anthropic API provider — uses tool_use for schema-enforced structured output."""

    _API_BASE = "https://api.anthropic.com"
    _API_VERSION = "2023-06-01"

    def __init__(
        self,
        *,
        model: str = "claude-sonnet-4-20250514",
        api_key: str | None = None,
        timeout: float = 120.0,
        max_tokens: int = 4096,
    ) -> None:
        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._model = model
        self._max_tokens = max_tokens
        self._client = httpx.AsyncClient(
            base_url=self._API_BASE,
            headers={
                "x-api-key": resolved_key,
                "anthropic-version": self._API_VERSION,
                "content-type": "application/json",
            },
            timeout=timeout,
        )

    async def analyze(
        self,
        system_prompt: str,
        user_prompt: str,
        response_schema: type[T],
    ) -> T:
        schema = response_schema.model_json_schema()
        payload: dict = {
            "model": self._model,
            "max_tokens": self._max_tokens,
            "system": system_prompt,
            "tools": [
                {
                    "name": "report",
                    "description": "Report the pipeline analysis results.",
                    "input_schema": schema,
                }
            ],
            "tool_choice": {"type": "tool", "name": "report"},
            "messages": [{"role": "user", "content": user_prompt}],
        }
        response = await self._client.post("/v1/messages", json=payload)
        response.raise_for_status()
        data: dict = response.json()
        for block in data.get("content", []):
            if block.get("type") == "tool_use" and block.get("name") == "report":
                return response_schema.model_validate(block["input"])
        raise ValueError(
            f"No tool_use block named 'report' in Anthropic response: {data}"
        )

    async def aclose(self) -> None:
        await self._client.aclose()


class OpenAIProvider(LLMProvider):
    """OpenAI API provider — uses structured outputs (json_schema response format)."""

    _API_BASE = "https://api.openai.com"

    def __init__(
        self,
        *,
        model: str = "gpt-4o",
        api_key: str | None = None,
        timeout: float = 120.0,
        max_tokens: int = 4096,
    ) -> None:
        resolved_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self._model = model
        self._max_tokens = max_tokens
        self._client = httpx.AsyncClient(
            base_url=self._API_BASE,
            headers={
                "Authorization": f"Bearer {resolved_key}",
                "content-type": "application/json",
            },
            timeout=timeout,
        )

    async def analyze(
        self,
        system_prompt: str,
        user_prompt: str,
        response_schema: type[T],
    ) -> T:
        schema = response_schema.model_json_schema()
        payload: dict = {
            "model": self._model,
            "max_tokens": self._max_tokens,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "result",
                    "strict": True,
                    "schema": schema,
                },
            },
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        response = await self._client.post("/v1/chat/completions", json=payload)
        response.raise_for_status()
        data: dict = response.json()
        content: str = data["choices"][0]["message"]["content"]
        return response_schema.model_validate_json(content)

    async def aclose(self) -> None:
        await self._client.aclose()


class MockProvider(LLMProvider):
    """Mock provider for testing — returns a fixed or minimal result."""

    def __init__(self, fixed_result: BaseModel | None = None) -> None:
        self._fixed_result = fixed_result

    async def analyze(
        self,
        system_prompt: str,
        user_prompt: str,
        response_schema: type[T],
    ) -> T:
        if self._fixed_result is not None and isinstance(
            self._fixed_result, response_schema
        ):
            return self._fixed_result  # type: ignore[return-value]
        # Return a minimal valid instance for PipelineAnalysisResult.
        from alma_atlas.agents.schemas import PipelineAnalysisResult

        if issubclass(response_schema, PipelineAnalysisResult):
            return response_schema.model_validate(  # type: ignore[return-value]
                {
                    "edges": [],
                    "repo_summary": "Mock provider: no analysis performed",
                }
            )
        return response_schema.model_validate({})


def make_provider(
    provider_name: str,
    *,
    model: str,
    api_key: str | None = None,
    timeout: float = 120.0,
    max_tokens: int = 4096,
) -> LLMProvider:
    """Instantiate an LLM provider by name.

    Args:
        provider_name: One of ``"anthropic"``, ``"openai"``, or ``"mock"``.
        model:         Model identifier passed to the provider.
        api_key:       Optional explicit API key (overrides env var lookup).
        timeout:       HTTP timeout in seconds.
        max_tokens:    Maximum tokens to generate.

    Returns:
        A configured :class:`LLMProvider` instance.

    Raises:
        ValueError: If *provider_name* is not recognised.
    """
    if provider_name == "anthropic":
        return AnthropicProvider(
            model=model,
            api_key=api_key,
            timeout=timeout,
            max_tokens=max_tokens,
        )
    if provider_name == "openai":
        return OpenAIProvider(
            model=model,
            api_key=api_key,
            timeout=timeout,
            max_tokens=max_tokens,
        )
    if provider_name == "mock":
        return MockProvider()
    raise ValueError(
        f"Unknown provider: {provider_name!r}. Choose from: anthropic, openai, mock"
    )
