"""Runtime backends for Atlas learning workflows.

Providers implement the :class:`LLMProvider` interface and can be selected
via Atlas configuration.

Supported providers: ``acp`` (any ACP-compatible agent), ``mock`` (tests).
The legacy ``anthropic`` and ``openai`` raw-HTTP providers have been removed.
Use ``agent.command: claude-agent-acp`` in atlas.yml instead.
"""

from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING, Any, TypeVar, cast

from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

if TYPE_CHECKING:
    from alma_atlas.agents.acp_provider import ACPSessionRuntime


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
            system_prompt: Instructions for the LLM.
            user_prompt: User-facing content (edges + file snippets).
            response_schema: Pydantic model class to validate the response.

        Returns:
            A validated instance of `response_schema`.
        """
        ...

    async def aclose(self) -> None:
        """Release any held resources.

        Subclasses that hold open resources should override this.
        """
        return  # default: nothing to release


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
        if self._fixed_result is not None and isinstance(self._fixed_result, response_schema):
            return self._fixed_result  # type: ignore[return-value]
        from alma_atlas.agents.schemas import AssetEnrichmentResult, ExplorerResult, PipelineAnalysisResult

        # Return a minimal valid instance for known result schemas.
        if issubclass(response_schema, PipelineAnalysisResult):
            return response_schema.model_validate(  # type: ignore[return-value]
                {
                    "edges": [],
                    "repo_summary": "Mock provider: no analysis performed",
                }
            )

        if issubclass(response_schema, AssetEnrichmentResult):
            return response_schema.model_validate(  # type: ignore[return-value]
                {
                    "annotations": [],
                    "repo_summary": "Mock provider: no enrichment performed",
                }
            )

        if issubclass(response_schema, ExplorerResult):
            return response_schema.model_validate(  # type: ignore[return-value]
                {
                    "files": [],
                    "repo_structure_summary": "Mock provider: no exploration performed",
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
    agent_command: str = "claude-agent-acp",
    agent_args: list[str] | None = None,
    agent_env: dict[str, str] | None = None,
    agent_cwd: str | None = None,
    runtime: ACPSessionRuntime | None = None,
    enable_terminal: bool = True,
    mcp_servers: list[Any] | None = None,
) -> LLMProvider:
    """Instantiate an LLM provider by name.

    Args:
        provider_name: One of ``"acp"`` or ``"mock"``.
        model: Unused. The ACP agent binary controls model choice.
        api_key: Unused. Pass credentials via `agent_env` instead.
        timeout: Unused, kept for backward-compatible call sites.
        max_tokens: Unused, kept for backward-compatible call sites.
        agent_command: ACP agent binary to spawn when `provider_name` is ``"acp"``.
        agent_args: Extra CLI arguments for the ACP agent subprocess.
        agent_env: Extra environment variables for the ACP agent subprocess.
        agent_cwd: Working directory for the ACP agent session.
        runtime: Optional shared ACP runtime/session manager.
        enable_terminal:
            Whether Atlas should advertise ACP terminal support.
        mcp_servers: Optional MCP server configs injected during ``session/new``.

    Returns:
        A configured :class:`LLMProvider` instance.

    Raises:
        ValueError: If *provider_name* is not recognised or is a removed provider.
    """
    if provider_name in ("anthropic", "openai"):
        raise ValueError(
            f"Provider {provider_name!r} is no longer supported. "
            "Atlas now uses ACP agents exclusively. "
            "Update your atlas.yml to use 'learning.agent.command: claude-agent-acp' "
            "(or any other ACP-compatible binary). "
            "See the migration guide for details."
        )
    if provider_name == "acp":
        from .acp_provider import ACPProvider

        return cast(
            LLMProvider,
            ACPProvider(
                command=agent_command,
                args=agent_args,
                env=agent_env,
                cwd=agent_cwd,
                runtime=runtime,
                enable_terminal=enable_terminal,
                mcp_servers=mcp_servers,
            ),
        )
    if provider_name == "mock":
        return MockProvider()
    raise ValueError(f"Unknown provider: {provider_name!r}. Choose from: acp, mock")
