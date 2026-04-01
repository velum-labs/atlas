"""Runtime and capability helpers for learning workflows."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alma_atlas.agents.acp.session_runtime import ACPSessionRuntime
    from alma_atlas.agents.provider import LLMProvider
    from alma_atlas.config import AgentConfig, LearningConfig


def is_real_provider(provider_name: str) -> bool:
    """Return True if the provider is a real (non-mock) LLM provider."""
    return provider_name != "mock"


def effective_provider_name(agent_cfg: AgentConfig) -> str:
    """Return the runtime provider name for one agent config."""
    return "acp" if agent_cfg.agent is not None else agent_cfg.provider


def agent_runtime_fingerprint(
    agent_cfg: AgentConfig,
    *,
    repo_path: Path,
) -> tuple[str, tuple[str, ...], tuple[tuple[str, str], ...], str] | None:
    """Return a fingerprint for the ACP subprocess/session config."""
    if effective_provider_name(agent_cfg) != "acp":
        return None

    process_cfg = agent_cfg.agent
    command = process_cfg.command if process_cfg is not None else "claude-agent-acp"
    args = tuple(process_cfg.args) if process_cfg is not None else ()
    env = tuple(sorted((process_cfg.env if process_cfg is not None else {}).items()))
    return (command, args, env, str(repo_path))


def shared_runtime_for_configs(
    agent_cfgs: list[AgentConfig],
    *,
    repo_path: Path,
) -> ACPSessionRuntime | None:
    """Return one shared ACP runtime when all configs resolve identically."""
    fingerprints = [
        fingerprint
        for cfg in agent_cfgs
        if (fingerprint := agent_runtime_fingerprint(cfg, repo_path=repo_path)) is not None
    ]
    if not fingerprints:
        return None
    if any(fingerprint != fingerprints[0] for fingerprint in fingerprints[1:]):
        return None

    from alma_atlas.agents.acp.session_runtime import ACPSessionRuntime

    command, args, env_items, cwd = fingerprints[0]
    return ACPSessionRuntime(
        command=command,
        args=list(args),
        env=dict(env_items),
        cwd=cwd,
    )


def supports_direct_repo_exploration(provider: LLMProvider) -> bool:
    return bool(getattr(provider, "supports_direct_repo_exploration", False))


def agent_config_is_enabled(agent_cfg: AgentConfig) -> bool:
    """Return True when one agent config is configured for non-mock execution."""
    return is_real_provider(effective_provider_name(agent_cfg))


def edge_learning_is_enabled(config: LearningConfig) -> bool:
    """Return True when edge learning has the required non-mock agents."""
    return agent_config_is_enabled(config.explorer) and agent_config_is_enabled(config.pipeline_analyzer)


def asset_annotation_is_enabled(config: LearningConfig) -> bool:
    """Return True when asset annotation has the required non-mock agents."""
    return agent_config_is_enabled(config.explorer) and agent_config_is_enabled(config.annotator)


def provider_from_agent_config(
    agent_cfg: AgentConfig,
    *,
    repo_path: Path | None = None,
    runtime: ACPSessionRuntime | None = None,
) -> LLMProvider:
    """Instantiate an LLMProvider from one agent config."""
    from alma_atlas.agents.provider import make_provider

    agent_process_config = agent_cfg.agent
    return make_provider(
        effective_provider_name(agent_cfg),
        model=agent_cfg.model,
        agent_command=agent_process_config.command if agent_process_config else "claude-agent-acp",
        agent_args=list(agent_process_config.args) if agent_process_config else None,
        agent_env=dict(agent_process_config.env) if agent_process_config else None,
        agent_cwd=str(repo_path) if repo_path is not None else None,
        runtime=runtime,
    )


async def close_owned_learning_runtime(
    *,
    runtime: ACPSessionRuntime | None,
    providers: list[LLMProvider],
) -> None:
    """Close one shared runtime or the individually owned providers."""
    if runtime is not None:
        await runtime.aclose()
        return
    for provider in providers:
        await provider.aclose()
