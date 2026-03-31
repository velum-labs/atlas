"""Configuration management for Alma Atlas.

Manages the Atlas configuration directory (``~/.alma/`` by default),
which stores:
- Source connection profiles (``sources.json``)
- The SQLite asset database (``atlas.db``)
- User preferences (``config.json``)

Configuration can be overridden via environment variables or CLI flags.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from alma_atlas.config_store import AtlasConfigStore
from alma_atlas.source_registry import redact_source_params

# Known top-level keys for atlas.yml.  Unknown keys are rejected (fail-closed).
# `enrichment` is kept as a deprecated alias for `learning`.
_KNOWN_ATLAS_YML_KEYS = frozenset({"version", "sources", "team", "hooks", "learning", "enrichment"})

SUPPORTED_LEARNING_PROVIDERS = frozenset({"acp", "mock"})
DEFAULT_AGENT_PROVIDER = "mock"
DEFAULT_AGENT_MODEL = "claude-opus-4-6"
DEFAULT_EXPLORER_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_AGENT_API_KEY_ENV = "ANTHROPIC_API_KEY"
DEFAULT_AGENT_TIMEOUT = 120
DEFAULT_AGENT_MAX_TOKENS = 4096

logger = logging.getLogger(__name__)


def default_config_dir() -> Path:
    """Return the default Alma Atlas config directory.

    Respects the ``ALMA_CONFIG_DIR`` environment variable.
    Defaults to ``~/.alma``.
    """
    env = os.environ.get("ALMA_CONFIG_DIR")
    if env:
        return Path(env)
    return Path.home() / ".alma"


def _validate_learning_provider(provider: str, *, context: str) -> str:
    if provider not in SUPPORTED_LEARNING_PROVIDERS:
        raise ValueError(
            f"{context}: unsupported learning provider {provider!r}. "
            f"Supported providers: {sorted(SUPPORTED_LEARNING_PROVIDERS)}. "
            "Use 'provider: acp' with an optional 'agent.command', or 'provider: mock'."
        )
    return provider


@dataclass
class AgentProcessConfig:
    """Configuration for an ACP agent subprocess.

    Used by :class:`ACPProvider` when ``provider: acp`` is set.
    Any ACP-compatible binary can be used as the agent command.
    """

    command: str = "claude-agent-acp"
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class AgentConfig:
    """Configuration for a single learning agent.

    Atlas only supports ``mock`` and ACP-backed agents at runtime.
    ``model``, ``api_key_env``, ``timeout``, and ``max_tokens`` are preserved
    as compatibility metadata for existing configs and tests; ACP execution is
    controlled by ``agent`` / ``provider`` rather than these fields.
    """

    provider: str = DEFAULT_AGENT_PROVIDER  # mock | acp
    model: str = DEFAULT_AGENT_MODEL
    api_key_env: str = DEFAULT_AGENT_API_KEY_ENV
    timeout: int = DEFAULT_AGENT_TIMEOUT
    max_tokens: int = DEFAULT_AGENT_MAX_TOKENS
    # ACP agent subprocess config. Set when provider == "acp".
    agent: AgentProcessConfig | None = None

    def with_agent_process(self, agent: AgentProcessConfig) -> AgentConfig:
        """Return a copy with a shared ACP agent subprocess config."""
        return replace(self, agent=agent)


@dataclass
class LearningConfig:
    """Configuration for the learning pipeline agents.

    Supports two formats in ``atlas.yml``:

    *Flat (legacy)*: top-level ``provider``, ``model``, etc. fields are applied
    to all three agents for backward compatibility.

    *Nested (per-agent)*: ``explorer``, ``pipeline_analyzer``, and
    ``annotator`` sub-sections each carry their own :class:`AgentConfig`.
    """

    # Flat fields — preserved for backward compatibility with older atlas.yml.
    provider: str = DEFAULT_AGENT_PROVIDER  # mock | acp
    model: str = DEFAULT_AGENT_MODEL
    api_key_env: str = DEFAULT_AGENT_API_KEY_ENV
    timeout: int = DEFAULT_AGENT_TIMEOUT
    max_tokens: int = DEFAULT_AGENT_MAX_TOKENS
    # ACP agent subprocess config. Set when provider == "acp" or via agent: key.
    agent: AgentProcessConfig | None = None

    # Per-agent configs.  When the flat YAML format is used these are
    # populated from the flat fields by ``load_atlas_yml``.
    explorer: AgentConfig = field(
        default_factory=lambda: AgentConfig(
            provider=DEFAULT_AGENT_PROVIDER,
            model=DEFAULT_EXPLORER_MODEL,
        )
    )
    pipeline_analyzer: AgentConfig = field(
        default_factory=lambda: AgentConfig(provider=DEFAULT_AGENT_PROVIDER)
    )
    annotator: AgentConfig = field(
        default_factory=lambda: AgentConfig(provider=DEFAULT_AGENT_PROVIDER)
    )


# Backward compatibility alias.
EnrichmentConfig = LearningConfig


@dataclass
class PostScanHook:
    """Configuration for a post-scan hook."""

    name: str
    type: str  # 'webhook' | 'log'
    events: list[str]
    url: str | None = None  # required for webhook
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class SourceConfig:
    """Configuration for a registered data source."""

    id: str
    kind: str  # bigquery, snowflake, postgres, dbt
    params: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        redacted = redact_source_params(self.kind, self.params)
        return f"SourceConfig(id={self.id!r}, kind={self.kind!r}, params={redacted})"


@dataclass
class AtlasConfig:
    """Top-level Alma Atlas configuration."""

    config_dir: Path = field(default_factory=default_config_dir)
    sources: list[SourceConfig] = field(default_factory=list)
    hooks: list[PostScanHook] = field(default_factory=list)
    db_path: Path | None = None
    team_server_url: str | None = None
    team_api_key: str | None = None
    team_id: str | None = None
    learning: LearningConfig = field(default_factory=LearningConfig)

    def __post_init__(self) -> None:
        if self.db_path is None:
            self.db_path = self.config_dir / "atlas.db"

    def __repr__(self) -> str:
        api_key_repr = "***" if self.team_api_key else None
        return (
            f"AtlasConfig("
            f"config_dir={self.config_dir!r}, "
            f"db_path={self.db_path!r}, "
            f"team_server_url={self.team_server_url!r}, "
            f"team_api_key={api_key_repr!r}, "
            f"team_id={self.team_id!r})"
        )

    @property
    def enrichment(self) -> LearningConfig:
        """Backward compatibility alias for ``learning``."""
        return self.learning

    @property
    def sources_file(self) -> Path:
        return self._store().paths.sources_file

    @property
    def config_file(self) -> Path:
        return self._store().paths.config_file

    @property
    def sync_cursor_file(self) -> Path:
        return self._store().paths.sync_cursor_file

    def ensure_dir(self) -> None:
        """Create config directory if it does not exist."""
        self._store().ensure_dir()

    def _store(self) -> AtlasConfigStore:
        return AtlasConfigStore(self.config_dir)

    def load_team_config(self, *, override_existing: bool = False) -> None:
        """Load team settings from config.json into this instance.

        By default, persisted values only fill fields that are currently unset.
        This lets runtime config loaded from ``atlas.yml`` override the local
        persisted team config for a single run without being clobbered later.
        Pass ``override_existing=True`` when the persisted config should win.
        """
        team = self._store().load_team_config()
        if not team:
            return
        if override_existing or self.team_server_url is None:
            self.team_server_url = team.get("server_url")
        if override_existing or self.team_api_key is None:
            self.team_api_key = team.get("api_key")
        if override_existing or self.team_id is None:
            self.team_id = team.get("team_id")

    def save_team_config(self) -> None:
        """Persist team settings to config.json."""
        self._store().save_team_config(
            {
                "server_url": self.team_server_url,
                "api_key": self.team_api_key,
                "team_id": self.team_id,
            }
        )

    def load_sync_cursor(self) -> str | None:
        """Return the stored sync cursor timestamp, or None if none exists."""
        return self._store().load_sync_cursor()

    def save_sync_cursor(self, cursor: str) -> None:
        """Store the sync cursor timestamp."""
        self._store().save_sync_cursor(cursor)

    def load_sources(self) -> list[SourceConfig]:
        """Load registered sources from disk."""
        return [SourceConfig(**raw) for raw in self._store().load_sources()]

    def resolved_sources(self) -> list[SourceConfig]:
        """Return the effective sources for the current run.

        Runtime-only sources from ``atlas.yml`` take precedence when present.
        Otherwise Atlas falls back to the persisted source registry.
        """
        if self.sources:
            return list(self.sources)
        return self.load_sources()

    def save_sources(self, sources: list[SourceConfig]) -> None:
        """Persist registered sources to disk."""
        self._store().save_sources(
            [{"id": source.id, "kind": source.kind, "params": source.params} for source in sources]
        )

    def add_source(self, source: SourceConfig) -> None:
        """Add or update a source in the config file."""
        sources = self.load_sources()
        sources = [s for s in sources if s.id != source.id]
        sources.append(source)
        self.save_sources(sources)

    def remove_source(self, source_id: str) -> bool:
        """Remove a source by ID. Returns True if it existed."""
        sources = self.load_sources()
        new_sources = [s for s in sources if s.id != source_id]
        if len(new_sources) == len(sources):
            return False
        self.save_sources(new_sources)
        return True


_config: AtlasConfig | None = None


def get_config() -> AtlasConfig:
    """Return the global AtlasConfig singleton.

    Auto-discovers ``atlas.yml`` in the default config directory when present,
    so hooks and other YAML-only settings are loaded automatically.
    """
    global _config
    default_dir = default_config_dir()
    should_reload = _config is None or (
        os.environ.get("ALMA_CONFIG_DIR") is not None and _config.config_dir != default_dir
    )
    if should_reload:
        yml_path = default_dir / "atlas.yml"
        _config = load_atlas_yml(yml_path) if yml_path.exists() else AtlasConfig(config_dir=default_dir)
    assert _config is not None
    return _config


def load_atlas_yml(path: Path | str) -> AtlasConfig:
    """Load Atlas configuration from an ``atlas.yml`` file.

    Unknown top-level keys are rejected (fail-closed) to prevent silent
    misconfiguration caused by typos or unsupported options.

    Supports both ``learning:`` (current) and ``enrichment:`` (deprecated alias).
    When both are present, ``learning:`` takes precedence.

    Args:
        path: Path to the ``atlas.yml`` file.

    Returns:
        An :class:`AtlasConfig` populated from the YAML file.

    Raises:
        ValueError:  If the file contains unknown top-level keys.
        FileNotFoundError: If the file does not exist.
    """
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "PyYAML is required to load atlas.yml files. "
            "Install it with: pip install pyyaml"
        ) from exc

    path = Path(path)
    data: dict = yaml.safe_load(path.read_text()) or {}

    unknown = set(data) - _KNOWN_ATLAS_YML_KEYS
    if unknown:
        raise ValueError(
            f"Unknown top-level key(s) in {path.name}: {sorted(unknown)}. "
            f"Allowed keys: {sorted(_KNOWN_ATLAS_YML_KEYS)}"
        )

    cfg = AtlasConfig()

    # Parse sources list.
    for raw_source in data.get("sources", []):
        cfg.sources.append(
            SourceConfig(
                id=raw_source["id"],
                kind=raw_source["kind"],
                params=raw_source.get("params", {}),
            )
        )

    # Parse post-scan hooks.
    for raw_hook in data.get("hooks", {}).get("post_scan", []):
        cfg.hooks.append(
            PostScanHook(
                name=raw_hook["name"],
                type=raw_hook["type"],
                events=raw_hook.get("events", []),
                url=raw_hook.get("url"),
                headers=raw_hook.get("headers", {}),
            )
        )

    # Parse team settings.
    team = data.get("team", {})
    if team:
        cfg.team_server_url = team.get("server_url")
        cfg.team_id = team.get("team_id")
        # Support api_key directly or via env-var indirection.
        api_key_env = team.get("api_key_env")
        if api_key_env:
            cfg.team_api_key = os.environ.get(api_key_env)
        else:
            cfg.team_api_key = team.get("api_key")

    # Parse learning/enrichment settings.
    # `learning:` takes precedence even when it is explicitly empty.
    learning_raw = data.get("learning") or {} if "learning" in data else data.get("enrichment", {})
    if "enrichment" in data and "learning" not in data:
        logger.warning(
            "atlas.yml: 'enrichment:' key is deprecated. Rename it to 'learning:' to silence this warning."
        )
    if learning_raw:
        _per_agent_keys = frozenset({"explorer", "pipeline_analyzer", "annotator", "asset_enricher"})
        has_nested = bool(set(learning_raw) & _per_agent_keys)

        def _parse_agent_process_config(sub: dict) -> AgentProcessConfig | None:
            """Parse an optional ``agent:`` sub-key into an AgentProcessConfig."""
            agent_raw = sub.get("agent")
            if not agent_raw:
                return None
            return AgentProcessConfig(
                command=agent_raw.get("command", "claude-agent-acp"),
                args=list(agent_raw.get("args", [])),
                env=dict(agent_raw.get("env", {})),
            )

        if has_nested:
            # Nested per-agent format: each sub-key is an AgentConfig.
            def _parse_agent(sub: dict) -> AgentConfig:
                provider = _validate_learning_provider(
                    sub.get("provider", DEFAULT_AGENT_PROVIDER),
                    context="atlas.yml",
                )
                return AgentConfig(
                    provider=provider,
                    model=sub.get("model", DEFAULT_AGENT_MODEL),
                    api_key_env=sub.get("api_key_env", DEFAULT_AGENT_API_KEY_ENV),
                    timeout=int(sub.get("timeout", DEFAULT_AGENT_TIMEOUT)),
                    max_tokens=int(sub.get("max_tokens", DEFAULT_AGENT_MAX_TOKENS)),
                    agent=_parse_agent_process_config(sub),
                )

            # Support both `annotator` and legacy `asset_enricher` keys in YAML.
            annotator_raw = learning_raw.get("annotator") or learning_raw.get("asset_enricher", {})

            top_agent = _parse_agent_process_config(learning_raw)

            # Parse per-agent configs, inheriting top-level agent if not overridden.
            explorer = _parse_agent(learning_raw.get("explorer", {}))
            pipeline_analyzer = _parse_agent(learning_raw.get("pipeline_analyzer", {}))
            annotator = _parse_agent(annotator_raw)

            # Propagate top-level agent config to sub-agents that lack their own.
            if top_agent is not None:
                if explorer.agent is None:
                    explorer = explorer.with_agent_process(top_agent)
                if pipeline_analyzer.agent is None:
                    pipeline_analyzer = pipeline_analyzer.with_agent_process(top_agent)
                if annotator.agent is None:
                    annotator = annotator.with_agent_process(top_agent)

            cfg.learning = LearningConfig(
                agent=top_agent,
                explorer=explorer,
                pipeline_analyzer=pipeline_analyzer,
                annotator=annotator,
            )
        else:
            # Flat (legacy) format: apply the same values to all agents.
            flat_provider = _validate_learning_provider(
                learning_raw.get("provider", DEFAULT_AGENT_PROVIDER),
                context="atlas.yml",
            )
            flat_model = learning_raw.get("model", DEFAULT_AGENT_MODEL)
            flat_api_key_env = learning_raw.get("api_key_env", DEFAULT_AGENT_API_KEY_ENV)
            flat_timeout = int(learning_raw.get("timeout", DEFAULT_AGENT_TIMEOUT))
            flat_max_tokens = int(learning_raw.get("max_tokens", DEFAULT_AGENT_MAX_TOKENS))
            flat_agent = _parse_agent_process_config(learning_raw)

            def _flat_agent() -> AgentConfig:
                return AgentConfig(
                    provider=flat_provider,
                    model=flat_model,
                    api_key_env=flat_api_key_env,
                    timeout=flat_timeout,
                    max_tokens=flat_max_tokens,
                    agent=flat_agent,
                )

            cfg.learning = LearningConfig(
                provider=flat_provider,
                model=flat_model,
                api_key_env=flat_api_key_env,
                timeout=flat_timeout,
                max_tokens=flat_max_tokens,
                agent=flat_agent,
                explorer=_flat_agent(),
                pipeline_analyzer=_flat_agent(),
                annotator=_flat_agent(),
            )

    return cfg
