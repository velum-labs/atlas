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
from alma_atlas.source_records import AtlasSourceDefinition, AtlasSourceRecord, AtlasSourceState
from alma_connectors.catalog import redact_source_params

# Known top-level keys for atlas.yml. Unknown keys are rejected (fail-closed).
_KNOWN_ATLAS_YML_KEYS = frozenset(
    {"version", "sources", "team", "hooks", "learning", "privacy", "edge_discovery"}
)
_ALLOWED_HOOK_TYPES = frozenset({"webhook", "log"})
_ALLOWED_PRIVACY_STORAGE_MODES = frozenset({"full_sql", "redacted_sql", "fingerprint_only"})

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


def _parse_bool(value: object, *, context: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{context}: expected a boolean value, got {value!r}")


def _parse_positive_int(
    value: object,
    *,
    context: str,
    allow_none: bool = False,
) -> int | None:
    if value is None and allow_none:
        return None
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{context}: expected a positive integer, got {value!r}") from exc
    if parsed < 1:
        raise ValueError(f"{context}: expected a positive integer, got {parsed!r}")
    return parsed


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
    """Configuration for one learning workflow role.

    Atlas only supports ``mock`` and ACP-backed agents at runtime.
    These configs describe workflow roles such as ``explorer`` or
    ``annotator``; Atlas may reuse one ACP runtime/session across multiple roles
    when they resolve to the same subprocess settings.
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
    """Configuration for ACP-backed learning workflows."""

    # Optional shared ACP agent subprocess config applied to roles
    # that do not define their own `agent` block.
    agent: AgentProcessConfig | None = None

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


@dataclass
class PrivacyConfig:
    """Privacy and retention controls for Atlas query data surfaces."""

    include_sql_previews: bool = False
    query_storage_mode: str = "full_sql"
    query_retention_days: int | None = None

    def __post_init__(self) -> None:
        if self.query_storage_mode not in _ALLOWED_PRIVACY_STORAGE_MODES:
            raise ValueError(
                "query_storage_mode must be one of "
                f"{sorted(_ALLOWED_PRIVACY_STORAGE_MODES)}"
            )
        if self.query_retention_days is not None and self.query_retention_days < 1:
            raise ValueError("query_retention_days must be >= 1 when provided")


def _normalize_source_pairs(value: object, *, context: str) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{context}: expected a list of [source_a, source_b] pairs")

    pairs: list[tuple[str, str]] = []
    for raw_pair in value:
        if (
            not isinstance(raw_pair, (list, tuple))
            or len(raw_pair) != 2
            or not isinstance(raw_pair[0], str)
            or not isinstance(raw_pair[1], str)
        ):
            raise ValueError(f"{context}: each pair must be a two-item string sequence")
        left = raw_pair[0].strip()
        right = raw_pair[1].strip()
        if not left or not right:
            raise ValueError(f"{context}: source pair values must be non-empty strings")
        normalized = tuple(sorted((left, right)))
        pairs.append((normalized[0], normalized[1]))
    return tuple(dict.fromkeys(pairs))


@dataclass
class EdgeDiscoverySettings:
    """Runtime controls for the pairwise cross-system edge discovery pass."""

    match_threshold: float | None = None
    dest_dataset_scope: tuple[str, ...] = ()
    allowed_source_pairs: tuple[tuple[str, str], ...] = ()
    denied_source_pairs: tuple[tuple[str, str], ...] = ()
    max_source_pairs: int | None = None

    def __post_init__(self) -> None:
        if self.match_threshold is not None and not 0.0 <= self.match_threshold <= 1.0:
            raise ValueError("match_threshold must be in [0.0, 1.0] when provided")
        normalized_scope = tuple(
            str(item).strip().lower()
            for item in self.dest_dataset_scope
            if str(item).strip()
        )
        object.__setattr__(self, "dest_dataset_scope", normalized_scope)
        if self.max_source_pairs is not None and self.max_source_pairs < 1:
            raise ValueError("max_source_pairs must be >= 1 when provided")


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
    privacy: PrivacyConfig = field(default_factory=PrivacyConfig)
    edge_discovery: EdgeDiscoverySettings = field(default_factory=EdgeDiscoverySettings)

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
        return [self._source_config_from_record(record) for record in self.load_source_records()]

    def load_source_records(self) -> list[AtlasSourceRecord]:
        """Load registered source definitions plus runtime state from disk."""
        return self._store().load_source_records()

    def resolved_sources(self) -> list[SourceConfig]:
        """Return the effective sources for the current run.

        Runtime-only sources from ``atlas.yml`` take precedence when present.
        Otherwise Atlas falls back to the persisted source registry.
        """
        return [self._source_config_from_record(record) for record in self.resolved_source_records()]

    def resolved_source_records(self) -> list[AtlasSourceRecord]:
        """Return the effective source definitions for the current run."""
        if self.sources:
            return [self._record_from_source_config(source) for source in self.sources]
        return self.load_source_records()

    def save_sources(self, sources: list[SourceConfig]) -> None:
        """Persist registered sources to disk."""
        self.save_source_records([self._record_from_source_config(source) for source in sources])

    def save_source_records(self, sources: list[AtlasSourceRecord]) -> None:
        """Persist registered source definitions plus runtime state to disk."""
        self._store().save_source_records(sources)

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

    @staticmethod
    def _record_from_source_config(source: SourceConfig) -> AtlasSourceRecord:
        params = dict(source.params)
        observation_cursor = params.pop("observation_cursor", None)
        return AtlasSourceRecord(
            definition=AtlasSourceDefinition(
                id=source.id,
                kind=source.kind,
                params=params,
            ),
            state=AtlasSourceState(
                observation_cursor=dict(observation_cursor) if isinstance(observation_cursor, dict) else None,
            ),
        )

    @staticmethod
    def _source_config_from_record(record: AtlasSourceRecord) -> SourceConfig:
        params = dict(record.definition.params)
        if record.state.observation_cursor is not None:
            params["observation_cursor"] = dict(record.state.observation_cursor)
        return SourceConfig(
            id=record.definition.id,
            kind=record.definition.kind,
            params=params,
        )


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

    path = Path(path).expanduser().resolve()
    raw_data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(raw_data, dict):
        raise ValueError(f"{path.name}: expected a top-level mapping, got {type(raw_data).__name__}")
    data: dict[str, Any] = raw_data

    unknown = set(data) - _KNOWN_ATLAS_YML_KEYS
    if unknown:
        raise ValueError(
            f"Unknown top-level key(s) in {path.name}: {sorted(unknown)}. "
            f"Allowed keys: {sorted(_KNOWN_ATLAS_YML_KEYS)}"
        )

    version = data.get("version")
    if version is not None:
        parsed_version = _parse_positive_int(version, context=f"{path.name}:version")
        if parsed_version != 1:
            raise ValueError(f"{path.name}: unsupported version {version!r}. Supported versions: [1]")

    env_config_dir = os.environ.get("ALMA_CONFIG_DIR")
    config_dir = default_config_dir() if env_config_dir else path.parent
    if env_config_dir and path.parent != config_dir:
        logger.warning(
            "atlas.yml loaded from %s but ALMA_CONFIG_DIR points to %s; persisted Atlas state will use %s",
            path.parent,
            config_dir,
            config_dir,
        )
    cfg = AtlasConfig(config_dir=config_dir)

    # Parse sources list.
    raw_sources = data.get("sources", [])
    if not isinstance(raw_sources, list):
        raise ValueError("atlas.yml: 'sources' must be a list")
    for raw_source in raw_sources:
        if not isinstance(raw_source, dict):
            raise ValueError("atlas.yml: each source must be a mapping")
        source_id = raw_source.get("id")
        source_kind = raw_source.get("kind")
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("atlas.yml: each source requires a non-empty string 'id'")
        if not isinstance(source_kind, str) or not source_kind.strip():
            raise ValueError(f"atlas.yml: source {source_id!r} requires a non-empty string 'kind'")
        raw_params = raw_source.get("params", {})
        if raw_params is None:
            raw_params = {}
        if not isinstance(raw_params, dict):
            raise ValueError(f"atlas.yml: source {source_id!r} params must be a mapping")
        cfg.sources.append(
            SourceConfig(
                id=source_id.strip(),
                kind=source_kind.strip(),
                params=dict(raw_params),
            )
        )

    # Parse post-scan hooks.
    hooks_raw = data.get("hooks")
    if hooks_raw is not None:
        if not isinstance(hooks_raw, dict):
            raise ValueError("atlas.yml: 'hooks' must be a mapping")
        post_scan_hooks = hooks_raw.get("post_scan", [])
        if not isinstance(post_scan_hooks, list):
            raise ValueError("atlas.yml: 'hooks.post_scan' must be a list")
    else:
        post_scan_hooks = []
    for raw_hook in post_scan_hooks:
        if not isinstance(raw_hook, dict):
            raise ValueError("atlas.yml: each post_scan hook must be a mapping")
        hook_name = raw_hook.get("name")
        hook_type = raw_hook.get("type")
        if not isinstance(hook_name, str) or not hook_name.strip():
            raise ValueError("atlas.yml: each post_scan hook requires a non-empty string 'name'")
        if not isinstance(hook_type, str) or hook_type not in _ALLOWED_HOOK_TYPES:
            raise ValueError(
                "atlas.yml: post_scan hook 'type' must be one of "
                f"{sorted(_ALLOWED_HOOK_TYPES)}"
            )
        raw_events = raw_hook.get("events", [])
        if not isinstance(raw_events, list) or any(not isinstance(event, str) for event in raw_events):
            raise ValueError("atlas.yml: post_scan hook 'events' must be a list of strings")
        raw_headers = raw_hook.get("headers", {})
        if not isinstance(raw_headers, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in raw_headers.items()
        ):
            raise ValueError("atlas.yml: post_scan hook 'headers' must be a string-to-string mapping")
        raw_url = raw_hook.get("url")
        if hook_type == "webhook":
            if not isinstance(raw_url, str) or not raw_url.strip():
                raise ValueError("atlas.yml: webhook post_scan hooks require a non-empty string 'url'")
            url = raw_url.strip()
        else:
            if raw_url is not None and not isinstance(raw_url, str):
                raise ValueError("atlas.yml: post_scan hook 'url' must be a string when provided")
            url = raw_url.strip() if isinstance(raw_url, str) and raw_url.strip() else None
        cfg.hooks.append(
            PostScanHook(
                name=hook_name.strip(),
                type=hook_type,
                events=[event.strip() for event in raw_events if event.strip()],
                url=url,
                headers=dict(raw_headers),
            )
        )

    # Parse team settings.
    team = data.get("team", {})
    if team and not isinstance(team, dict):
        raise ValueError("atlas.yml: 'team' must be a mapping")
    if team:
        cfg.team_server_url = team.get("server_url")
        cfg.team_id = team.get("team_id")
        # Support api_key directly or via env-var indirection.
        api_key_env = team.get("api_key_env")
        if api_key_env:
            if not isinstance(api_key_env, str) or not api_key_env.strip():
                raise ValueError("atlas.yml: 'team.api_key_env' must be a non-empty string")
            cfg.team_api_key = os.environ.get(api_key_env)
            if cfg.team_api_key is None:
                logger.warning("atlas.yml references team.api_key_env=%s but it is not set", api_key_env)
        else:
            cfg.team_api_key = team.get("api_key")

    # Parse privacy settings.
    privacy_raw = data.get("privacy", {})
    if privacy_raw:
        if not isinstance(privacy_raw, dict):
            raise ValueError("atlas.yml: 'privacy' must be a mapping")
        allowed_privacy_keys = frozenset(
            {"include_sql_previews", "query_storage_mode", "query_retention_days"}
        )
        unknown_privacy_keys = set(privacy_raw) - allowed_privacy_keys
        if unknown_privacy_keys:
            raise ValueError(
                f"atlas.yml: unknown privacy key(s): {sorted(unknown_privacy_keys)}. "
                f"Allowed keys: {sorted(allowed_privacy_keys)}"
            )
        cfg.privacy = PrivacyConfig(
            include_sql_previews=(
                _parse_bool(
                    privacy_raw.get("include_sql_previews"),
                    context="atlas.yml:privacy.include_sql_previews",
                )
                if "include_sql_previews" in privacy_raw
                else False
            ),
            query_storage_mode=str(privacy_raw.get("query_storage_mode", "full_sql")),
            query_retention_days=_parse_positive_int(
                privacy_raw.get("query_retention_days"),
                context="atlas.yml:privacy.query_retention_days",
                allow_none=True,
            ),
        )

    edge_discovery_raw = data.get("edge_discovery", {})
    if edge_discovery_raw:
        if not isinstance(edge_discovery_raw, dict):
            raise ValueError("atlas.yml: 'edge_discovery' must be a mapping")
        allowed_edge_discovery_keys = frozenset(
            {
                "match_threshold",
                "dest_dataset_scope",
                "allowed_source_pairs",
                "denied_source_pairs",
                "max_source_pairs",
            }
        )
        unknown_edge_keys = set(edge_discovery_raw) - allowed_edge_discovery_keys
        if unknown_edge_keys:
            raise ValueError(
                f"atlas.yml: unknown edge_discovery key(s): {sorted(unknown_edge_keys)}. "
                f"Allowed keys: {sorted(allowed_edge_discovery_keys)}"
            )
        raw_threshold = edge_discovery_raw.get("match_threshold")
        match_threshold = None if raw_threshold is None else float(raw_threshold)
        raw_scope = edge_discovery_raw.get("dest_dataset_scope", ())
        if raw_scope is None:
            raw_scope = ()
        if not isinstance(raw_scope, (list, tuple)):
            raise ValueError("atlas.yml: edge_discovery.dest_dataset_scope must be a list of strings")
        cfg.edge_discovery = EdgeDiscoverySettings(
            match_threshold=match_threshold,
            dest_dataset_scope=tuple(str(item) for item in raw_scope),
            allowed_source_pairs=_normalize_source_pairs(
                edge_discovery_raw.get("allowed_source_pairs"),
                context="atlas.yml:edge_discovery.allowed_source_pairs",
            ),
            denied_source_pairs=_normalize_source_pairs(
                edge_discovery_raw.get("denied_source_pairs"),
                context="atlas.yml:edge_discovery.denied_source_pairs",
            ),
            max_source_pairs=_parse_positive_int(
                edge_discovery_raw.get("max_source_pairs"),
                context="atlas.yml:edge_discovery.max_source_pairs",
                allow_none=True,
            ),
        )

    # Parse learning settings.
    learning_raw = data.get("learning", {})
    if learning_raw:
        if not isinstance(learning_raw, dict):
            raise ValueError("atlas.yml: 'learning' must be a mapping")

        def _parse_agent_process_config(sub: dict) -> AgentProcessConfig | None:
            """Parse an optional ``agent:`` sub-key into an AgentProcessConfig."""
            agent_raw = sub.get("agent")
            if not agent_raw:
                return None
            if not isinstance(agent_raw, dict):
                raise ValueError("atlas.yml: 'agent' must be a mapping")
            return AgentProcessConfig(
                command=agent_raw.get("command", "claude-agent-acp"),
                args=list(agent_raw.get("args", [])),
                env=dict(agent_raw.get("env", {})),
            )
        allowed_learning_keys = frozenset({"agent", "explorer", "pipeline_analyzer", "annotator"})
        unknown_learning_keys = set(learning_raw) - allowed_learning_keys
        if unknown_learning_keys:
            raise ValueError(
                f"atlas.yml: unknown learning key(s): {sorted(unknown_learning_keys)}. "
                f"Allowed keys: {sorted(allowed_learning_keys)}"
            )

        def _parse_agent(sub: dict, *, context: str) -> AgentConfig:
            if not isinstance(sub, dict):
                raise ValueError(f"atlas.yml: '{context}' must be a mapping")
            provider = _validate_learning_provider(
                sub.get("provider", DEFAULT_AGENT_PROVIDER),
                context=f"atlas.yml:{context}",
            )
            return AgentConfig(
                provider=provider,
                model=sub.get("model", DEFAULT_AGENT_MODEL),
                api_key_env=sub.get("api_key_env", DEFAULT_AGENT_API_KEY_ENV),
                timeout=int(
                    _parse_positive_int(
                        sub.get("timeout", DEFAULT_AGENT_TIMEOUT),
                        context=f"atlas.yml:{context}.timeout",
                    )
                ),
                max_tokens=int(
                    _parse_positive_int(
                        sub.get("max_tokens", DEFAULT_AGENT_MAX_TOKENS),
                        context=f"atlas.yml:{context}.max_tokens",
                    )
                ),
                agent=_parse_agent_process_config(sub),
            )

        top_agent = _parse_agent_process_config(learning_raw)
        explorer = _parse_agent(learning_raw.get("explorer", {}), context="explorer")
        pipeline_analyzer = _parse_agent(
            learning_raw.get("pipeline_analyzer", {}),
            context="pipeline_analyzer",
        )
        annotator = _parse_agent(learning_raw.get("annotator", {}), context="annotator")

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

    return cfg
