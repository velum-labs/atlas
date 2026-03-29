"""Configuration management for Alma Atlas.

Manages the Atlas configuration directory (``~/.alma/`` by default),
which stores:
- Source connection profiles (``sources.json``)
- The SQLite asset database (``atlas.db``)
- User preferences (``config.json``)

Configuration can be overridden via environment variables or CLI flags.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Known top-level keys for atlas.yml.  Unknown keys are rejected (fail-closed).
_KNOWN_ATLAS_YML_KEYS = frozenset({"version", "sources", "team", "scan", "hooks", "enrichment"})

# Keys whose values must be redacted in __repr__ output.
_SECRET_PARAM_KEYS = frozenset({"dsn", "password", "api_key", "api_secret", "client_secret", "auth_token"})


def default_config_dir() -> Path:
    """Return the default Alma Atlas config directory.

    Respects the ``ALMA_CONFIG_DIR`` environment variable.
    Defaults to ``~/.alma``.
    """
    env = os.environ.get("ALMA_CONFIG_DIR")
    if env:
        return Path(env)
    return Path.home() / ".alma"


@dataclass
class EnrichmentConfig:
    """Configuration for the pipeline analysis enrichment agent."""

    provider: str = "mock"  # anthropic | openai | mock
    model: str = "claude-sonnet-4-20250514"
    api_key_env: str = "ANTHROPIC_API_KEY"  # env var name containing the key
    timeout: int = 120
    max_tokens: int = 4096


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
        redacted = {
            k: "***" if k in _SECRET_PARAM_KEYS else v
            for k, v in self.params.items()
        }
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
    enrichment: EnrichmentConfig = field(default_factory=EnrichmentConfig)

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
        return self.config_dir / "sources.json"

    @property
    def config_file(self) -> Path:
        return self.config_dir / "config.json"

    @property
    def sync_cursor_file(self) -> Path:
        return self.config_dir / "sync_cursor.json"

    def ensure_dir(self) -> None:
        """Create config directory if it does not exist."""
        self.config_dir.mkdir(parents=True, exist_ok=True)

    def load_team_config(self) -> None:
        """Load team settings from config.json into this instance."""
        if not self.config_file.exists():
            return
        data: dict = json.loads(self.config_file.read_text())
        team = data.get("team", {})
        self.team_server_url = team.get("server_url")
        self.team_api_key = team.get("api_key")
        self.team_id = team.get("team_id")

    def save_team_config(self) -> None:
        """Persist team settings to config.json."""
        self.ensure_dir()
        data: dict = {}
        if self.config_file.exists():
            data = json.loads(self.config_file.read_text())
        data["team"] = {
            "server_url": self.team_server_url,
            "api_key": self.team_api_key,
            "team_id": self.team_id,
        }
        self.config_file.write_text(json.dumps(data, indent=2))

    def load_sync_cursor(self) -> str | None:
        """Return the stored sync cursor timestamp, or None if none exists."""
        if not self.sync_cursor_file.exists():
            return None
        data: dict = json.loads(self.sync_cursor_file.read_text())
        return data.get("cursor")

    def save_sync_cursor(self, cursor: str) -> None:
        """Store the sync cursor timestamp."""
        self.ensure_dir()
        self.sync_cursor_file.write_text(json.dumps({"cursor": cursor}))

    def load_sources(self) -> list[SourceConfig]:
        """Load registered sources from disk."""
        if not self.sources_file.exists():
            return []
        raw: list[dict] = json.loads(self.sources_file.read_text())
        return [SourceConfig(**s) for s in raw]

    def save_sources(self, sources: list[SourceConfig]) -> None:
        """Persist registered sources to disk."""
        self.ensure_dir()
        self.sources_file.write_text(
            json.dumps([{"id": s.id, "kind": s.kind, "params": s.params} for s in sources], indent=2)
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
    if _config is None:
        default_dir = default_config_dir()
        yml_path = default_dir / "atlas.yml"
        _config = load_atlas_yml(yml_path) if yml_path.exists() else AtlasConfig()
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

    # Parse enrichment settings.
    enrichment = data.get("enrichment", {})
    if enrichment:
        cfg.enrichment = EnrichmentConfig(
            provider=enrichment.get("provider", "mock"),
            model=enrichment.get("model", "claude-sonnet-4-20250514"),
            api_key_env=enrichment.get("api_key_env", "ANTHROPIC_API_KEY"),
            timeout=int(enrichment.get("timeout", 120)),
            max_tokens=int(enrichment.get("max_tokens", 4096)),
        )

    return cfg
