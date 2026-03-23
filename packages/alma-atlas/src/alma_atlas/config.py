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
class SourceConfig:
    """Configuration for a registered data source."""

    id: str
    kind: str  # bigquery, snowflake, postgres, dbt
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class AtlasConfig:
    """Top-level Alma Atlas configuration."""

    config_dir: Path = field(default_factory=default_config_dir)
    sources: list[SourceConfig] = field(default_factory=list)
    db_path: Path | None = None

    def __post_init__(self) -> None:
        if self.db_path is None:
            self.db_path = self.config_dir / "atlas.db"

    @property
    def sources_file(self) -> Path:
        return self.config_dir / "sources.json"

    def ensure_dir(self) -> None:
        """Create config directory if it does not exist."""
        self.config_dir.mkdir(parents=True, exist_ok=True)

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
    """Return the global AtlasConfig singleton."""
    global _config
    if _config is None:
        _config = AtlasConfig()
    return _config
