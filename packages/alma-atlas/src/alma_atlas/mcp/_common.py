"""Shared primitives for MCP tool category modules.

Lives outside `tools.py` so category modules (`tools_search`, `tools_schema`,
etc.) can import the spec dataclass and the database-path helper without
creating a circular import with `tools.py` (which aggregates them).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.types import Tool

from alma_atlas.config import AtlasConfig


@dataclass(frozen=True)
class AtlasToolSpec:
    """Declarative description of one Atlas MCP tool."""

    name: str
    description: str
    input_schema: dict[str, Any]

    def to_tool(self) -> Tool:
        """Convert this spec into the MCP SDK's `Tool` payload."""
        return Tool(name=self.name, description=self.description, inputSchema=self.input_schema)


def _db_path(cfg: AtlasConfig) -> Path:
    """Resolve the configured Atlas SQLite database path."""
    from alma_atlas.application.query.service import require_db_path

    return require_db_path(cfg)
