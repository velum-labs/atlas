"""MCP tool registration entry point for Alma Atlas.

Aggregates tool specs and handlers from category modules and exposes the
canonical `register(server, cfg)` consumed by `mcp/server.py`.

Handler implementations live in category modules. Import them from there:

    from alma_atlas.mcp.tools_search import _handle_search
    from alma_atlas.mcp.tools_schema import _handle_get_asset
    from alma_atlas.mcp.tools_lineage import _handle_lineage
    from alma_atlas.mcp.tools_contracts import _handle_check_contract, _dispatch_verify
    from alma_atlas.mcp.tools_meta import _handle_status, _handle_team_sync
    from alma_atlas.mcp.tools_agent import _handle_context, _handle_ask

Category modules:
    - tools_search       atlas_search, atlas_suggest_tables, atlas_find_term
    - tools_schema       atlas_get_asset, atlas_get_annotations, atlas_get_schema,
                         atlas_explain_column, atlas_profile_column
    - tools_lineage      atlas_lineage, atlas_impact, atlas_describe_relationship
    - tools_contracts    atlas_check_contract, atlas_list_violations,
                         atlas_verify (sync+deep), atlas_define_term
    - tools_meta         atlas_status, atlas_get_query_patterns, atlas_team_sync
    - tools_agent        atlas_context, atlas_ask
"""

from __future__ import annotations

import inspect
from typing import Any

from mcp.server import Server
from mcp.types import TextContent, Tool

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp import (
    tools_agent,
    tools_contracts,
    tools_lineage,
    tools_meta,
    tools_schema,
    tools_search,
)
from alma_atlas.mcp._common import AtlasToolSpec

__all__ = ["AtlasToolSpec", "register"]


_CATEGORY_MODULES = (
    tools_search,
    tools_schema,
    tools_lineage,
    tools_contracts,
    tools_meta,
    tools_agent,
)


def _tool_specs() -> tuple[AtlasToolSpec, ...]:
    """Aggregate the canonical Atlas MCP tool catalog from every category module."""
    aggregated: list[AtlasToolSpec] = []
    for module in _CATEGORY_MODULES:
        aggregated.extend(module.specs())
    return tuple(aggregated)


def _tool_handlers() -> dict[str, Any]:
    """Aggregate the dispatch dict from every category module."""
    aggregated: dict[str, Any] = {}
    for module in _CATEGORY_MODULES:
        aggregated.update(module.handlers())
    return aggregated


def register(server: Server, cfg: AtlasConfig) -> None:
    """Register all Atlas tools on the given MCP server instance."""

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [spec.to_tool() for spec in _tool_specs()]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        from alma_atlas.application.query.service import require_db_path

        try:
            require_db_path(cfg)
        except ValueError as exc:
            return [TextContent(type="text", text=str(exc))]

        handler = _tool_handlers().get(name)
        if handler is None:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
        result = handler(cfg, arguments)
        if inspect.isawaitable(result):
            return await result
        return result
