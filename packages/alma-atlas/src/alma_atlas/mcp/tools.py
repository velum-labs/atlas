"""MCP tool registration entry point for Alma Atlas.

Aggregates tool specs and handlers from category modules and exposes the
canonical `register(server, cfg)` consumed by `mcp/server.py`.

Two registration profiles:

- **Default (`alma-atlas serve`)**: registers the 20 atlas_* tools across
  `tools_search`, `tools_schema`, `tools_lineage`, `tools_contracts`,
  `tools_meta`, `tools_agent`. No auth.

- **Atlas Companion (`alma-atlas serve --alma-token <invite>`)**: registers
  only the 3 companion_* tools from `tools_companion` and gates every
  `call_tool` invocation behind a token validator that hits the Alma
  deployment endpoint per the eng review's Issue 3A (no caching, instant
  revocation).

The two profiles never coexist on the same server instance — a Companion
mode session exposes ONLY the curated 3-tool surface.

Handler implementations live in category modules. Import them from there:

    from alma_atlas.mcp.tools_search import _handle_search
    from alma_atlas.mcp.tools_schema import _handle_get_asset
    from alma_atlas.mcp.tools_lineage import _handle_lineage
    from alma_atlas.mcp.tools_contracts import _handle_check_contract, _dispatch_verify
    from alma_atlas.mcp.tools_meta import _handle_status, _handle_team_sync
    from alma_atlas.mcp.tools_agent import _handle_context, _handle_ask
    from alma_atlas.mcp.tools_companion import _handle_companion_search, ...
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable
from typing import Any

from mcp.server import Server
from mcp.types import TextContent, Tool

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp import (
    tools_agent,
    tools_companion,
    tools_contracts,
    tools_lineage,
    tools_meta,
    tools_schema,
    tools_search,
)
from alma_atlas.mcp._common import AtlasToolSpec

__all__ = [
    "ATLAS_CATEGORY_MODULES",
    "AtlasToolSpec",
    "COMPANION_CATEGORY_MODULES",
    "TokenValidator",
    "register",
]

# Type alias for the per-call token validator. Returns an object whose `.valid`
# is bool and whose `.display_message` is the agent-facing error string. The
# dispatcher only checks `validation.valid`; on False the message is returned
# verbatim to the agent.
TokenValidator = Callable[[], Any]

# Default registration: the full 20-tool atlas_* surface.
ATLAS_CATEGORY_MODULES: tuple = (
    tools_search,
    tools_schema,
    tools_lineage,
    tools_contracts,
    tools_meta,
    tools_agent,
)

# Companion mode: the curated 3-tool surface, gated by token validation.
COMPANION_CATEGORY_MODULES: tuple = (tools_companion,)


def _tool_specs(modules: Iterable | None = None) -> tuple[AtlasToolSpec, ...]:
    """Aggregate tool specs from the given category modules.

    Args:
        modules: Iterable of category modules. Defaults to the atlas_* surface.
    """
    selected = ATLAS_CATEGORY_MODULES if modules is None else tuple(modules)
    aggregated: list[AtlasToolSpec] = []
    for module in selected:
        aggregated.extend(module.specs())
    return tuple(aggregated)


def _tool_handlers(modules: Iterable | None = None) -> dict[str, Any]:
    """Aggregate the dispatch dict from the given category modules.

    Args:
        modules: Iterable of category modules. Defaults to the atlas_* surface.
    """
    selected = ATLAS_CATEGORY_MODULES if modules is None else tuple(modules)
    aggregated: dict[str, Any] = {}
    for module in selected:
        aggregated.update(module.handlers())
    return aggregated


def register(
    server: Server,
    cfg: AtlasConfig,
    *,
    modules: Iterable | None = None,
    token_validator: TokenValidator | None = None,
) -> None:
    """Register Atlas tools on an MCP server.

    Args:
        server: MCP Server instance.
        cfg: Atlas runtime configuration.
        modules: Category modules to register. Defaults to the 20-tool atlas_*
            surface (`ATLAS_CATEGORY_MODULES`). Pass `COMPANION_CATEGORY_MODULES`
            to register only the 3 companion_* tools.
        token_validator: Optional zero-arg callable returning a validation
            result. When provided, every `call_tool` invocation calls the
            validator first; on `validation.valid is False`, the validator's
            `display_message` is returned to the agent and the handler is NOT
            invoked. Per eng review Issue 3A (no caching).
    """
    selected_modules = ATLAS_CATEGORY_MODULES if modules is None else tuple(modules)

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [spec.to_tool() for spec in _tool_specs(selected_modules)]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        if token_validator is not None:
            validation = token_validator()
            if not validation.valid:
                return [TextContent(type="text", text=validation.display_message)]

        from alma_atlas.application.query.service import require_db_path

        try:
            require_db_path(cfg)
        except ValueError as exc:
            return [TextContent(type="text", text=str(exc))]

        handler = _tool_handlers(selected_modules).get(name)
        if handler is None:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
        result = handler(cfg, arguments)
        if inspect.isawaitable(result):
            return await result
        return result
