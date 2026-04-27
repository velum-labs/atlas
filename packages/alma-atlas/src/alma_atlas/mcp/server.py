"""MCP server factory for Alma Atlas.

Creates and configures the MCP server instance with the right tool surface
registered. The server is transport-agnostic — the CLI's ``serve`` command
attaches the chosen transport (stdio or SSE).

Two registration profiles, controlled by the `modules` and `token_validator`
arguments:

- Default: 20 atlas_* tools, no auth.
- Companion mode: 3 companion_* tools, gated by per-call token validation.
"""

from __future__ import annotations

from collections.abc import Iterable

from mcp.server import Server

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp import tools
from alma_atlas.telemetry import TelemetryConfig


def create_server(
    cfg: AtlasConfig,
    *,
    modules: Iterable | None = None,
    token_validator: tools.TokenValidator | None = None,
    telemetry_cfg: TelemetryConfig | None = None,
    install_source: str | None = None,
) -> Server:
    """Create and configure the Alma Atlas MCP server.

    Args:
        cfg: Atlas configuration (used to open the SQLite store).
        modules: Tool category modules to register. Defaults to the 20-tool
            atlas_* surface (`tools.ATLAS_CATEGORY_MODULES`). Pass
            `tools.COMPANION_CATEGORY_MODULES` to register only the 3
            companion_* tools.
        token_validator: Optional zero-arg callable for per-call invite-token
            validation. Required when `modules == tools.COMPANION_CATEGORY_MODULES`
            (Companion mode is gated by token); ignored if no validator is
            configured.
        telemetry_cfg: Optional TelemetryConfig. When provided, the dispatcher
            fires a `tool_call` mandatory event after each handler completes.
            Required for Premise 5 success metric measurement.
        install_source: Optional acquisition-channel tag attached to telemetry
            events ("concierge_invite", "direct_pip", ...).

    Returns:
        Configured MCP Server instance ready to be wired to a transport.
    """
    server = Server("alma-atlas")

    tools.register(
        server,
        cfg,
        modules=modules,
        token_validator=token_validator,
        telemetry_cfg=telemetry_cfg,
        install_source=install_source,
    )

    return server
