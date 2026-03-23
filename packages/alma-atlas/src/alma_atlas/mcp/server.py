"""MCP server factory for Alma Atlas.

Creates and configures the MCP server instance with all Atlas tools
registered. The server is transport-agnostic — the CLI's ``serve``
command attaches the chosen transport (stdio or SSE).
"""

from __future__ import annotations

from mcp.server import Server

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp import tools


def create_server(cfg: AtlasConfig) -> Server:
    """Create and configure the Alma Atlas MCP server.

    Registers all Atlas tool handlers and returns a configured MCP Server
    instance ready to be wired to a transport.

    Args:
        cfg: Atlas configuration (used to open the SQLite store).

    Returns:
        Configured MCP Server instance.
    """
    server = Server("alma-atlas")

    tools.register(server, cfg)

    return server
