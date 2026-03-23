"""MCP tool registrations for Alma Atlas.

Registers all Atlas tools with the MCP server. Each tool exposes a slice
of the Atlas graph to AI agents: asset search, lineage traversal, schema
lookup, and status summaries.

Tool catalogue:
    - atlas_search        Search for assets by name or keyword
    - atlas_get_asset     Get a specific asset by ID
    - atlas_lineage       Get upstream or downstream lineage for an asset
    - atlas_status        Summarise the current graph (counts by kind)
"""

from __future__ import annotations

from typing import Any

from mcp.server import Server
from mcp.types import TextContent, Tool

from alma_atlas.config import AtlasConfig


def register(server: Server, cfg: AtlasConfig) -> None:
    """Register all Atlas tools on the given MCP server instance."""

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="atlas_search",
                description="Search for data assets in the Atlas graph by name, ID, or keyword.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search term"},
                        "limit": {"type": "integer", "description": "Maximum number of results", "default": 20},
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="atlas_get_asset",
                description="Retrieve full details for a specific data asset by its ID.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "asset_id": {"type": "string", "description": "Fully-qualified asset ID"},
                    },
                    "required": ["asset_id"],
                },
            ),
            Tool(
                name="atlas_lineage",
                description="Trace upstream or downstream lineage for a data asset.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "asset_id": {"type": "string", "description": "Asset ID to trace from"},
                        "direction": {
                            "type": "string",
                            "enum": ["upstream", "downstream"],
                            "description": "Direction of traversal",
                        },
                        "depth": {"type": "integer", "description": "Maximum traversal depth (omit for unlimited)"},
                    },
                    "required": ["asset_id", "direction"],
                },
            ),
            Tool(
                name="atlas_status",
                description="Return a summary of the Atlas graph: total assets, edges, and asset counts by kind.",
                inputSchema={"type": "object", "properties": {}},
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        if not cfg.db_path or not cfg.db_path.exists():
            return [TextContent(type="text", text="No Atlas database found. Run `alma-atlas scan` first.")]

        from alma_atlas_store.asset_repository import AssetRepository
        from alma_atlas_store.db import Database
        from alma_atlas_store.edge_repository import EdgeRepository

        if name == "atlas_search":
            query = arguments["query"]
            limit = arguments.get("limit", 20)
            with Database(cfg.db_path) as db:
                results = AssetRepository(db).search(query)[:limit]
            if not results:
                return [TextContent(type="text", text=f"No assets found matching {query!r}.")]
            lines = [f"Found {len(results)} asset(s) matching {query!r}:\n"]
            for a in results:
                lines.append(f"  {a.id}  [{a.kind}]  source={a.source}")
            return [TextContent(type="text", text="\n".join(lines))]

        if name == "atlas_get_asset":
            asset_id = arguments["asset_id"]
            with Database(cfg.db_path) as db:
                asset = AssetRepository(db).get(asset_id)
            if asset is None:
                return [TextContent(type="text", text=f"Asset not found: {asset_id}")]
            import json

            return [
                TextContent(
                    type="text",
                    text=json.dumps(
                        {
                            "id": asset.id,
                            "source": asset.source,
                            "kind": asset.kind,
                            "name": asset.name,
                            "description": asset.description,
                            "tags": asset.tags,
                            "metadata": asset.metadata,
                            "first_seen": asset.first_seen,
                            "last_seen": asset.last_seen,
                        },
                        indent=2,
                    ),
                )
            ]

        if name == "atlas_lineage":
            asset_id = arguments["asset_id"]
            direction = arguments["direction"]
            depth = arguments.get("depth")
            with Database(cfg.db_path) as db:
                raw_edges = EdgeRepository(db).list_all()
            from alma_analysis.edges import Edge
            from alma_analysis.lineage import compute_lineage

            edges = [Edge(upstream_id=e.upstream_id, downstream_id=e.downstream_id, kind=e.kind) for e in raw_edges]
            graph = compute_lineage(edges)
            if not graph.has_asset(asset_id):
                return [TextContent(type="text", text=f"Asset not found in lineage graph: {asset_id}")]
            if direction == "upstream":
                related = graph.upstream(asset_id, depth=depth)
            else:
                related = graph.downstream(asset_id, depth=depth)
            if not related:
                return [TextContent(type="text", text=f"No {direction} assets found for {asset_id}.")]
            lines = [f"{direction.capitalize()} lineage for {asset_id} ({len(related)} nodes):"]
            for node in related:
                lines.append(f"  {node}")
            return [TextContent(type="text", text="\n".join(lines))]

        if name == "atlas_status":
            with Database(cfg.db_path) as db:
                assets = AssetRepository(db).list_all()
                edges = EdgeRepository(db).list_all()
            kind_counts: dict[str, int] = {}
            for a in assets:
                kind_counts[a.kind] = kind_counts.get(a.kind, 0) + 1
            lines = [
                f"Atlas graph: {len(assets)} assets, {len(edges)} edges",
                "",
                "Assets by kind:",
            ]
            for kind, count in sorted(kind_counts.items()):
                lines.append(f"  {kind}: {count}")
            return [TextContent(type="text", text="\n".join(lines))]

        return [TextContent(type="text", text=f"Unknown tool: {name}")]
