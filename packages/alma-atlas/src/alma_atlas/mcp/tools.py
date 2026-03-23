"""MCP tool registrations for Alma Atlas.

Registers all Atlas tools with the MCP server. Each tool exposes a slice
of the Atlas graph to AI agents: asset search, lineage traversal, schema
lookup, and status summaries.

Tool catalogue:
    - atlas_search        Search for assets by name or keyword
    - atlas_get_asset     Get a specific asset by ID
    - atlas_lineage       Get upstream or downstream lineage for an asset
    - atlas_status        Summarise the current graph (counts by kind)
    - atlas_get_schema    Get the latest schema snapshot for an asset
    - atlas_impact        Analyse downstream impact of changes to an asset
"""

from __future__ import annotations

import json
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
            Tool(
                name="atlas_get_schema",
                description="Get the latest schema snapshot for a data asset (columns, types, nullability).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "asset_id": {"type": "string", "description": "Asset ID to get schema for"},
                    },
                    "required": ["asset_id"],
                },
            ),
            Tool(
                name="atlas_impact",
                description="Analyse the downstream impact of changes to an asset — shows all assets that depend on it.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "asset_id": {"type": "string", "description": "Asset ID to analyse impact for"},
                        "depth": {"type": "integer", "description": "Maximum depth of impact analysis"},
                    },
                    "required": ["asset_id"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        if not cfg.db_path or not cfg.db_path.exists():
            return [TextContent(type="text", text="No Atlas database found. Run `alma-atlas scan` first.")]


        if name == "atlas_search":
            return _handle_search(cfg, arguments)

        if name == "atlas_get_asset":
            return _handle_get_asset(cfg, arguments)

        if name == "atlas_lineage":
            return _handle_lineage(cfg, arguments)

        if name == "atlas_status":
            return _handle_status(cfg)

        if name == "atlas_get_schema":
            return _handle_get_schema(cfg, arguments)

        if name == "atlas_impact":
            return _handle_impact(cfg, arguments)

        return [TextContent(type="text", text=f"Unknown tool: {name}")]


def _handle_search(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    query = arguments["query"]
    limit = arguments.get("limit", 20)
    with Database(cfg.db_path) as db:
        results = AssetRepository(db).search(query)[:limit]
    if not results:
        return [TextContent(type="text", text=f"No assets found matching {query!r}.")]
    lines = [f"Found {len(results)} asset(s) matching {query!r}:\n"]
    for a in results:
        desc = f"  {a.description}" if a.description else ""
        lines.append(f"  {a.id}  [{a.kind}]  source={a.source}{desc}")
    return [TextContent(type="text", text="\n".join(lines))]


def _handle_get_asset(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    asset_id = arguments["asset_id"]
    with Database(cfg.db_path) as db:
        asset = AssetRepository(db).get(asset_id)
    if asset is None:
        return [TextContent(type="text", text=f"Asset not found: {asset_id}")]
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


def _handle_lineage(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_analysis.lineage import Edge, compute_lineage
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    asset_id = arguments["asset_id"]
    direction = arguments["direction"]
    depth = arguments.get("depth")

    with Database(cfg.db_path) as db:
        raw_edges = EdgeRepository(db).list_all()

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


def _handle_status(cfg: AtlasConfig) -> list[TextContent]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.query_repository import QueryRepository

    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).list_all()
        edges = EdgeRepository(db).list_all()
        queries = QueryRepository(db).list_all()

    kind_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for a in assets:
        kind_counts[a.kind] = kind_counts.get(a.kind, 0) + 1
        source_counts[a.source] = source_counts.get(a.source, 0) + 1

    lines = [
        f"Atlas graph: {len(assets)} assets, {len(edges)} edges, {len(queries)} query fingerprints",
        "",
        "Assets by kind:",
    ]
    for kind, count in sorted(kind_counts.items()):
        lines.append(f"  {kind}: {count}")

    if source_counts:
        lines.append("")
        lines.append("Assets by source:")
        for source, count in sorted(source_counts.items()):
            lines.append(f"  {source}: {count}")

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_get_schema(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_id = arguments["asset_id"]
    with Database(cfg.db_path) as db:
        asset = AssetRepository(db).get(asset_id)
        if asset is None:
            return [TextContent(type="text", text=f"Asset not found: {asset_id}")]

        schema_repo = SchemaRepository(db)
        snapshot = schema_repo.get_latest(asset_id)

    if snapshot is None:
        # Fall back to asset metadata if it contains column info
        columns = asset.metadata.get("columns", [])
        if columns:
            lines = [f"Schema for {asset_id} (from asset metadata):\n"]
            for col in columns:
                nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
                lines.append(f"  {col['name']}  {col.get('type', 'unknown')}  {nullable}")
            return [TextContent(type="text", text="\n".join(lines))]
        return [TextContent(type="text", text=f"No schema snapshot found for {asset_id}.")]

    if isinstance(snapshot.columns, str):
        col_list = json.loads(snapshot.columns)
    else:
        col_list = [vars(c) if hasattr(c, "__dict__") else c for c in snapshot.columns]
    lines = [f"Schema for {asset_id} (captured {snapshot.captured_at}):\n"]
    for col in col_list:
        name = col.get("name", "?")
        dtype = col.get("type", "unknown")
        nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
        desc = f"  -- {col['description']}" if col.get("description") else ""
        lines.append(f"  {name}  {dtype}  {nullable}{desc}")

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_impact(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_analysis.lineage import Edge, compute_lineage
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    asset_id = arguments["asset_id"]
    depth = arguments.get("depth")

    with Database(cfg.db_path) as db:
        raw_edges = EdgeRepository(db).list_all()
        asset_repo = AssetRepository(db)
        asset = asset_repo.get(asset_id)

    if asset is None:
        return [TextContent(type="text", text=f"Asset not found: {asset_id}")]

    edges = [Edge(upstream_id=e.upstream_id, downstream_id=e.downstream_id, kind=e.kind) for e in raw_edges]
    graph = compute_lineage(edges)

    if not graph.has_asset(asset_id):
        return [TextContent(type="text", text=f"Asset not found in lineage graph: {asset_id}")]

    downstream = graph.downstream(asset_id, depth=depth)

    if not downstream:
        return [TextContent(type="text", text=f"No downstream dependencies found for {asset_id}. Changes to this asset have no detected downstream impact.")]

    lines = [
        f"Impact analysis for {asset_id}:",
        f"  {len(downstream)} downstream asset(s) would be affected by changes:\n",
    ]
    for node_id in downstream:
        lines.append(f"  ⚠ {node_id}")

    lines.append(f"\nRecommendation: Review these {len(downstream)} downstream assets before making changes to {asset_id}.")
    return [TextContent(type="text", text="\n".join(lines))]
