"""MCP tool registrations for Alma Atlas.

Registers all Atlas tools with the MCP server. Each tool exposes a slice
of the Atlas graph to AI agents: asset search, lineage traversal, schema
lookup, and status summaries.

Tool catalogue:
    - atlas_search              Search for assets by name or keyword
    - atlas_get_asset           Get a specific asset by ID
    - atlas_lineage             Get upstream or downstream lineage for an asset
    - atlas_status              Summarise the current graph (counts by kind)
    - atlas_get_schema          Get the latest schema snapshot for an asset
    - atlas_impact              Analyse downstream impact of changes to an asset
    - atlas_get_query_patterns  Show top SQL query patterns by execution count
    - atlas_suggest_tables      Suggest relevant tables for a search query
    - atlas_check_contract      Validate schema against data contract spec
    - atlas_list_violations     List recent enforcement violations from the store
    - atlas_team_sync           Trigger a team graph sync programmatically
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
                description="Analyse the downstream impact of changes to an asset — shows all assets that depend on it, with query exposure and blast radius.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "asset_id": {"type": "string", "description": "Asset ID to analyse impact for"},
                        "depth": {"type": "integer", "description": "Maximum depth of impact analysis"},
                    },
                    "required": ["asset_id"],
                },
            ),
            Tool(
                name="atlas_get_query_patterns",
                description="Show the top SQL query patterns observed in Atlas, grouped by fingerprint with execution counts and referenced tables.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "top_n": {"type": "integer", "description": "Number of top patterns to return", "default": 20},
                    },
                },
            ),
            Tool(
                name="atlas_suggest_tables",
                description="Suggest relevant data tables for a search query, ranked by name relevance and column overlap.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query describing the data you need"},
                        "limit": {"type": "integer", "description": "Maximum number of suggestions to return", "default": 10},
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="atlas_check_contract",
                description="Validate the current schema snapshot for an asset against its data contract spec, reporting any violations.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "asset_id": {"type": "string", "description": "Asset ID to check contracts for"},
                    },
                    "required": ["asset_id"],
                },
            ),
            Tool(
                name="atlas_list_violations",
                description="List recent enforcement violations stored in Atlas. Optionally filter by asset ID.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "asset_id": {"type": "string", "description": "Filter violations to a specific asset ID (omit for all assets)"},
                        "limit": {"type": "integer", "description": "Maximum number of violations to return", "default": 50},
                    },
                },
            ),
            Tool(
                name="atlas_team_sync",
                description="Trigger a team graph sync — push local Atlas changes to the team server and pull team contracts. Requires team sync to be configured via `alma-atlas team init`.",
                inputSchema={"type": "object", "properties": {}},
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

        if name == "atlas_get_query_patterns":
            return _handle_get_query_patterns(cfg, arguments)

        if name == "atlas_suggest_tables":
            return _handle_suggest_tables(cfg, arguments)

        if name == "atlas_check_contract":
            return _handle_check_contract(cfg, arguments)

        if name == "atlas_list_violations":
            return _handle_list_violations(cfg, arguments)

        if name == "atlas_team_sync":
            return await _handle_team_sync(cfg)

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
    from alma_atlas_store.query_repository import QueryRepository

    asset_id = arguments["asset_id"]
    depth = arguments.get("depth")

    with Database(cfg.db_path) as db:
        raw_edges = EdgeRepository(db).list_all()
        asset = AssetRepository(db).get(asset_id)
        all_queries = QueryRepository(db).list_all()

    if asset is None:
        return [TextContent(type="text", text=f"Asset not found: {asset_id}")]

    edges = [Edge(upstream_id=e.upstream_id, downstream_id=e.downstream_id, kind=e.kind) for e in raw_edges]
    graph = compute_lineage(edges)

    if not graph.has_asset(asset_id):
        return [TextContent(type="text", text=f"Asset not found in lineage graph: {asset_id}")]

    downstream = graph.downstream(asset_id, depth=depth)

    if not downstream:
        return [
            TextContent(
                type="text",
                text=f"No downstream dependencies found for {asset_id}. Changes to this asset have no detected downstream impact.",
            )
        ]

    # Count query executions touching each downstream asset
    query_counts: dict[str, int] = {}
    for q in all_queries:
        for table in q.tables:
            if table in downstream:
                query_counts[table] = query_counts.get(table, 0) + q.execution_count

    blast_radius = len(downstream)
    total_query_exposure = sum(query_counts.values())

    lines = [
        f"Impact analysis for {asset_id}:",
        f"  Blast radius: {blast_radius} downstream asset(s)",
        f"  Query exposure: {total_query_exposure} query execution(s) across affected assets\n",
    ]
    for node_id in downstream:
        qcount = query_counts.get(node_id, 0)
        qinfo = f"  ({qcount} query exec(s))" if qcount else ""
        lines.append(f"  ⚠ {node_id}{qinfo}")

    lines.append(
        f"\nRecommendation: Review these {blast_radius} downstream assets before making changes to {asset_id}."
    )
    return [TextContent(type="text", text="\n".join(lines))]


def _handle_get_query_patterns(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.db import Database
    from alma_atlas_store.query_repository import QueryRepository

    top_n = arguments.get("top_n", 20)
    with Database(cfg.db_path) as db:
        queries = QueryRepository(db).list_all()  # already sorted by execution_count DESC

    queries = queries[:top_n]
    if not queries:
        return [TextContent(type="text", text="No query patterns found.")]

    lines = [f"Top {len(queries)} query pattern(s) by execution count:\n"]
    for i, q in enumerate(queries, 1):
        tables_str = ", ".join(q.tables) if q.tables else "(none)"
        sql_preview = q.sql_text[:120].replace("\n", " ") + ("..." if len(q.sql_text) > 120 else "")
        lines.append(f"  {i}. fingerprint={q.fingerprint}  executions={q.execution_count}")
        lines.append(f"     tables: {tables_str}")
        lines.append(f"     source: {q.source}")
        lines.append(f"     sql: {sql_preview}")
        lines.append("")

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_suggest_tables(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.schema_repository import SchemaRepository

    query = arguments["query"]
    limit = arguments.get("limit", 10)
    query_tokens = {t.lower() for t in query.split() if t}

    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).search(query)
        schema_repo = SchemaRepository(db)
        results = []
        for asset in assets:
            snapshot = schema_repo.get_latest(asset.id)
            col_names: set[str] = set()
            if snapshot:
                col_names = {c.name.lower() for c in snapshot.columns}
            elif "columns" in asset.metadata:
                col_names = {c.get("name", "").lower() for c in asset.metadata["columns"]}

            # Jaccard overlap between query tokens and column names
            if col_names and query_tokens:
                union = query_tokens | col_names
                jaccard = len(query_tokens & col_names) / len(union)
            else:
                jaccard = 0.0

            name_match = 1.0 if query.lower() in asset.name.lower() else 0.0
            score = 0.5 * name_match + 0.5 * jaccard
            results.append((score, asset, col_names))

    results.sort(key=lambda x: -x[0])
    results = results[:limit]

    if not results:
        return [TextContent(type="text", text=f"No table suggestions found for {query!r}.")]

    lines = [f"Table suggestions for {query!r} ({len(results)} result(s)):\n"]
    for score, asset, col_names in results:
        cols_preview = ", ".join(sorted(col_names)[:5]) + ("..." if len(col_names) > 5 else "")
        lines.append(f"  {asset.id}  [{asset.kind}]  relevance={score:.2f}")
        if col_names:
            lines.append(f"    columns: {cols_preview}")
    return [TextContent(type="text", text="\n".join(lines))]


def _handle_list_violations(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.db import Database
    from alma_atlas_store.violation_repository import ViolationRepository

    asset_id = arguments.get("asset_id")
    limit = arguments.get("limit", 50)

    with Database(cfg.db_path) as db:
        repo = ViolationRepository(db)
        violations = repo.list_for_asset(asset_id)[:limit] if asset_id else repo.list_recent(limit=limit)

    if not violations:
        msg = f"No open violations found for {asset_id!r}." if asset_id else "No open violations found."
        return [TextContent(type="text", text=msg)]

    lines = [f"{len(violations)} open violation(s):\n"]
    for v in violations:
        resolved = " [resolved]" if v.resolved_at else ""
        lines.append(
            f"  [{v.severity}] {v.asset_id}  {v.violation_type}{resolved}"
            f"\n    {v.details.get('message', json.dumps(v.details))}"
            f"\n    detected: {v.detected_at}"
        )
    return [TextContent(type="text", text="\n".join(lines))]


async def _handle_team_sync(cfg: AtlasConfig) -> list[TextContent]:
    cfg.load_team_config()
    if not cfg.team_server_url or not cfg.team_api_key:
        return [TextContent(type="text", text="Team sync not configured. Run `alma-atlas team init` first.")]
    if not cfg.db_path or not cfg.db_path.exists():
        return [TextContent(type="text", text="No Atlas database found. Run `alma-atlas scan` first.")]

    from alma_atlas.sync.auth import TeamAuth
    from alma_atlas.sync.client import SyncClient
    from alma_atlas_store.db import Database

    auth = TeamAuth(cfg.team_api_key)
    try:
        async with SyncClient(cfg.team_server_url, auth, cfg.team_id or "default") as client:
            with Database(cfg.db_path) as db:
                response = await client.full_sync(db, cfg)
        return [
            TextContent(
                type="text",
                text=f"Team sync complete: {response.accepted_count} record(s) accepted, {len(response.rejected)} rejected.",
            )
        ]
    except Exception as exc:
        return [TextContent(type="text", text=f"Team sync failed: {exc}")]


def _handle_check_contract(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_id = arguments["asset_id"]
    with Database(cfg.db_path) as db:
        contracts = ContractRepository(db).list_for_asset(asset_id)
        if not contracts:
            return [TextContent(type="text", text=f"No contracts found for asset: {asset_id}")]
        snapshot = SchemaRepository(db).get_latest(asset_id)

    violations: list[str] = []
    for contract in contracts:
        spec_columns = contract.spec.get("columns", [])
        if not spec_columns:
            continue

        if snapshot is None:
            violations.append(
                f"[{contract.id}] No schema snapshot available to validate against contract v{contract.version}"
            )
            continue

        actual_cols = {c.name.lower(): c for c in snapshot.columns}
        for spec_col in spec_columns:
            col_name = spec_col.get("name", "")
            if not col_name:
                continue
            actual = actual_cols.get(col_name.lower())
            if actual is None:
                violations.append(f"[{contract.id}] Missing column: {col_name!r}")
                continue
            spec_type = spec_col.get("type", "")
            if spec_type and actual.type.lower() != spec_type.lower():
                violations.append(
                    f"[{contract.id}] Type mismatch for {col_name!r}: expected {spec_type!r}, got {actual.type!r}"
                )
            spec_nullable = spec_col.get("nullable", True)
            if not spec_nullable and actual.nullable:
                violations.append(
                    f"[{contract.id}] Nullability violation for {col_name!r}: contract requires NOT NULL but column is nullable"
                )

    if not violations:
        lines = [
            f"Contract check PASSED for {asset_id}",
            f"  {len(contracts)} contract(s) validated, no violations found.",
        ]
    else:
        lines = [f"Contract check FAILED for {asset_id}: {len(violations)} violation(s)\n"]
        for v in violations:
            lines.append(f"  ✗ {v}")

    return [TextContent(type="text", text="\n".join(lines))]
