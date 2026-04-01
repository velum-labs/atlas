"""MCP tool registrations for Alma Atlas.

Registers all Atlas tools with the MCP server. Each tool exposes a slice
of the Atlas graph to AI agents: asset search, lineage traversal, schema
lookup, and status summaries.

Tool catalogue:
    - atlas_search              Search for assets by name or keyword
    - atlas_get_asset           Get a specific asset by ID
    - atlas_get_annotations     Get agent-generated asset annotations
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

import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.types import TextContent, Tool

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


def _tool_specs() -> tuple[AtlasToolSpec, ...]:
    """Return the canonical Atlas MCP tool catalog."""
    return (
        AtlasToolSpec(
            name="atlas_search",
            description="Search for data assets in the Atlas graph by name, ID, or keyword.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search term"},
                    "limit": {"type": "integer", "description": "Maximum number of results", "default": 20},
                },
                "required": ["query"],
            },
        ),
        AtlasToolSpec(
            name="atlas_get_asset",
            description="Retrieve full details for a specific data asset by its ID.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Fully-qualified asset ID"},
                },
                "required": ["asset_id"],
            },
        ),
        AtlasToolSpec(
            name="atlas_get_annotations",
            description="Retrieve agent-generated business metadata annotations for an asset (ownership, granularity, join keys, freshness).",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Optional asset ID to fetch annotations for"},
                    "limit": {"type": "integer", "description": "Max records when listing all", "default": 100},
                },
            },
        ),
        AtlasToolSpec(
            name="atlas_lineage",
            description="Trace upstream or downstream lineage for a data asset.",
            input_schema={
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
        AtlasToolSpec(
            name="atlas_status",
            description="Return a summary of the Atlas graph: total assets, edges, and asset counts by kind.",
            input_schema={"type": "object", "properties": {}},
        ),
        AtlasToolSpec(
            name="atlas_get_schema",
            description="Get the latest schema snapshot for a data asset (columns, types, nullability).",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Asset ID to get schema for"},
                },
                "required": ["asset_id"],
            },
        ),
        AtlasToolSpec(
            name="atlas_impact",
            description="Analyse the downstream impact of changes to an asset — shows all assets that depend on it, with query exposure and blast radius.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Asset ID to analyse impact for"},
                    "depth": {"type": "integer", "description": "Maximum depth of impact analysis"},
                },
                "required": ["asset_id"],
            },
        ),
        AtlasToolSpec(
            name="atlas_get_query_patterns",
            description="Show the top SQL query patterns observed in Atlas, grouped by fingerprint with execution counts and referenced tables.",
            input_schema={
                "type": "object",
                "properties": {
                    "top_n": {"type": "integer", "description": "Number of top patterns to return", "default": 20},
                },
            },
        ),
        AtlasToolSpec(
            name="atlas_suggest_tables",
            description="Suggest relevant data tables for a search query, ranked by name relevance and column overlap.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query describing the data you need"},
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of suggestions to return",
                        "default": 10,
                    },
                },
                "required": ["query"],
            },
        ),
        AtlasToolSpec(
            name="atlas_check_contract",
            description="Validate the current schema snapshot for an asset against its data contract spec, reporting any violations.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Asset ID to check contracts for"},
                },
                "required": ["asset_id"],
            },
        ),
        AtlasToolSpec(
            name="atlas_list_violations",
            description="List recent enforcement violations stored in Atlas. Optionally filter by asset ID.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {
                        "type": "string",
                        "description": "Filter violations to a specific asset ID (omit for all assets)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of violations to return",
                        "default": 50,
                    },
                },
            },
        ),
        AtlasToolSpec(
            name="atlas_team_sync",
            description="Trigger a team graph sync — push local Atlas changes to the team server and pull team contracts. Requires team sync to be configured via `alma-atlas team init`.",
            input_schema={"type": "object", "properties": {}},
        ),
    )


def _db_path(cfg: AtlasConfig) -> Path:
    """Resolve the configured Atlas SQLite database path."""
    from alma_atlas.application.query.service import require_db_path

    return require_db_path(cfg)


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


def _handle_search(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import search_assets

    query = arguments["query"]
    limit = arguments.get("limit", 20)
    results = search_assets(_db_path(cfg), query, limit=limit)
    if not results:
        return [TextContent(type="text", text=f"No assets found matching {query!r}.")]
    lines = [f"Found {len(results)} asset(s) matching {query!r}:\n"]
    for a in results:
        desc = f"  {a.description}" if a.description else ""
        lines.append(f"  {a.id}  [{a.kind}]  source={a.source}{desc}")
    return [TextContent(type="text", text="\n".join(lines))]


def _handle_get_asset(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import get_asset

    asset_id = arguments["asset_id"]
    asset = get_asset(_db_path(cfg), asset_id)
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


def _handle_get_annotations(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    """Return asset annotations from the store.

    If asset_id is provided, returns that annotation (or a not-found message).
    Otherwise returns up to `limit` most recent annotations.
    """
    from alma_atlas.application.query.service import get_annotations

    asset_id = arguments.get("asset_id")
    limit = int(arguments.get("limit", 100))
    records = get_annotations(_db_path(cfg), asset_id=asset_id, limit=limit)
    if asset_id and not records:
        return [TextContent(type="text", text=f"No annotation found for asset: {asset_id}")]
    payload = [
        {
            "asset_id": ann.asset_id,
            "ownership": ann.ownership,
            "granularity": ann.granularity,
            "join_keys": ann.join_keys,
            "freshness_guarantee": ann.freshness_guarantee,
            "business_logic_summary": ann.business_logic_summary,
            "sensitivity": ann.sensitivity,
            "annotated_at": ann.annotated_at,
            "annotated_by": ann.annotated_by,
        }
        for ann in records
    ]
    if asset_id:
        return [TextContent(type="text", text=json.dumps(payload[0], indent=2))]
    return [TextContent(type="text", text=json.dumps({"annotations": payload}, indent=2))]


def _handle_lineage(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import get_lineage_summary

    asset_id = arguments["asset_id"]
    direction = arguments["direction"]
    depth = arguments.get("depth")

    summary = get_lineage_summary(_db_path(cfg), asset_id, direction=direction, depth=depth)

    if not summary.asset_exists:
        return [TextContent(type="text", text=f"Asset not found in lineage graph: {asset_id}")]

    if not summary.related:
        return [TextContent(type="text", text=f"No {direction} assets found for {asset_id}.")]

    lines = [f"{direction.capitalize()} lineage for {asset_id} ({len(summary.related)} nodes):"]
    for node in summary.related:
        lines.append(f"  {node}")
    return [TextContent(type="text", text="\n".join(lines))]


def _handle_status(cfg: AtlasConfig) -> list[TextContent]:
    from alma_atlas.application.query.service import get_graph_status

    summary = get_graph_status(_db_path(cfg))

    lines = [
        f"Atlas graph: {summary.asset_count} assets, {summary.edge_count} edges, {summary.query_count} query fingerprints",
        "",
        "Assets by kind:",
    ]
    for kind, count in sorted(summary.kind_counts.items()):
        lines.append(f"  {kind}: {count}")

    if summary.source_counts:
        lines.append("")
        lines.append("Assets by source:")
        for source, count in sorted(summary.source_counts.items()):
            lines.append(f"  {source}: {count}")

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_get_schema(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import get_latest_schema

    asset_id = arguments["asset_id"]
    asset, snapshot = get_latest_schema(_db_path(cfg), asset_id)
    if asset is None:
        return [TextContent(type="text", text=f"Asset not found: {asset_id}")]

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
    from alma_atlas.application.query.service import get_impact_summary

    asset_id = arguments["asset_id"]
    depth = arguments.get("depth")
    summary = get_impact_summary(_db_path(cfg), asset_id, depth=depth)
    if not summary.asset_exists:
        return [TextContent(type="text", text=f"Asset not found: {asset_id}")]
    downstream = summary.downstream_assets
    if not downstream:
        return [
            TextContent(
                type="text",
                text=f"No downstream dependencies found for {asset_id}. Changes to this asset have no detected downstream impact.",
            )
        ]
    blast_radius = len(downstream)
    total_query_exposure = sum(summary.query_counts.values())

    lines = [
        f"Impact analysis for {asset_id}:",
        f"  Blast radius: {blast_radius} downstream asset(s)",
        f"  Query exposure: {total_query_exposure} query execution(s) across affected assets\n",
    ]
    for node_id in downstream:
        qcount = summary.query_counts.get(node_id, 0)
        qinfo = f"  ({qcount} query exec(s))" if qcount else ""
        lines.append(f"  ⚠ {node_id}{qinfo}")

    lines.append(
        f"\nRecommendation: Review these {blast_radius} downstream assets before making changes to {asset_id}."
    )
    return [TextContent(type="text", text="\n".join(lines))]


def _handle_get_query_patterns(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import get_query_patterns

    top_n = arguments.get("top_n", 20)
    queries = get_query_patterns(_db_path(cfg), top_n=top_n)
    if not queries:
        return [TextContent(type="text", text="No query patterns found.")]

    lines = [f"Top {len(queries)} query pattern(s) by execution count:\n"]
    for i, q in enumerate(queries, 1):
        tables_str = ", ".join(q.tables) if q.tables else "(none)"
        lines.append(f"  {i}. fingerprint={q.fingerprint}  executions={q.execution_count}")
        lines.append(f"     tables: {tables_str}")
        lines.append(f"     source: {q.source}")
        if cfg.privacy.include_sql_previews and q.sql_text:
            sql_preview = q.sql_text[:120].replace("\n", " ") + ("..." if len(q.sql_text) > 120 else "")
            lines.append(f"     sql: {sql_preview}")
        lines.append("")

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_suggest_tables(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import suggest_tables

    query = arguments["query"]
    limit = arguments.get("limit", 10)
    results = suggest_tables(_db_path(cfg), query, limit=limit)

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
    from alma_atlas.application.query.service import list_violations

    asset_id = arguments.get("asset_id")
    limit = arguments.get("limit", 50)
    violations = list_violations(_db_path(cfg), asset_id=asset_id, limit=limit)

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


def _tool_handlers():
    return {
        "atlas_search": _handle_search,
        "atlas_get_asset": _handle_get_asset,
        "atlas_get_annotations": _handle_get_annotations,
        "atlas_lineage": _handle_lineage,
        "atlas_status": lambda cfg, arguments: _handle_status(cfg),
        "atlas_get_schema": _handle_get_schema,
        "atlas_impact": _handle_impact,
        "atlas_get_query_patterns": _handle_get_query_patterns,
        "atlas_suggest_tables": _handle_suggest_tables,
        "atlas_check_contract": _handle_check_contract,
        "atlas_list_violations": _handle_list_violations,
        "atlas_team_sync": lambda cfg, arguments: _handle_team_sync(cfg),
    }


async def _handle_team_sync(cfg: AtlasConfig) -> list[TextContent]:
    from alma_atlas.application.sync.use_cases import run_team_sync

    try:
        response = await run_team_sync(cfg)
        return [
            TextContent(
                type="text",
                text=f"Team sync complete: {response.accepted_count} record(s) accepted, {len(response.rejected)} rejected.",
            )
        ]
    except Exception as exc:
        return [TextContent(type="text", text=f"Team sync failed: {exc}")]


def _handle_check_contract(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.contracts.use_cases import check_asset_contracts

    asset_id = arguments["asset_id"]
    checks = check_asset_contracts(cfg, asset_id)
    if not checks:
        return [TextContent(type="text", text=f"No contracts found for asset: {asset_id}")]

    violations: list[str] = []
    for check in checks:
        issues = check.issues
        violations.extend(str(issue.get("message", "Unknown contract validation issue")) for issue in issues)

    if not violations:
        lines = [
            f"Contract check PASSED for {asset_id}",
            f"  {len(checks)} contract(s) validated, no violations found.",
        ]
    else:
        lines = [f"Contract check FAILED for {asset_id}: {len(violations)} violation(s)\n"]
        for v in violations:
            lines.append(f"  ✗ {v}")

    return [TextContent(type="text", text="\n".join(lines))]
