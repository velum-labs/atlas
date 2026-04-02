"""MCP tool registrations for Alma Atlas.

Registers all Atlas tools with the MCP server. Each tool exposes a slice
of the Atlas graph to AI agents: asset search, lineage traversal, schema
lookup, and status summaries.

Tool catalogue:
    - atlas_search                  Search for assets by name, keyword, or annotation text
    - atlas_get_asset               Get a specific asset by ID
    - atlas_get_annotations         Get agent-generated asset annotations
    - atlas_lineage                 Get upstream or downstream lineage for an asset
    - atlas_status                  Summarise the current graph (counts by kind)
    - atlas_get_schema              Get the latest schema snapshot for an asset
    - atlas_impact                  Analyse downstream impact of changes to an asset
    - atlas_get_query_patterns      Show top SQL query patterns by execution count
    - atlas_suggest_tables          Suggest relevant tables for a search query
    - atlas_check_contract          Validate schema against data contract spec
    - atlas_list_violations         List recent enforcement violations from the store
    - atlas_team_sync               Trigger a team graph sync programmatically
    - atlas_explain_column          Explain what a column means: type, business context, stats
    - atlas_profile_column          Get data distribution stats for a column
    - atlas_describe_relationship   Describe how two tables relate
    - atlas_find_term               Find assets and columns matching a business term
    - atlas_verify                  Check if a SQL query is correct against Atlas knowledge
    - atlas_define_term             Define or update a business term in the glossary
    - atlas_context                 Get curated context (tables, joins, columns, warnings) for a question
    - atlas_ask                     Ask Atlas a data question -- returns an explanation, no SQL
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
        AtlasToolSpec(
            name="atlas_explain_column",
            description="Explain what a column means: schema type, business context, data distribution, and lineage.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Asset ID containing the column"},
                    "column": {"type": "string", "description": "Column name to explain"},
                },
                "required": ["asset_id", "column"],
            },
        ),
        AtlasToolSpec(
            name="atlas_profile_column",
            description="Get data distribution stats for a column: distinct values, nulls, min/max, top values.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Asset ID containing the column"},
                    "column": {"type": "string", "description": "Column name to profile"},
                },
                "required": ["asset_id", "column"],
            },
        ),
        AtlasToolSpec(
            name="atlas_describe_relationship",
            description="Describe how two tables relate: join paths, foreign keys, edge metadata, and join guidance.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_a": {"type": "string", "description": "First asset ID"},
                    "asset_b": {"type": "string", "description": "Second asset ID"},
                },
                "required": ["asset_a", "asset_b"],
            },
        ),
        AtlasToolSpec(
            name="atlas_find_term",
            description="Find which columns and tables map to a business concept (e.g., 'revenue', 'active user').",
            input_schema={
                "type": "object",
                "properties": {
                    "term": {"type": "string", "description": "Business term or concept to search for"},
                },
                "required": ["term"],
            },
        ),
        AtlasToolSpec(
            name="atlas_verify",
            description="Check if something is correct -- a SQL query, a join path, a metric definition. Atlas will validate against its learned knowledge.",
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to verify"},
                    "source_id": {"type": "string", "description": "Source/database context"},
                    "deep": {
                        "type": "boolean",
                        "description": "Use Atlas agent for deeper LLM-backed analysis (default: false)",
                        "default": False,
                    },
                },
                "required": ["sql"],
            },
        ),
        AtlasToolSpec(
            name="atlas_define_term",
            description="Define or update a business term (e.g., 'revenue', 'active user') with its definition, formula, and referenced columns.",
            input_schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "definition": {"type": "string"},
                    "formula": {"type": "string"},
                    "referenced_columns": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["name"],
            },
        ),
        AtlasToolSpec(
            name="atlas_context",
            description=(
                "Get the context you need to work with this data. Describe what you're trying to do "
                "and Atlas will gather relevant tables, join paths, column semantics, value "
                "distributions, and known pitfalls."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "What you're trying to do or answer",
                    },
                    "db_id": {
                        "type": "string",
                        "description": "Database or source identifier (optional scope)",
                    },
                    "evidence": {
                        "type": "string",
                        "description": "Optional hints or domain context",
                    },
                },
                "required": ["question"],
            },
        ),
        AtlasToolSpec(
            name="atlas_ask",
            description=(
                "Ask Atlas a question about your data. Returns an explanation grounded in schema, "
                "annotations, profiling stats, and lineage. No SQL -- just understanding."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "source_id": {
                        "type": "string",
                        "description": "Optional: scope to a specific source",
                    },
                },
                "required": ["question"],
            },
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
    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    query = arguments["query"]
    limit = int(arguments.get("limit", 20))

    fts_results: list[tuple[str, str]] = []
    name_results = []
    fts_only_assets: dict[str, Any] = {}

    with Database(_db_path(cfg)) as db:
        asset_repo = AssetRepository(db)
        annotation_repo = AnnotationRepository(db)

        name_results = asset_repo.search(query)[:limit]
        name_map = {a.id: a for a in name_results}

        try:
            fts_results = annotation_repo.search_fts(query, limit=limit)
        except Exception:
            pass

        fts_only_ids = [aid for aid, _ in fts_results if aid not in name_map]
        for aid in fts_only_ids:
            asset = asset_repo.get(aid)
            if asset:
                fts_only_assets[aid] = asset

    seen: set[str] = set()
    lines: list[str] = []

    for asset_id, snippet in fts_results:
        if asset_id not in seen:
            seen.add(asset_id)
            asset = name_map.get(asset_id) or fts_only_assets.get(asset_id)
            if asset:
                lines.append(f"  {asset.id}  [{asset.kind}]  source={asset.source}  [annotation match]")
            else:
                lines.append(f"  {asset_id}  [annotation match]")
            lines.append(f"    ...{snippet}...")

    for a in name_results:
        if a.id not in seen:
            seen.add(a.id)
            desc = f"  {a.description}" if a.description else ""
            lines.append(f"  {a.id}  [{a.kind}]  source={a.source}{desc}")

    if not lines:
        return [TextContent(type="text", text=f"No assets found matching {query!r}.")]
    header = f"Found {len(seen)} asset(s) matching {query!r}:\n"
    return [TextContent(type="text", text=header + "\n".join(lines))]


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


def _handle_explain_column(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.profiling_repository import ProfilingRepository
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_id = arguments["asset_id"]
    column = arguments["column"]

    with Database(_db_path(cfg)) as db:
        snapshot = SchemaRepository(db).get_latest(asset_id)
        annotation = AnnotationRepository(db).get(asset_id)
        profile = ProfilingRepository(db).get(asset_id, column)

    if snapshot is None and annotation is None and profile is None:
        return [TextContent(type="text", text=f"No information found for column '{column}' in asset '{asset_id}'.")]

    parts: list[str] = [f"Column: {asset_id}.{column}"]

    if snapshot is not None:
        if isinstance(snapshot.columns, str):
            col_list = json.loads(snapshot.columns)
        else:
            col_list = [vars(c) if hasattr(c, "__dict__") else c for c in snapshot.columns]
        for col in col_list:
            col_name = col.get("name", "") if isinstance(col, dict) else getattr(col, "name", "")
            if col_name == column:
                dtype = col.get("type", "unknown") if isinstance(col, dict) else getattr(col, "type", "unknown")
                nullable = col.get("nullable", True) if isinstance(col, dict) else getattr(col, "nullable", True)
                parts.append(f"Type: {dtype}  {'NULL' if nullable else 'NOT NULL'}")
                break

    if annotation is not None:
        column_notes = annotation.properties.get("column_notes", {})
        note = column_notes.get(column)
        if note:
            parts.append(f"Business context: {note}")

    if profile is not None:
        dist_lines: list[str] = ["Data distribution:"]
        if profile.distinct_count is not None:
            dist_lines.append(f"  distinct_count: {profile.distinct_count}")
        if profile.null_count is not None:
            pct = f" ({profile.null_fraction:.1%})" if profile.null_fraction is not None else ""
            dist_lines.append(f"  null_count: {profile.null_count}{pct}")
        if profile.min_value is not None:
            dist_lines.append(f"  min: {profile.min_value}")
        if profile.max_value is not None:
            dist_lines.append(f"  max: {profile.max_value}")
        if profile.top_values:
            top_str = ", ".join(f"{v['value']} ({v['count']})" for v in profile.top_values[:5])
            dist_lines.append(f"  top values: {top_str}")
        if profile.sample_values:
            sample_str = ", ".join(str(v) for v in profile.sample_values[:5])
            dist_lines.append(f"  samples: {sample_str}")
        parts.append("\n".join(dist_lines))

    return [TextContent(type="text", text="\n".join(parts))]


def _handle_profile_column(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.db import Database
    from alma_atlas_store.profiling_repository import ProfilingRepository

    asset_id = arguments["asset_id"]
    column = arguments["column"]

    with Database(_db_path(cfg)) as db:
        profile = ProfilingRepository(db).get(asset_id, column)

    if profile is None:
        return [TextContent(type="text", text=f"No profile found for column '{column}' in asset '{asset_id}'.")]

    return [
        TextContent(
            type="text",
            text=json.dumps(
                {
                    "asset_id": profile.asset_id,
                    "column_name": profile.column_name,
                    "distinct_count": profile.distinct_count,
                    "null_count": profile.null_count,
                    "null_fraction": profile.null_fraction,
                    "min_value": profile.min_value,
                    "max_value": profile.max_value,
                    "top_values": profile.top_values,
                    "sample_values": profile.sample_values,
                    "profiled_at": profile.profiled_at,
                },
                indent=2,
            ),
        )
    ]


def _handle_describe_relationship(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_a = arguments["asset_a"]
    asset_b = arguments["asset_b"]

    with Database(_db_path(cfg)) as db:
        asset_repo = AssetRepository(db)
        edge_repo = EdgeRepository(db)
        schema_repo = SchemaRepository(db)

        a_exists = asset_repo.get(asset_a) is not None
        b_exists = asset_repo.get(asset_b) is not None

        if not a_exists and not b_exists:
            return [TextContent(type="text", text=f"No relationship found between '{asset_a}' and '{asset_b}'.")]

        edges_a = edge_repo.list_for_asset(asset_a)
        direct_edges = [
            e for e in edges_a
            if (e.upstream_id == asset_a and e.downstream_id == asset_b)
            or (e.upstream_id == asset_b and e.downstream_id == asset_a)
        ]

        snap_a = schema_repo.get_latest(asset_a)
        snap_b = schema_repo.get_latest(asset_b)

    lines: list[str] = [f"Relationship: {asset_a} <-> {asset_b}"]

    if direct_edges:
        lines.append(f"\nEdges ({len(direct_edges)}):")
        for edge in direct_edges:
            lines.append(f"  {edge.upstream_id} --[{edge.kind}]--> {edge.downstream_id}")
            join_guidance = edge.metadata.get("join_guidance")
            if join_guidance:
                lines.append(f"  join_guidance: {join_guidance}")
    else:
        lines.append("\nNo direct edges found.")

    fk_hints: list[str] = []
    for snap, this_id, other_id in [(snap_a, asset_a, asset_b), (snap_b, asset_b, asset_a)]:
        if snap is None:
            continue
        if isinstance(snap.columns, str):
            col_list = json.loads(snap.columns)
        else:
            col_list = [vars(c) if hasattr(c, "__dict__") else c for c in snap.columns]
        other_table = other_id.split("::")[-1].split(".")[-1]
        for col in col_list:
            col_name = col.get("name", "") if isinstance(col, dict) else getattr(col, "name", "")
            col_desc = (col.get("description") or "") if isinstance(col, dict) else (getattr(col, "description", "") or "")
            if other_table.lower() in col_name.lower() or other_table.lower() in col_desc.lower():
                fk_hints.append(f"  {this_id}.{col_name} (may reference {other_id})")

    if fk_hints:
        lines.append("\nPotential FK columns:")
        lines.extend(fk_hints)

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_find_term(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.business_term_repository import BusinessTermRepository
    from alma_atlas_store.db import Database

    term = arguments["term"]
    limit = 20

    fts_results: list[tuple[str, str]] = []
    asset_results = []
    fts_only_assets: dict[str, Any] = {}
    term_results = []

    with Database(_db_path(cfg)) as db:
        annotation_repo = AnnotationRepository(db)
        asset_repo = AssetRepository(db)
        term_repo = BusinessTermRepository(db)

        try:
            term_results = term_repo.search(term)[:limit]
        except Exception:
            pass

        try:
            fts_results = annotation_repo.search_fts(term, limit=limit)
        except Exception:
            pass

        asset_results = asset_repo.search(term)[:limit]
        name_map = {a.id: a for a in asset_results}

        fts_only_ids = [aid for aid, _ in fts_results if aid not in name_map]
        for aid in fts_only_ids:
            asset = asset_repo.get(aid)
            if asset:
                fts_only_assets[aid] = asset

    if not term_results and not fts_results and not asset_results:
        return [TextContent(type="text", text=f"No assets or terms found for {term!r}.")]

    seen_assets: set[str] = set()
    lines: list[str] = [f"Results for term {term!r}:\n"]

    if term_results:
        lines.append("Business terms:")
        for bt in term_results:
            lines.append(f"  {bt.name}  [business_term]  source={bt.source}")
            if bt.definition:
                lines.append(f"    {bt.definition}")
            if bt.formula:
                lines.append(f"    formula: {bt.formula}")
            if bt.referenced_columns:
                lines.append(f"    columns: {', '.join(bt.referenced_columns)}")
        lines.append("")

    for asset_id, snippet in fts_results:
        if asset_id not in seen_assets:
            seen_assets.add(asset_id)
            asset = name_map.get(asset_id) or fts_only_assets.get(asset_id)
            if asset:
                lines.append(f"  {asset.id}  [{asset.kind}]  source={asset.source}")
            else:
                lines.append(f"  {asset_id}")
            lines.append(f"    ...{snippet}...")

    for a in asset_results:
        if a.id not in seen_assets:
            seen_assets.add(a.id)
            desc = f"  {a.description}" if a.description else ""
            lines.append(f"  {a.id}  [{a.kind}]  source={a.source}{desc}")

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_verify(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    import sqlglot
    from sqlglot import exp

    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_sqlkit.table_refs import extract_tables_from_sql

    sql = arguments.get("sql", "").strip()
    if not sql:
        result = {"valid": False, "warnings": ["No SQL provided."], "suggestions": []}
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    try:
        table_refs = extract_tables_from_sql(sql)
    except Exception as exc:
        result = {"valid": False, "warnings": [f"SQL parse error: {exc}"], "suggestions": []}
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    warnings: list[str] = []
    suggestions: list[str] = []

    agg_columns: set[str] = set()
    try:
        parsed = sqlglot.parse_one(sql)
        for agg in parsed.find_all(exp.Sum, exp.Avg):
            for col in agg.find_all(exp.Column):
                agg_columns.add(col.name.lower())
    except Exception:
        pass

    with Database(_db_path(cfg)) as db:
        asset_repo = AssetRepository(db)
        annotation_repo = AnnotationRepository(db)
        edge_repo = EdgeRepository(db)

        table_asset_map: dict[str, str] = {}
        for ref in table_refs:
            candidates = asset_repo.search(ref.canonical_name)
            if candidates:
                best = next(
                    (a for a in candidates if a.name == ref.canonical_name),
                    candidates[0],
                )
                table_asset_map[ref.canonical_name] = best.id

        for asset_id in table_asset_map.values():
            annotation = annotation_repo.get(asset_id)
            if annotation is None:
                continue
            column_notes = annotation.properties.get("column_notes", {})
            for col_name, note in column_notes.items():
                if note and "surrogate key" in note.lower() and col_name.lower() in agg_columns:
                    warnings.append(
                        f"Column '{col_name}' in '{asset_id}' is annotated as a surrogate key "
                        f"but appears in an aggregate function (SUM/AVG): {note}"
                    )

        table_names = list(table_asset_map.keys())
        for i, name_a in enumerate(table_names):
            for name_b in table_names[i + 1:]:
                asset_a = table_asset_map[name_a]
                asset_b = table_asset_map[name_b]
                for edge in edge_repo.list_for_asset(asset_a):
                    pair_match = (
                        (edge.upstream_id == asset_a and edge.downstream_id == asset_b)
                        or (edge.upstream_id == asset_b and edge.downstream_id == asset_a)
                    )
                    if pair_match:
                        join_guidance = edge.metadata.get("join_guidance")
                        if join_guidance:
                            warnings.append(
                                f"JOIN between '{name_a}' and '{name_b}': {join_guidance}"
                            )

    result = {"valid": len(warnings) == 0, "warnings": warnings, "suggestions": suggestions}
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


def _handle_define_term(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.business_term_repository import BusinessTermRepository
    from alma_atlas_store.db import Database
    from alma_ports.business_term import BusinessTerm

    name = arguments["name"]
    definition = arguments.get("definition")
    formula = arguments.get("formula")
    referenced_columns = arguments.get("referenced_columns", [])

    term = BusinessTerm(
        name=name,
        definition=definition,
        formula=formula,
        referenced_columns=referenced_columns,
        source="manual",
    )

    with Database(_db_path(cfg)) as db:
        BusinessTermRepository(db).upsert(term)

    parts = [f"Business term '{name}' defined."]
    if definition:
        parts.append(f"Definition: {definition}")
    if formula:
        parts.append(f"Formula: {formula}")
    if referenced_columns:
        parts.append(f"Referenced columns: {', '.join(referenced_columns)}")

    return [TextContent(type="text", text="\n".join(parts))]


async def _handle_context(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.agents.atlas_agent import run_atlas_context

    question = arguments["question"]
    db_id = arguments.get("db_id", "")
    evidence = arguments.get("evidence")
    result = await run_atlas_context(cfg, question, db_id=db_id, evidence=evidence)
    return [TextContent(type="text", text=result.model_dump_json(indent=2))]


async def _handle_ask(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.agents.atlas_agent import run_atlas_ask

    question = arguments["question"]
    source_id = arguments.get("source_id")
    result = await run_atlas_ask(cfg, question, source_id=source_id)
    return [TextContent(type="text", text=result.model_dump_json(indent=2))]


async def _handle_verify_deep(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.agents.atlas_agent import run_verify_deep

    sql = arguments.get("sql", "").strip()
    source_id = arguments.get("source_id")

    # Run static analysis first, then hand off to the LLM for deeper analysis.
    static_texts = _handle_verify(cfg, {k: v for k, v in arguments.items() if k != "deep"})
    static_result: dict[str, Any] | None = None
    if static_texts:
        try:
            static_result = json.loads(static_texts[0].text)
        except Exception:
            pass

    result = await run_verify_deep(cfg, sql, source_id=source_id, static_result=static_result)
    return [TextContent(type="text", text=result.model_dump_json(indent=2))]


def _dispatch_verify(cfg: AtlasConfig, arguments: dict[str, Any]):
    """Dispatch atlas_verify to static or deep handler based on the 'deep' flag."""
    if arguments.get("deep"):
        return _handle_verify_deep(cfg, arguments)
    return _handle_verify(cfg, arguments)


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
        "atlas_explain_column": _handle_explain_column,
        "atlas_profile_column": _handle_profile_column,
        "atlas_describe_relationship": _handle_describe_relationship,
        "atlas_find_term": _handle_find_term,
        "atlas_verify": _dispatch_verify,
        "atlas_define_term": _handle_define_term,
        "atlas_context": _handle_context,
        "atlas_ask": _handle_ask,
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
