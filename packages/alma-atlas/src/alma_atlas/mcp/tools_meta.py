"""Meta / operational MCP tools: graph status, query patterns, team sync."""

from __future__ import annotations

from typing import Any

from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp._common import AtlasToolSpec, _db_path


def specs() -> tuple[AtlasToolSpec, ...]:
    """Return tool specs for the meta category."""
    return (
        AtlasToolSpec(
            name="atlas_status",
            description="Return a summary of the Atlas graph: total assets, edges, and asset counts by kind.",
            input_schema={"type": "object", "properties": {}},
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
            name="atlas_team_sync",
            description="Trigger a team graph sync — push local Atlas changes to the team server and pull team contracts. Requires team sync to be configured via `alma-atlas team init`.",
            input_schema={"type": "object", "properties": {}},
        ),
    )


def handlers():
    """Return the dispatch dict for the meta category.

    Note: atlas_status and atlas_team_sync take no arguments; their handlers
    have a different signature than the standard `(cfg, arguments)` shape.
    Wrapping with lambdas at the dispatch table keeps the handler signatures
    consistent for the call_tool dispatcher.
    """
    return {
        "atlas_status": lambda cfg, arguments: _handle_status(cfg),
        "atlas_get_query_patterns": _handle_get_query_patterns,
        "atlas_team_sync": lambda cfg, arguments: _handle_team_sync(cfg),
    }


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
