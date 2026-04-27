"""Lineage-oriented MCP tools: upstream/downstream traversal, impact analysis, table relationships."""

from __future__ import annotations

import json
from typing import Any

from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp._common import AtlasToolSpec, _db_path


def specs() -> tuple[AtlasToolSpec, ...]:
    """Return tool specs for the lineage category."""
    return (
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
    )


def handlers():
    """Return the dispatch dict for the lineage category."""
    return {
        "atlas_lineage": _handle_lineage,
        "atlas_impact": _handle_impact,
        "atlas_describe_relationship": _handle_describe_relationship,
    }


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
