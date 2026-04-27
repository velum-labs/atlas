"""Schema-oriented MCP tools: asset lookup, schema snapshots, column inspection, annotations."""

from __future__ import annotations

import json
from typing import Any

from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp._common import AtlasToolSpec, _db_path


def specs() -> tuple[AtlasToolSpec, ...]:
    """Return tool specs for the schema category."""
    return (
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
    )


def handlers():
    """Return the dispatch dict for the schema category."""
    return {
        "atlas_get_asset": _handle_get_asset,
        "atlas_get_annotations": _handle_get_annotations,
        "atlas_get_schema": _handle_get_schema,
        "atlas_explain_column": _handle_explain_column,
        "atlas_profile_column": _handle_profile_column,
    }


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


def _handle_get_schema(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import get_latest_schema

    asset_id = arguments["asset_id"]
    asset, snapshot = get_latest_schema(_db_path(cfg), asset_id)
    if asset is None:
        return [TextContent(type="text", text=f"Asset not found: {asset_id}")]

    if snapshot is None:
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
