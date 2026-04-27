"""Atlas Companion MCP tools: curated 3-tool surface for the concierge audience.

These three tools replace the 20-tool Atlas surface when Atlas runs in
Companion mode (started via `alma-atlas serve --alma-token <invite>`).
They produce `CompanionBundle` responses optimized for an agent's prompt
window: short, structured, with explicit "ask for more" affordances.

Tool catalogue:
    - companion_search_assets               Find assets matching a query
    - companion_get_schema_and_owner        Get one asset's schema + owner
    - companion_explain_lineage_and_contract  Get one asset's lineage + contract status

Internally these compose the same repositories used by the full Atlas surface
(AssetRepository, SchemaRepository, AnnotationRepository, EdgeRepository).
The differentiator is the response format: `CompanionBundle` rendered as
prompt-ready text via `agent_bundle.render()`, not raw structured metadata.
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp._common import AtlasToolSpec, _db_path
from alma_atlas.mcp.agent_bundle import (
    CompanionAsset,
    CompanionBundle,
    CompanionColumn,
    CompanionRelationship,
    render,
)


def specs() -> tuple[AtlasToolSpec, ...]:
    """Return tool specs for the Companion category."""
    return (
        AtlasToolSpec(
            name="companion_search_assets",
            description=(
                "Find data assets matching a query. Returns a curated list of related "
                "assets with their identifiers and owners, formatted for an agent prompt window."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search term (asset name, keyword, or annotation)"},
                    "limit": {"type": "integer", "description": "Max results", "default": 20},
                },
                "required": ["query"],
            },
        ),
        AtlasToolSpec(
            name="companion_get_schema_and_owner",
            description=(
                "Get an asset's schema, owner, freshness, and description as a single "
                "agent-ready context block."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Fully-qualified asset ID"},
                },
                "required": ["asset_id"],
            },
        ),
        AtlasToolSpec(
            name="companion_explain_lineage_and_contract",
            description=(
                "Explain an asset's upstream/downstream lineage and current contract "
                "validation status as a single agent-ready context block."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Asset ID to trace lineage for"},
                    "depth": {"type": "integer", "description": "Maximum lineage depth", "default": 3},
                },
                "required": ["asset_id"],
            },
        ),
    )


def handlers() -> dict[str, Any]:
    """Return the dispatch dict for the Companion category."""
    return {
        "companion_search_assets": _handle_companion_search,
        "companion_get_schema_and_owner": _handle_companion_get_schema_and_owner,
        "companion_explain_lineage_and_contract": _handle_companion_explain_lineage_and_contract,
    }


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _handle_companion_search(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    """Find assets matching a query; return as a CompanionBundle of related assets."""
    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    query = arguments["query"]
    limit = int(arguments.get("limit", 20))

    related_assets: list[CompanionAsset] = []
    seen: set[str] = set()

    with Database(_db_path(cfg)) as db:
        asset_repo = AssetRepository(db)
        annotation_repo = AnnotationRepository(db)

        name_results = asset_repo.search(query)[:limit]
        for asset in name_results:
            if asset.id in seen:
                continue
            seen.add(asset.id)
            owner = _resolve_owner(annotation_repo, asset.id)
            related_assets.append(
                CompanionAsset(
                    asset_id=asset.id,
                    kind=asset.kind,
                    source=asset.source,
                    owner=owner,
                    description=asset.description,
                )
            )

        with contextlib.suppress(Exception):
            fts_results = annotation_repo.search_fts(query, limit=limit)
            for asset_id, _snippet in fts_results:
                if asset_id in seen:
                    continue
                seen.add(asset_id)
                asset = asset_repo.get(asset_id)
                if asset is None:
                    continue
                owner = _resolve_owner(annotation_repo, asset_id)
                related_assets.append(
                    CompanionAsset(
                        asset_id=asset.id,
                        kind=asset.kind,
                        source=asset.source,
                        owner=owner,
                        description=asset.description,
                    )
                )

    summary = (
        f"Found {len(related_assets)} asset(s) matching {query!r}."
        if related_assets
        else f"No assets found matching {query!r}."
    )
    bundle = CompanionBundle(related_assets=related_assets, summary=summary)
    return [TextContent(type="text", text=render(bundle))]


def _handle_companion_get_schema_and_owner(
    cfg: AtlasConfig, arguments: dict[str, Any]
) -> list[TextContent]:
    """Build a CompanionBundle around one asset: schema + owner + freshness."""
    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_id = arguments["asset_id"]

    with Database(_db_path(cfg)) as db:
        asset = AssetRepository(db).get(asset_id)
        snapshot = SchemaRepository(db).get_latest(asset_id) if asset is not None else None
        annotation = AnnotationRepository(db).get(asset_id) if asset is not None else None

    if asset is None:
        bundle = CompanionBundle(summary=f"Asset not found: {asset_id}")
        return [TextContent(type="text", text=render(bundle))]

    columns = _columns_from_snapshot_or_metadata(snapshot, asset.metadata)
    owner = annotation.ownership if annotation else None
    last_updated = (
        snapshot.captured_at if snapshot is not None and snapshot.captured_at else asset.last_seen
    )

    primary = CompanionAsset(
        asset_id=asset.id,
        kind=asset.kind,
        source=asset.source,
        owner=owner,
        last_updated=last_updated,
        description=asset.description,
        columns=columns,
        columns_total=len(columns),
    )
    bundle = CompanionBundle(primary_asset=primary)
    return [TextContent(type="text", text=render(bundle))]


def _handle_companion_explain_lineage_and_contract(
    cfg: AtlasConfig, arguments: dict[str, Any]
) -> list[TextContent]:
    """Build a CompanionBundle around one asset: lineage edges + contract violations."""
    from alma_atlas.application.contracts.use_cases import check_asset_contracts
    from alma_atlas.application.query.service import get_lineage_summary
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    asset_id = arguments["asset_id"]
    depth = int(arguments.get("depth", 3))

    with Database(_db_path(cfg)) as db:
        asset = AssetRepository(db).get(asset_id)

    if asset is None:
        bundle = CompanionBundle(summary=f"Asset not found: {asset_id}")
        return [TextContent(type="text", text=render(bundle))]

    upstream = get_lineage_summary(_db_path(cfg), asset_id, direction="upstream", depth=depth)
    downstream = get_lineage_summary(_db_path(cfg), asset_id, direction="downstream", depth=depth)

    relationships: list[CompanionRelationship] = []
    seen_pairs: set[tuple[str, str]] = set()
    for related_id in upstream.related:
        pair = (related_id, asset_id)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        relationships.append(
            CompanionRelationship(upstream=related_id, downstream=asset_id, kind="lineage_upstream")
        )
    for related_id in downstream.related:
        pair = (asset_id, related_id)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        relationships.append(
            CompanionRelationship(upstream=asset_id, downstream=related_id, kind="lineage_downstream")
        )

    warnings: list[str] = []
    summary_parts: list[str] = []
    try:
        checks = check_asset_contracts(cfg, asset_id)
    except Exception as exc:  # contract layer is optional / may not be configured
        checks = []
        warnings.append(f"Contract check unavailable: {exc}")

    if not checks:
        summary_parts.append("No contracts defined.")
    else:
        violation_count = 0
        for check in checks:
            for issue in check.issues:
                violation_count += 1
                warnings.append(str(issue.get("message", "Unknown contract violation")))
        if violation_count == 0:
            summary_parts.append(
                f"Contract check PASSED ({len(checks)} contract(s), no violations)."
            )
        else:
            summary_parts.append(
                f"Contract check FAILED: {violation_count} violation(s) across {len(checks)} contract(s)."
            )

    if relationships:
        summary_parts.insert(0, f"Lineage: {len(relationships)} related asset(s).")
    else:
        summary_parts.insert(0, "Lineage: no related assets found.")

    primary = CompanionAsset(
        asset_id=asset.id,
        kind=asset.kind,
        source=asset.source,
        description=asset.description,
    )
    bundle = CompanionBundle(
        primary_asset=primary,
        relationships=relationships,
        warnings=warnings,
        summary=" ".join(summary_parts) or None,
    )
    return [TextContent(type="text", text=render(bundle))]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_owner(annotation_repo, asset_id: str) -> str | None:
    """Look up an asset's owner from its annotation, if any."""
    try:
        annotation = annotation_repo.get(asset_id)
    except Exception:
        return None
    return annotation.ownership if annotation else None


def _columns_from_snapshot_or_metadata(snapshot, metadata: dict) -> list[CompanionColumn]:
    """Extract columns from the schema snapshot, falling back to asset metadata."""
    if snapshot is not None:
        if isinstance(snapshot.columns, str):
            raw = json.loads(snapshot.columns)
        else:
            raw = [vars(c) if hasattr(c, "__dict__") else c for c in snapshot.columns]
        return _columns_from_raw(raw)
    return _columns_from_raw(metadata.get("columns", []))


def _columns_from_raw(raw: list) -> list[CompanionColumn]:
    columns: list[CompanionColumn] = []
    for col in raw:
        if isinstance(col, dict):
            name = col.get("name", "?")
            ctype = col.get("type", "unknown")
            nullable = bool(col.get("nullable", True))
            annotation = col.get("description") or col.get("annotation")
        else:
            name = getattr(col, "name", "?")
            ctype = getattr(col, "type", "unknown")
            nullable = bool(getattr(col, "nullable", True))
            annotation = getattr(col, "description", None) or getattr(col, "annotation", None)
        columns.append(
            CompanionColumn(name=name, type=ctype, nullable=nullable, annotation=annotation)
        )
    return columns
