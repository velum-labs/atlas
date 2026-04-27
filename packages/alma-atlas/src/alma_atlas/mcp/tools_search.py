"""Search-oriented MCP tools: free-text asset search, table suggestions, business term lookup."""

from __future__ import annotations

import contextlib
from typing import Any

from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp._common import AtlasToolSpec, _db_path


def specs() -> tuple[AtlasToolSpec, ...]:
    """Return tool specs for the search category."""
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
    )


def handlers():
    """Return the dispatch dict for the search category."""
    return {
        "atlas_search": _handle_search,
        "atlas_suggest_tables": _handle_suggest_tables,
        "atlas_find_term": _handle_find_term,
    }


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

        with contextlib.suppress(Exception):
            fts_results = annotation_repo.search_fts(query, limit=limit)

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

        with contextlib.suppress(Exception):
            term_results = term_repo.search(term)[:limit]

        with contextlib.suppress(Exception):
            fts_results = annotation_repo.search_fts(term, limit=limit)

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
