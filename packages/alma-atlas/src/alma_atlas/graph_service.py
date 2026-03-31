"""Shared read/query helpers for the Atlas graph."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from alma_atlas.config import AtlasConfig
from alma_ports.annotation import AnnotationRecord
from alma_ports.asset import Asset
from alma_ports.edge import Edge as GraphEdge
from alma_ports.query import QueryObservation
from alma_ports.schema import SchemaSnapshot
from alma_ports.violation import Violation


@dataclass(frozen=True)
class GraphStatusSummary:
    asset_count: int
    edge_count: int
    query_count: int
    kind_counts: dict[str, int]
    source_counts: dict[str, int]


@dataclass(frozen=True)
class LineageSummary:
    asset_exists: bool
    related: list[str]


@dataclass(frozen=True)
class ImpactSummary:
    asset_exists: bool
    downstream_assets: list[str]
    query_counts: dict[str, int]


def require_db_path(cfg: AtlasConfig) -> Path:
    if cfg.db_path is None or not cfg.db_path.exists():
        raise ValueError("No Atlas database found. Run `alma-atlas scan` first.")
    return cfg.db_path


def search_assets(db_path: Path, query: str, *, limit: int) -> list[Asset]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return AssetRepository(db).search(query)[:limit]


def get_asset(db_path: Path, asset_id: str) -> Asset | None:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return AssetRepository(db).get(asset_id)


def get_annotations(db_path: Path, *, asset_id: str | None = None, limit: int = 100) -> list[AnnotationRecord]:
    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        repo = AnnotationRepository(db)
        if asset_id is not None:
            record = repo.get(asset_id)
            return [record] if record is not None else []
        return repo.list_all(limit=limit)


def get_latest_schema(db_path: Path, asset_id: str) -> tuple[Asset | None, SchemaSnapshot | None]:
    from alma_atlas_store.db import Database
    from alma_atlas_store.schema_repository import SchemaRepository

    asset = get_asset(db_path, asset_id)
    if asset is None:
        return None, None
    with Database(db_path) as db:
        return asset, SchemaRepository(db).get_latest(asset_id)


def get_query_patterns(db_path: Path, *, top_n: int) -> list[QueryObservation]:
    from alma_atlas_store.db import Database
    from alma_atlas_store.query_repository import QueryRepository

    with Database(db_path) as db:
        return QueryRepository(db).list_all()[:top_n]


def suggest_tables(db_path: Path, query: str, *, limit: int) -> list[tuple[float, Asset, set[str]]]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.schema_repository import SchemaRepository

    query_tokens = {token.lower() for token in query.split() if token}
    with Database(db_path) as db:
        assets = AssetRepository(db).search(query)
        schema_repo = SchemaRepository(db)
        results: list[tuple[float, Asset, set[str]]] = []
        for asset in assets:
            snapshot = schema_repo.get_latest(asset.id)
            col_names: set[str] = set()
            if snapshot:
                col_names = {column.name.lower() for column in snapshot.columns}
            elif "columns" in asset.metadata:
                col_names = {column.get("name", "").lower() for column in asset.metadata["columns"]}
            if col_names and query_tokens:
                union = query_tokens | col_names
                jaccard = len(query_tokens & col_names) / len(union)
            else:
                jaccard = 0.0
            name_match = 1.0 if query.lower() in asset.name.lower() else 0.0
            score = 0.5 * name_match + 0.5 * jaccard
            results.append((score, asset, col_names))
    results.sort(key=lambda item: -item[0])
    return results[:limit]


def list_violations(
    db_path: Path,
    *,
    asset_id: str | None = None,
    limit: int = 50,
) -> list[Violation]:
    from alma_atlas_store.db import Database
    from alma_atlas_store.violation_repository import ViolationRepository

    with Database(db_path) as db:
        repo = ViolationRepository(db)
        if asset_id is not None:
            return repo.list_for_asset(asset_id)[:limit]
        return repo.list_recent(limit=limit)


def export_graph(db_path: Path) -> dict[str, list[dict[str, Any]]]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    with Database(db_path) as db:
        assets = AssetRepository(db).list_all()
        edges = EdgeRepository(db).list_all()
    return {
        "assets": [
            {
                "id": asset.id,
                "source": asset.source,
                "kind": asset.kind,
                "name": asset.name,
                "description": asset.description,
                "tags": asset.tags,
                "metadata": asset.metadata,
            }
            for asset in assets
        ],
        "edges": [
            {
                "upstream_id": edge.upstream_id,
                "downstream_id": edge.downstream_id,
                "kind": edge.kind,
                "metadata": edge.metadata,
            }
            for edge in edges
        ],
    }


def get_graph_status(db_path: Path) -> GraphStatusSummary:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.query_repository import QueryRepository

    with Database(db_path) as db:
        assets = AssetRepository(db).list_all()
        edges = EdgeRepository(db).list_all()
        queries = QueryRepository(db).list_all()

    kind_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for asset in assets:
        kind_counts[asset.kind] = kind_counts.get(asset.kind, 0) + 1
        source_counts[asset.source] = source_counts.get(asset.source, 0) + 1

    return GraphStatusSummary(
        asset_count=len(assets),
        edge_count=len(edges),
        query_count=len(queries),
        kind_counts=kind_counts,
        source_counts=source_counts,
    )


def get_lineage_summary(
    db_path: Path,
    asset_id: str,
    *,
    direction: Literal["upstream", "downstream"],
    depth: int | None,
) -> LineageSummary:
    from alma_analysis.lineage import compute_lineage
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    with Database(db_path) as db:
        raw_edges = EdgeRepository(db).list_all()

    edges = [_to_lineage_edge(edge) for edge in raw_edges]
    graph = compute_lineage(edges)
    if not graph.has_asset(asset_id):
        return LineageSummary(asset_exists=False, related=[])

    if direction == "upstream":
        related = graph.upstream(asset_id, depth=depth)
    else:
        related = graph.downstream(asset_id, depth=depth)
    return LineageSummary(asset_exists=True, related=related)


def get_impact_summary(
    db_path: Path,
    asset_id: str,
    *,
    depth: int | None,
) -> ImpactSummary:
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.query_repository import QueryRepository

    asset = get_asset(db_path, asset_id)
    if asset is None:
        return ImpactSummary(asset_exists=False, downstream_assets=[], query_counts={})
    with Database(db_path) as db:
        raw_edges = EdgeRepository(db).list_all()
        all_queries = QueryRepository(db).list_all()
    from alma_analysis.lineage import compute_lineage

    graph = compute_lineage([_to_lineage_edge(edge) for edge in raw_edges])
    if not graph.has_asset(asset_id):
        return ImpactSummary(asset_exists=False, downstream_assets=[], query_counts={})
    downstream = graph.downstream(asset_id, depth=depth)
    query_counts: dict[str, int] = {}
    for query in all_queries:
        for table in query.tables:
            if table in downstream:
                query_counts[table] = query_counts.get(table, 0) + query.execution_count
    return ImpactSummary(asset_exists=True, downstream_assets=downstream, query_counts=query_counts)


async def run_team_sync(cfg: AtlasConfig):
    """Run a full team sync using the configured team connection."""

    cfg.load_team_config()
    if not cfg.team_server_url or not cfg.team_api_key:
        raise ValueError("Team sync not configured. Run `alma-atlas team init` first.")
    db_path = require_db_path(cfg)

    from alma_atlas.sync.auth import TeamAuth
    from alma_atlas.sync.client import SyncClient
    from alma_atlas_store.db import Database

    auth = TeamAuth(cfg.team_api_key)
    async with SyncClient(cfg.team_server_url, auth, cfg.team_id or "default") as client:
        with Database(db_path) as db:
            with db.transaction():
                return await client.full_sync(db, cfg)


def _to_lineage_edge(edge: GraphEdge):
    from alma_analysis.lineage import Edge

    return Edge(
        upstream_id=edge.upstream_id,
        downstream_id=edge.downstream_id,
        kind=edge.kind,
    )
