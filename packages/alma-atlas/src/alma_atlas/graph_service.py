"""Shared read/query helpers for the Atlas graph."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from alma_atlas.config import AtlasConfig


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


def search_assets(db_path: Path, query: str, *, limit: int) -> list[object]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return AssetRepository(db).search(query)[:limit]


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
    from alma_analysis.lineage import Edge, compute_lineage
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    with Database(db_path) as db:
        raw_edges = EdgeRepository(db).list_all()

    edges = [Edge(upstream_id=e.upstream_id, downstream_id=e.downstream_id, kind=e.kind) for e in raw_edges]
    graph = compute_lineage(edges)
    if not graph.has_asset(asset_id):
        return LineageSummary(asset_exists=False, related=[])

    if direction == "upstream":
        related = graph.upstream(asset_id, depth=depth)
    else:
        related = graph.downstream(asset_id, depth=depth)
    return LineageSummary(asset_exists=True, related=related)


async def run_team_sync(cfg: AtlasConfig):
    """Run a full team sync using the configured team connection."""

    cfg.load_team_config()
    if not cfg.team_server_url or not cfg.team_api_key:
        raise ValueError("Team sync not configured. Run `alma-atlas team init` first.")
    if cfg.db_path is None or not cfg.db_path.exists():
        raise ValueError("No Atlas database found. Run `alma-atlas scan` first.")

    from alma_atlas.sync.auth import TeamAuth
    from alma_atlas.sync.client import SyncClient
    from alma_atlas_store.db import Database

    auth = TeamAuth(cfg.team_api_key)
    async with SyncClient(cfg.team_server_url, auth, cfg.team_id or "default") as client:
        with Database(cfg.db_path) as db:
            return await client.full_sync(db, cfg)
