"""Application-layer team sync status helpers."""

from __future__ import annotations

from dataclasses import dataclass

from alma_atlas.application.query.service import get_graph_status
from alma_atlas.application.sync.mappers import parse_sync_timestamp
from alma_atlas.config import AtlasConfig


@dataclass(frozen=True)
class TeamSyncStatus:
    server_url: str | None
    team_id: str | None
    cursor: str | None
    pending_asset_changes: int | None
    asset_count: int | None
    edge_count: int | None
    contract_count: int | None


def get_team_sync_status(cfg: AtlasConfig) -> TeamSyncStatus:
    """Build a transport-neutral view of current team-sync state."""
    cfg.load_team_config()
    cursor = cfg.load_sync_cursor()

    pending_asset_changes: int | None = None
    asset_count: int | None = None
    edge_count: int | None = None
    contract_count: int | None = None

    if cfg.db_path and cfg.db_path.exists():
        from alma_atlas_store.asset_repository import AssetRepository
        from alma_atlas_store.contract_repository import ContractRepository
        from alma_atlas_store.db import Database

        summary = get_graph_status(cfg.db_path)
        asset_count = summary.asset_count
        edge_count = summary.edge_count
        with Database(cfg.db_path) as db:
            contract_count = len(ContractRepository(db).list_all())
            if cursor:
                pending_asset_changes = len(
                    [
                        asset
                        for asset in AssetRepository(db).list_all()
                        if parse_sync_timestamp(asset.last_seen) >= parse_sync_timestamp(cursor)
                    ]
                )

    return TeamSyncStatus(
        server_url=cfg.team_server_url,
        team_id=cfg.team_id,
        cursor=cursor,
        pending_asset_changes=pending_asset_changes,
        asset_count=asset_count,
        edge_count=edge_count,
        contract_count=contract_count,
    )
