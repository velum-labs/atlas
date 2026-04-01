"""Application-layer sync use cases."""

from __future__ import annotations

from alma_atlas.config import AtlasConfig
from alma_atlas.sync.auth import TeamAuth
from alma_atlas.sync.conflict import ConflictResolver
from alma_atlas.sync.protocol import SyncResponse

from .mappers import (
    NULL_SYNC_CURSOR,
    asset_to_payload,
    contract_to_payload,
    dict_to_asset,
    dict_to_contract,
    edge_to_payload,
    latest_sync_cursor,
    parse_sync_timestamp,
    violation_to_payload,
)
from .ports import TeamSyncApi


async def run_full_sync(api: TeamSyncApi, db, cfg: AtlasConfig) -> SyncResponse:
    """Push all local changes and pull remote assets/contracts."""
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.violation_repository import ViolationRepository

    cursor = cfg.load_sync_cursor() or NULL_SYNC_CURSOR

    all_assets = AssetRepository(db).list_all()
    all_edges = EdgeRepository(db).list_all()
    all_contracts = ContractRepository(db).list_all()
    all_violations = ViolationRepository(db).list_recent(limit=1000)

    assets = [asset for asset in all_assets if parse_sync_timestamp(asset.last_seen) >= parse_sync_timestamp(cursor)]
    edges = [edge for edge in all_edges if parse_sync_timestamp(edge.last_seen) >= parse_sync_timestamp(cursor)]
    contracts = [
        contract
        for contract in all_contracts
        if parse_sync_timestamp(contract.updated_at) >= parse_sync_timestamp(cursor)
    ]
    violations = [
        violation
        for violation in all_violations
        if parse_sync_timestamp(violation.detected_at) >= parse_sync_timestamp(cursor)
    ]

    asset_resp = await api.push_assets([asset_to_payload(asset) for asset in assets], cursor)
    edge_resp = await api.push_edges([edge_to_payload(edge) for edge in edges], cursor)
    contract_push_resp = await api.push_contracts(
        [contract_to_payload(contract) for contract in contracts],
        cursor,
    )
    violation_resp = await api.push_violations(
        [violation_to_payload(violation) for violation in violations],
        cursor,
    )

    new_cursor = latest_sync_cursor(
        asset_resp.new_cursor,
        edge_resp.new_cursor,
        contract_push_resp.new_cursor,
        violation_resp.new_cursor,
    ) or cursor

    resolver = ConflictResolver()
    contract_repo = ContractRepository(db)
    pulled_contracts = await api.pull_contracts(cursor)
    for remote in pulled_contracts:
        local = contract_repo.get(remote["id"])
        resolved = resolver.resolve_contract(contract_to_payload(local) if local else {}, remote)
        contract_repo.upsert(dict_to_contract(resolved))

    asset_repo = AssetRepository(db)
    pulled_assets = await api.pull_assets(cursor)
    for remote in pulled_assets:
        local = asset_repo.get(remote["id"])
        resolved = resolver.resolve_asset(asset_to_payload(local) if local else {}, remote)
        asset_repo.upsert(dict_to_asset(resolved))

    if new_cursor:
        cfg.save_sync_cursor(new_cursor)

    return SyncResponse(
        accepted_count=(
            asset_resp.accepted_count
            + edge_resp.accepted_count
            + contract_push_resp.accepted_count
            + violation_resp.accepted_count
        ),
        rejected=[
            *asset_resp.rejected,
            *edge_resp.rejected,
            *contract_push_resp.rejected,
            *violation_resp.rejected,
        ],
        new_cursor=new_cursor,
    )


async def run_team_sync(cfg: AtlasConfig):
    """Run a full team sync using the configured team connection."""
    from alma_atlas.application.query.service import require_db_path
    from alma_atlas.sync.client import SyncClient
    from alma_atlas_store.db import Database

    cfg.load_team_config()
    if not cfg.team_server_url or not cfg.team_api_key:
        raise ValueError("Team sync not configured. Run `alma-atlas team init` first.")

    db_path = require_db_path(cfg)
    auth = TeamAuth(cfg.team_api_key)
    async with SyncClient(cfg.team_server_url, auth, cfg.team_id or "default") as client:
        with Database(db_path) as db, db.transaction():
            return await client.full_sync(db, cfg)
