"""Sync-specific orchestration services."""

from __future__ import annotations

from alma_atlas.config import AtlasConfig
from alma_atlas.graph_service import require_db_path


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
        with Database(db_path) as db, db.transaction():
            return await client.full_sync(db, cfg)
