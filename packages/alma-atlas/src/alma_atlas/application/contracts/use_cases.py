"""Application-layer contract validation entrypoints."""

from __future__ import annotations

from alma_atlas.application.query.service import require_db_path
from alma_atlas.config import AtlasConfig
from alma_atlas.contract_service import validate_stored_contracts_for_asset


def check_asset_contracts(cfg: AtlasConfig, asset_id: str):
    """Validate stored contracts for one asset against the latest stored schema."""
    from alma_atlas_store.db import Database

    with Database(require_db_path(cfg)) as db:
        return validate_stored_contracts_for_asset(db, asset_id)
