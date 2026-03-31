"""Runtime helpers for enforcing contracts during scan execution."""

from __future__ import annotations

import logging
from typing import Any


def run_enforcement_for_snapshot(snapshot: Any, source_id: str, db: Any) -> tuple[bool, bool]:
    """Run drift detection + enforcement for assets with contracts."""
    from alma_atlas.enforcement.drift import DriftDetector
    from alma_atlas.enforcement.engine import EnforcementEngine
    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository
    from alma_atlas_store.schema_repository import SchemaSnapshot as StoreSnapshot

    log = logging.getLogger(__name__)
    detector = DriftDetector()
    contract_repo = ContractRepository(db)
    schema_repo = SchemaRepository(db)
    engine = EnforcementEngine(db)

    any_blocked = False
    has_violations = False

    for obj in snapshot.objects:
        asset_id = f"{source_id}::{obj.schema_name}.{obj.object_name}"
        contracts = contract_repo.list_for_asset(asset_id)
        current_cols = [
            ColumnInfo(
                name=column.name,
                type=getattr(column, "data_type", None) or getattr(column, "type", "unknown"),
                nullable=getattr(column, "nullable", True),
            )
            for column in (obj.columns or [])
        ]
        current = StoreSnapshot(asset_id=asset_id, columns=current_cols)

        history = schema_repo.list_history(asset_id)
        previous = history[1] if len(history) >= 2 else None

        schema_repo.upsert(current)

        if not contracts:
            continue

        report = detector.detect(asset_id, previous, current)
        if not report.has_violations:
            continue

        has_violations = True
        for contract in contracts:
            result = engine.enforce(report, contract.mode)
            if result.blocked:
                any_blocked = True
                log.warning(
                    "[enforcement/enforce] Pipeline BLOCKED for asset %s — %d error violation(s) detected.",
                    asset_id,
                    report.error_count,
                )

    return any_blocked, has_violations
