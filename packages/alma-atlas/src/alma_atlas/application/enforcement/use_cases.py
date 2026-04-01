"""Application-layer enforcement use cases."""

from __future__ import annotations

import logging
from typing import Any

from alma_atlas.contract_service import validate_snapshot_contracts
from alma_atlas.enforcement.drift import DriftReport, DriftViolation


def run_enforcement_for_snapshot(snapshot: Any, source_id: str, db: Any) -> tuple[bool, bool]:
    """Validate current snapshots against stored contracts."""
    from alma_atlas.enforcement.engine import EnforcementEngine
    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository
    from alma_atlas_store.schema_repository import SchemaSnapshot as StoreSnapshot

    log = logging.getLogger(__name__)
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
        schema_repo.upsert(current)

        if not contracts:
            continue

        for check in validate_snapshot_contracts(
            asset_id=asset_id,
            contracts=contracts,
            snapshot=current,
        ):
            if check.passed:
                continue

            has_violations = True
            report = DriftReport(
                violations=[
                    DriftViolation(
                        asset_id=asset_id,
                        violation_type=str(issue["code"]),
                        severity=str(issue["severity"]),
                        details={
                            **issue,
                            "contract_id": check.contract_id,
                        },
                    )
                    for issue in check.issues
                ]
            )
            contract = next(contract for contract in contracts if contract.id == check.contract_id)
            result = engine.enforce(report, contract.mode)
            if result.blocked:
                any_blocked = True
                log.warning(
                    "[enforcement/enforce] Pipeline BLOCKED for asset %s contract %s — %d error violation(s) detected.",
                    asset_id,
                    check.contract_id,
                    report.error_count,
                )

    return any_blocked, has_violations
