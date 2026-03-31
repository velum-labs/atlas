"""Runtime helpers for enforcing contracts during scan execution."""

from __future__ import annotations

import logging
from typing import Any

from alma_atlas.contract_validation import (
    select_contracts_for_validation,
    validate_contract_spec,
)
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
        contracts = select_contracts_for_validation(contract_repo.list_for_asset(asset_id))
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

        for contract in contracts:
            issues = validate_contract_spec(contract, current)
            if not issues:
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
                            "contract_id": contract.id,
                        },
                    )
                    for issue in issues
                ]
            )
            result = engine.enforce(report, contract.mode)
            if result.blocked:
                any_blocked = True
                log.warning(
                    "[enforcement/enforce] Pipeline BLOCKED for asset %s contract %s — %d error violation(s) detected.",
                    asset_id,
                    contract.id,
                    report.error_count,
                )

    return any_blocked, has_violations
