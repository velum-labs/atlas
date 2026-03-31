"""Shared orchestration for contract validation workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from alma_atlas.contract_validation import (
    select_contracts_for_validation,
    validate_contract_columns,
    validate_contract_spec,
)
from alma_ports.contract import Contract
from alma_ports.schema import SchemaSnapshot


@dataclass(frozen=True)
class ContractValidationCheck:
    contract_id: str
    asset_id: str
    issues: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.issues


def validate_stored_contracts_for_asset(db: Any, asset_id: str) -> list[ContractValidationCheck]:
    """Validate stored contracts for one asset against the latest stored schema."""
    from alma_atlas_store.session import AtlasStoreSession

    session = AtlasStoreSession.from_db(db)
    contracts = select_contracts_for_validation(session.contracts.list_for_asset(asset_id))
    snapshot = session.schemas.get_latest(asset_id)
    return validate_snapshot_contracts(asset_id=asset_id, contracts=contracts, snapshot=snapshot)


def validate_snapshot_contracts(
    *,
    asset_id: str,
    contracts: list[Contract],
    snapshot: SchemaSnapshot | None,
) -> list[ContractValidationCheck]:
    """Validate a set of contracts against one snapshot."""
    return [
        ContractValidationCheck(
            contract_id=contract.id,
            asset_id=asset_id,
            issues=validate_contract_spec(contract, snapshot),
        )
        for contract in select_contracts_for_validation(contracts)
    ]


def validate_contract_document(
    *,
    contract_id: str,
    asset_id: str,
    columns: list[dict[str, Any]],
    snapshot: SchemaSnapshot | None,
) -> ContractValidationCheck:
    """Validate one explicit contract document against a snapshot."""
    return ContractValidationCheck(
        contract_id=contract_id,
        asset_id=asset_id,
        issues=validate_contract_columns(
            contract_id=contract_id,
            columns=columns,
            snapshot=snapshot,
        ),
    )
