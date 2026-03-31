"""Tests for shared contract validation helpers."""

from __future__ import annotations

from alma_atlas.contract_validation import (
    select_contracts_for_validation,
    validate_contract_spec,
)
from alma_ports.contract import Contract
from alma_ports.schema import ColumnInfo, SchemaSnapshot


def _snapshot() -> SchemaSnapshot:
    return SchemaSnapshot(
        asset_id="pg::public.orders",
        columns=[ColumnInfo(name="id", type="INTEGER", nullable=False)],
    )


def test_select_contracts_prefers_active() -> None:
    contracts = [
        Contract(id="draft", asset_id="a", version="1", spec={}, status="draft"),
        Contract(id="active", asset_id="a", version="2", spec={}, status="active"),
    ]

    selected = select_contracts_for_validation(contracts)

    assert [contract.id for contract in selected] == ["active"]


def test_select_contracts_falls_back_to_latest_non_deprecated() -> None:
    contracts = [
        Contract(id="draft-latest", asset_id="a", version="2", spec={}, status="draft"),
        Contract(id="deprecated", asset_id="a", version="1", spec={}, status="deprecated"),
    ]

    selected = select_contracts_for_validation(contracts)

    assert [contract.id for contract in selected] == ["draft-latest"]


def test_validate_contract_spec_supports_nested_spec_columns() -> None:
    contract = Contract(
        id="contract.orders",
        asset_id="pg::public.orders",
        version="1",
        spec={"spec": {"columns": [{"name": "id", "type": "INTEGER", "nullable": False}]}},
        status="active",
    )

    issues = validate_contract_spec(contract, _snapshot())

    assert issues == []
