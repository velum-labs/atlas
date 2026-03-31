"""Shared contract selection and validation helpers."""

from __future__ import annotations

from typing import Any

from alma_ports.contract import Contract
from alma_ports.schema import SchemaSnapshot

ACTIVE_CONTRACT_STATUSES = frozenset({"active"})
INACTIVE_CONTRACT_STATUSES = frozenset({"deprecated"})


def resolve_contract_columns(payload: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Return normalized contract columns from one stored/document payload."""
    for candidate in (
        payload.get("columns"),
        payload.get("schema"),
        payload.get("spec"),
    ):
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
        if isinstance(candidate, dict):
            nested_columns = candidate.get("columns")
            if isinstance(nested_columns, list):
                return [item for item in nested_columns if isinstance(item, dict)]
    return None


def select_contracts_for_validation(contracts: list[Contract]) -> list[Contract]:
    """Return the contract(s) that should participate in validation.

    Preferred order:
    1. Any explicit `active` contracts.
    2. Otherwise the most recent non-deprecated contract, which preserves
       backward compatibility for older records that never set `status`.
    3. Deprecated contracts never validate.
    """
    active = [contract for contract in contracts if contract.status in ACTIVE_CONTRACT_STATUSES]
    if active:
        return active

    fallback = [contract for contract in contracts if contract.status not in INACTIVE_CONTRACT_STATUSES]
    return fallback[:1]


def validate_contract_columns(
    *,
    contract_id: str,
    columns: list[dict[str, Any]],
    snapshot: SchemaSnapshot | None,
) -> list[dict[str, Any]]:
    """Validate one contract column spec against a schema snapshot."""
    if snapshot is None:
        return [
            {
                "code": "missing_snapshot",
                "severity": "error",
                "message": (
                    f"[{contract_id}] No schema snapshot is available for this asset. "
                    "Run `alma-atlas scan` before validation."
                ),
            }
        ]

    actual_columns = {column.name.lower(): column for column in snapshot.columns}
    issues: list[dict[str, Any]] = []
    for column in columns:
        raw_name = column.get("name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            issues.append(
                {
                    "code": "invalid_column",
                    "severity": "error",
                    "message": f"[{contract_id}] Contract columns must include a non-empty name.",
                }
            )
            continue

        column_name = raw_name.strip()
        actual = actual_columns.get(column_name.lower())
        if actual is None:
            issues.append(
                {
                    "code": "missing_column",
                    "severity": "error",
                    "column_name": column_name,
                    "message": f"[{contract_id}] Missing column: {column_name!r}",
                }
            )
            continue

        expected_type = column.get("type")
        if isinstance(expected_type, str) and expected_type.strip():
            normalized_expected = expected_type.strip().lower()
            if actual.type.lower() != normalized_expected:
                issues.append(
                    {
                        "code": "type_mismatch",
                        "severity": "error",
                        "column_name": column_name,
                        "expected_type": expected_type,
                        "actual_type": actual.type,
                        "message": (
                            f"[{contract_id}] Type mismatch for {column_name!r}: "
                            f"expected {expected_type!r}, got {actual.type!r}"
                        ),
                    }
                )

        expected_nullable = column.get("nullable")
        if expected_nullable is False and actual.nullable:
            issues.append(
                {
                    "code": "nullability_mismatch",
                    "severity": "error",
                    "column_name": column_name,
                    "message": (
                        f"[{contract_id}] Nullability violation for {column_name!r}: "
                        "contract requires NOT NULL but column is nullable"
                    ),
                }
            )

    return issues


def validate_contract_spec(contract: Contract, snapshot: SchemaSnapshot | None) -> list[dict[str, Any]]:
    """Validate a stored contract against a schema snapshot."""
    columns = resolve_contract_columns(contract.spec)
    if columns is None:
        return [
            {
                "code": "invalid_contract",
                "severity": "error",
                "message": f"[{contract.id}] Contract spec requires columns or schema/spec.columns.",
            }
        ]
    return validate_contract_columns(contract_id=contract.id, columns=columns, snapshot=snapshot)
