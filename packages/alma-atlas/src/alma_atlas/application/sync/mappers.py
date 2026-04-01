"""Pure mapping helpers for team sync workflows."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any

NULL_SYNC_CURSOR = "1970-01-01T00:00:00Z"


def parse_sync_timestamp(ts: str | None) -> datetime:
    if not ts:
        return datetime.min.replace(tzinfo=UTC)
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def latest_sync_cursor(*cursors: str) -> str:
    latest = ""
    latest_dt = datetime.min.replace(tzinfo=UTC)
    for cursor in cursors:
        if not cursor:
            continue
        parsed = parse_sync_timestamp(cursor)
        if parsed >= latest_dt:
            latest_dt = parsed
            latest = cursor
    return latest


def to_payload_dict(value: Any) -> dict[str, Any]:
    return dataclasses.asdict(value) if dataclasses.is_dataclass(value) else dict(value)


def asset_to_payload(asset: Any) -> dict[str, Any]:
    return to_payload_dict(asset)


def edge_to_payload(edge: Any) -> dict[str, Any]:
    payload = to_payload_dict(edge)
    payload.setdefault("id", edge.id if hasattr(edge, "id") else "")
    return payload


def contract_to_payload(contract: Any) -> dict[str, Any]:
    return to_payload_dict(contract)


def violation_to_payload(violation: Any) -> dict[str, Any]:
    return to_payload_dict(violation)


def dict_to_asset(payload: dict[str, Any]) -> Any:
    from alma_atlas_store.asset_repository import Asset

    return Asset(
        id=payload["id"],
        source=payload.get("source", ""),
        kind=payload.get("kind", ""),
        name=payload.get("name", ""),
        description=payload.get("description"),
        tags=payload.get("tags", []),
        metadata=payload.get("metadata", {}),
        first_seen=payload.get("first_seen"),
        last_seen=payload.get("last_seen"),
    )


def dict_to_contract(payload: dict[str, Any]) -> Any:
    from alma_atlas_store.contract_repository import Contract

    return Contract(
        id=payload["id"],
        asset_id=payload.get("asset_id", ""),
        version=payload.get("version", "1"),
        spec=payload.get("spec", {}),
        status=payload.get("status", "draft"),
        mode=payload.get("mode", "shadow"),
        created_at=payload.get("created_at"),
        updated_at=payload.get("updated_at"),
    )
