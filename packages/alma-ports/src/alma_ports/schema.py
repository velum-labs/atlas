"""Schema port — Protocol interface for schema snapshot repositories.

Schema snapshots capture the column-level structure of an asset at a point
in time, enabling schema drift detection and impact analysis.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SchemaPort(Protocol):
    """Protocol for persisting and retrieving schema snapshots."""

    def upsert(self, snapshot: object) -> None:
        """Insert or update a schema snapshot for an asset."""
        ...

    def get_latest(self, asset_id: str) -> object | None:
        """Return the most recent schema snapshot for an asset."""
        ...

    def list_history(self, asset_id: str) -> list[object]:
        """Return all historical schema snapshots for an asset, newest first."""
        ...
