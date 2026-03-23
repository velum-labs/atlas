"""Edge port — Protocol interface for dependency edge repositories.

Edges represent directed relationships between assets in the dependency graph,
e.g. table A is read by query B, or model C depends on table D.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class EdgePort(Protocol):
    """Protocol for persisting and retrieving dependency edges."""

    def upsert(self, edge: object) -> None:
        """Insert or update a directed edge between two assets."""
        ...

    def get_upstream(self, asset_id: str) -> list[object]:
        """Return all edges where asset_id is the downstream node."""
        ...

    def get_downstream(self, asset_id: str) -> list[object]:
        """Return all edges where asset_id is the upstream node."""
        ...

    def list_all(self) -> list[object]:
        """Return all known edges."""
        ...
