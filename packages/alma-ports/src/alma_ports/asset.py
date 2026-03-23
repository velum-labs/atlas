"""Asset port — Protocol interface for data asset repositories.

A data asset represents any named, addressable entity in the data stack:
tables, views, models, dashboards, etc.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class AssetPort(Protocol):
    """Protocol for persisting and retrieving data assets."""

    def upsert(self, asset: object) -> None:
        """Insert or update an asset record."""
        ...

    def get(self, asset_id: str) -> object | None:
        """Retrieve an asset by its fully-qualified ID."""
        ...

    def list_all(self) -> list[object]:
        """Return all known assets."""
        ...

    def search(self, query: str) -> list[object]:
        """Full-text search over asset names and descriptions."""
        ...

    def delete(self, asset_id: str) -> None:
        """Remove an asset and all associated metadata."""
        ...
