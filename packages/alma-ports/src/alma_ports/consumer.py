"""Consumer port — Protocol interface for data consumer repositories.

Consumers are downstream users of assets: BI dashboards, notebooks, services,
pipelines, or individuals who run queries against data assets.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ConsumerPort(Protocol):
    """Protocol for persisting and retrieving data consumers."""

    def upsert(self, consumer: object) -> None:
        """Insert or update a consumer record."""
        ...

    def get(self, consumer_id: str) -> object | None:
        """Retrieve a consumer by ID."""
        ...

    def list_for_asset(self, asset_id: str) -> list[object]:
        """Return all consumers that depend on a given asset."""
        ...

    def list_all(self) -> list[object]:
        """Return all known consumers."""
        ...
