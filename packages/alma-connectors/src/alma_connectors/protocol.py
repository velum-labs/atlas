"""SourceAdapter protocol for alma-connectors.

Defines the interface that all source connector implementations must satisfy.
This protocol is intentionally minimal — connectors only need to implement
what they can actually observe from their source system.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from alma_connectors.domain import SchemaSnapshot, TrafficObservationResult


@runtime_checkable
class SourceAdapter(Protocol):
    """Protocol for data source adapters.

    Each adapter connects to one data platform (BigQuery, Snowflake, etc.)
    and surfaces the assets, schemas, and query traffic observed there.
    """

    @property
    def source_id(self) -> str:
        """Unique identifier for this source (e.g. ``bigquery:my-project``)."""
        ...

    @property
    def source_type(self) -> str:
        """Source platform type: ``bigquery``, ``snowflake``, ``postgres``, ``dbt``."""
        ...

    def list_assets(self) -> list[dict]:
        """Return all discoverable assets from this source.

        Each dict must contain at minimum: ``id``, ``name``, ``kind``.
        """
        ...

    def get_schema(self, asset_id: str) -> SchemaSnapshot | None:
        """Return the current schema for a given asset, or None if unavailable."""
        ...

    def get_traffic(self) -> TrafficObservationResult:
        """Return recent query traffic observations from this source."""
        ...
