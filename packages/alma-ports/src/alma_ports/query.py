"""Query port — Protocol interface for query observation repositories.

Query observations record SQL queries that have been executed against assets,
including their fingerprint (normalized form), source, and traffic metadata.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class QueryPort(Protocol):
    """Protocol for persisting and retrieving query observations."""

    def upsert(self, query: object) -> None:
        """Insert or update a query observation record."""
        ...

    def get_by_fingerprint(self, fingerprint: str) -> object | None:
        """Return the query observation matching a given SQL fingerprint."""
        ...

    def list_for_asset(self, asset_id: str) -> list[object]:
        """Return all queries that reference a given asset."""
        ...

    def list_all(self) -> list[object]:
        """Return all known query observations."""
        ...
