"""Contract port — Protocol interface for data contract repositories.

Data contracts define the expected schema, SLAs, and quality guarantees
for an asset. They can be defined manually or inferred from usage patterns.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ContractPort(Protocol):
    """Protocol for persisting and retrieving data contracts."""

    def upsert(self, contract: object) -> None:
        """Insert or update a data contract."""
        ...

    def get(self, contract_id: str) -> object | None:
        """Retrieve a contract by ID."""
        ...

    def list_for_asset(self, asset_id: str) -> list[object]:
        """Return all contracts associated with a given asset."""
        ...

    def list_all(self) -> list[object]:
        """Return all known data contracts."""
        ...
