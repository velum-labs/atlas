"""Asset storage protocols and shared asset record type."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class Asset:
    """Canonical asset record shared by storage implementations."""

    id: str
    source: str
    kind: str
    name: str
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen: str | None = None
    last_seen: str | None = None


@runtime_checkable
class AssetRepository(Protocol):
    """Concrete asset storage contract implemented by `alma-atlas-store`."""

    def upsert(self, asset: Asset) -> None: ...

    def get(self, asset_id: str) -> Asset | None: ...

    def list_all(self) -> list[Asset]: ...

    def search(self, query: str) -> list[Asset]: ...

    def delete(self, asset_id: str) -> None: ...
