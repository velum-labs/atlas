"""Consumer storage protocols and shared consumer record type."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class Consumer:
    """Canonical consumer record shared by storage implementations."""

    id: str
    kind: str
    name: str
    source: str
    asset_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen: str | None = None
    last_seen: str | None = None


@runtime_checkable
class ConsumerRepository(Protocol):
    """Concrete consumer storage contract implemented by `alma-atlas-store`."""

    def upsert(self, consumer: Consumer) -> None: ...

    def get(self, consumer_id: str) -> Consumer | None: ...

    def list_for_asset(self, asset_id: str) -> list[Consumer]: ...

    def list_all(self) -> list[Consumer]: ...
