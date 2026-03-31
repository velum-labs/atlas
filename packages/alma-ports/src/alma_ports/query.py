"""Query storage protocols and shared observation record type."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass
class QueryObservation:
    """Canonical query observation shared by storage implementations."""

    fingerprint: str
    sql_text: str
    tables: list[str]
    source: str
    first_seen: str | None = None
    last_seen: str | None = None
    execution_count: int = 1


@runtime_checkable
class QueryRepository(Protocol):
    """Concrete query storage contract implemented by `alma-atlas-store`."""

    def upsert(self, query: QueryObservation) -> None: ...

    def get_by_fingerprint(self, fingerprint: str) -> QueryObservation | None: ...

    def list_for_asset(self, asset_id: str) -> list[QueryObservation]: ...

    def list_all(self) -> list[QueryObservation]: ...
