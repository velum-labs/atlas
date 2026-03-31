"""Contract storage protocols and shared contract record type."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable


@dataclass
class Contract:
    """Canonical contract record shared by storage implementations."""

    id: str
    asset_id: str
    version: str
    spec: dict[str, Any]
    status: str = "draft"
    mode: Literal["shadow", "warn", "enforce"] = "shadow"
    created_at: str | None = None
    updated_at: str | None = None


@runtime_checkable
class ContractRepository(Protocol):
    """Concrete contract storage contract implemented by `alma-atlas-store`."""

    def upsert(self, contract: Contract) -> None: ...

    def get(self, contract_id: str) -> Contract | None: ...

    def list_for_asset(self, asset_id: str) -> list[Contract]: ...

    def list_all(self) -> list[Contract]: ...
