"""Violation storage protocol and shared violation record type."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass
class Violation:
    """Canonical persisted enforcement violation for one asset."""

    asset_id: str
    violation_type: str
    severity: str
    details: dict[str, Any]
    id: str | None = None
    detected_at: str | None = None
    resolved_at: str | None = None

    def __post_init__(self) -> None:
        if self.id is None:
            self.id = str(uuid.uuid4())


@runtime_checkable
class ViolationRepository(Protocol):
    """Concrete violation storage contract implemented by `alma-atlas-store`."""

    def insert(self, violation: Violation) -> None: ...

    def list_for_asset(self, asset_id: str, include_resolved: bool = False) -> list[Violation]: ...

    def list_recent(self, limit: int = 50) -> list[Violation]: ...

    def resolve(self, violation_id: str) -> None: ...
