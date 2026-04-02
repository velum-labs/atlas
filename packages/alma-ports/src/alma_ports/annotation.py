"""Annotation storage protocol and shared annotation record type."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class AnnotationRecord:
    """Canonical persisted annotation for a single asset."""

    asset_id: str
    ownership: str | None = None
    granularity: str | None = None
    join_keys: list[str] = field(default_factory=list)
    freshness_guarantee: str | None = None
    business_logic_summary: str | None = None
    sensitivity: str | None = None
    annotated_at: str | None = None
    annotated_by: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class AnnotationRepository(Protocol):
    """Concrete annotation storage contract implemented by `alma-atlas-store`."""

    def upsert(self, annotation: AnnotationRecord) -> None: ...

    def get(self, asset_id: str) -> AnnotationRecord | None: ...

    def list_unannotated(self, limit: int = 100) -> list[str]: ...

    def list_stale(self, max_age_days: int = 7) -> list[AnnotationRecord]: ...

    def list_all(self, limit: int = 500) -> list[AnnotationRecord]: ...
