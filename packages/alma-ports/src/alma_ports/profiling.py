from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class ColumnProfile:
    asset_id: str
    column_name: str
    distinct_count: int | None = None
    null_count: int | None = None
    null_fraction: float | None = None
    min_value: str | None = None
    max_value: str | None = None
    top_values: list[dict[str, Any]] = field(default_factory=list)  # [{"value": "X", "count": 42}]
    sample_values: list[str] = field(default_factory=list)
    profiled_at: str | None = None


@runtime_checkable
class ProfilingRepository(Protocol):
    def upsert(self, profile: ColumnProfile) -> None: ...
    def get(self, asset_id: str, column_name: str) -> ColumnProfile | None: ...
    def list_for_asset(self, asset_id: str) -> list[ColumnProfile]: ...
