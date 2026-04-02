"""Business term domain model and storage protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass
class BusinessTerm:
    name: str
    definition: str | None = None
    formula: str | None = None
    referenced_columns: list[str] = field(default_factory=list)
    source: str = "manual"  # "manual" | "learned"
    created_at: str | None = None


@runtime_checkable
class BusinessTermRepository(Protocol):
    def upsert(self, term: BusinessTerm) -> None: ...
    def get(self, name: str) -> BusinessTerm | None: ...
    def search(self, query: str) -> list[BusinessTerm]: ...
    def list_all(self, limit: int = 100) -> list[BusinessTerm]: ...
