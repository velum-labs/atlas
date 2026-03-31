"""Schema storage protocols and shared schema snapshot record types."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass
class ColumnInfo:
    """A single column in a schema snapshot."""

    name: str
    type: str
    nullable: bool = True
    description: str | None = None


@dataclass
class SchemaSnapshot:
    """A point-in-time schema capture for a data asset."""

    asset_id: str
    columns: list[ColumnInfo]
    captured_at: str | None = None

    @property
    def fingerprint(self) -> str:
        col_sig = json.dumps(
            [
                (column.name, column.type, column.nullable, column.description)
                for column in self.columns
            ],
            sort_keys=True,
        )
        return hashlib.sha256(col_sig.encode()).hexdigest()[:16]

    @property
    def id(self) -> str:
        return f"{self.asset_id}:{self.fingerprint}"


@runtime_checkable
class SchemaRepository(Protocol):
    """Concrete schema storage contract implemented by `alma-atlas-store`."""

    def upsert(self, snapshot: SchemaSnapshot) -> None: ...

    def get_latest(self, asset_id: str) -> SchemaSnapshot | None: ...

    def list_history(self, asset_id: str) -> list[SchemaSnapshot]: ...
