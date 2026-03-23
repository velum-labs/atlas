"""Domain objects for alma-connectors.

These are the data transfer objects produced by source adapters and consumed
by the alma-atlas pipeline. They are connector-layer objects, not store models —
the pipeline transforms them into store records.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnDef:
    """Column definition as reported by a source connector."""

    name: str
    type: str
    nullable: bool = True
    description: str | None = None
    mode: str | None = None  # BigQuery-style: NULLABLE, REQUIRED, REPEATED


@dataclass
class SchemaSnapshot:
    """Schema snapshot as returned by a source adapter's ``get_schema`` method."""

    asset_id: str
    source_type: str
    columns: list[ColumnDef] = field(default_factory=list)
    raw_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryRecord:
    """A single SQL query observation from a traffic scan."""

    sql: str
    source_type: str
    user: str | None = None
    timestamp: str | None = None
    execution_ms: int | None = None
    bytes_processed: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TrafficObservationResult:
    """Collection of query observations returned by a source adapter's ``get_traffic``."""

    source_id: str
    source_type: str
    queries: list[QueryRecord] = field(default_factory=list)
    error: str | None = None
