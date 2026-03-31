"""Canonical graph-edge types and storage ports.

Atlas currently stores one graph model in SQLite: assets connected by typed
dependency edges with opaque metadata. Higher-level connector-side transport
models may be richer, but they should adapt into this canonical graph shape
before crossing the storage boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class GraphEdge:
    """Canonical persisted edge in the Atlas graph."""

    upstream_id: str
    downstream_id: str
    kind: str = "reads"
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen: str | None = None
    last_seen: str | None = None

    @property
    def id(self) -> str:
        return f"{self.upstream_id}:{self.downstream_id}:{self.kind}"


# Backward-compatible alias for callers that used ``Edge`` as the store shape.
Edge = GraphEdge


@runtime_checkable
class EdgeRepository(Protocol):
    """Concrete graph-edge storage contract implemented by `alma-atlas-store`."""

    def upsert(self, edge: GraphEdge) -> None: ...

    def get(self, edge_id: str) -> GraphEdge | None: ...

    def list_all(self) -> list[GraphEdge]: ...

    def get_upstream(self, asset_id: str) -> list[GraphEdge]: ...

    def get_downstream(self, asset_id: str) -> list[GraphEdge]: ...

    def list_for_asset(self, asset_id: str) -> list[GraphEdge]: ...
