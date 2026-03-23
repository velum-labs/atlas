"""Edge repository — SQLite-backed implementation of EdgePort."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from alma_atlas_store.db import Database


@dataclass
class Edge:
    """A directed dependency edge between two data assets."""

    upstream_id: str
    downstream_id: str
    kind: str = "reads"
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen: str | None = None
    last_seen: str | None = None

    @property
    def id(self) -> str:
        return f"{self.upstream_id}:{self.downstream_id}:{self.kind}"


class EdgeRepository:
    """SQLite-backed repository for dependency edges."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, edge: Edge) -> None:
        """Insert or update an edge."""
        self._db.conn.execute(
            """
            INSERT INTO edges (id, upstream_id, downstream_id, kind, metadata, last_seen)
            VALUES (:id, :upstream_id, :downstream_id, :kind, :metadata, CURRENT_TIMESTAMP)
            ON CONFLICT(upstream_id, downstream_id, kind) DO UPDATE SET
                metadata  = excluded.metadata,
                last_seen = CURRENT_TIMESTAMP
            """,
            {
                "id": edge.id,
                "upstream_id": edge.upstream_id,
                "downstream_id": edge.downstream_id,
                "kind": edge.kind,
                "metadata": json.dumps(edge.metadata),
            },
        )
        self._db.conn.commit()

    def get_upstream(self, asset_id: str) -> list[Edge]:
        """Return all edges where asset_id is the downstream node."""
        rows = self._db.conn.execute("SELECT * FROM edges WHERE downstream_id = ?", (asset_id,)).fetchall()
        return [self._row_to_edge(r) for r in rows]

    def get_downstream(self, asset_id: str) -> list[Edge]:
        """Return all edges where asset_id is the upstream node."""
        rows = self._db.conn.execute("SELECT * FROM edges WHERE upstream_id = ?", (asset_id,)).fetchall()
        return [self._row_to_edge(r) for r in rows]

    def list_all(self) -> list[Edge]:
        """Return all known edges."""
        rows = self._db.conn.execute("SELECT * FROM edges").fetchall()
        return [self._row_to_edge(r) for r in rows]

    def _row_to_edge(self, row: sqlite3.Row) -> Edge:
        return Edge(
            upstream_id=row["upstream_id"],
            downstream_id=row["downstream_id"],
            kind=row["kind"],
            metadata=json.loads(row["metadata"] or "{}"),
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
        )
