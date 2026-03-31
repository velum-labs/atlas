"""Graph edge repository — SQLite-backed implementation of EdgePort."""

from __future__ import annotations

import json
import sqlite3

from alma_atlas_store.db import Database
from alma_ports.edge import Edge as GraphEdge

# Backward-compatible alias for callers that still import `Edge` from the
# concrete store module instead of the canonical ports module.
Edge = GraphEdge


class EdgeRepository:
    """SQLite-backed repository for dependency graph edges."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, edge: GraphEdge) -> None:
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
        self._db.maybe_commit()

    def get(self, edge_id: str) -> GraphEdge | None:
        """Retrieve one edge by its canonical ID."""
        row = self._db.conn.execute("SELECT * FROM edges WHERE id = ?", (edge_id,)).fetchone()
        return self._row_to_edge(row) if row else None

    def get_upstream(self, asset_id: str) -> list[GraphEdge]:
        """Return all edges where asset_id is the downstream node."""
        rows = self._db.conn.execute("SELECT * FROM edges WHERE downstream_id = ?", (asset_id,)).fetchall()
        return [self._row_to_edge(r) for r in rows]

    def get_downstream(self, asset_id: str) -> list[GraphEdge]:
        """Return all edges where asset_id is the upstream node."""
        rows = self._db.conn.execute("SELECT * FROM edges WHERE upstream_id = ?", (asset_id,)).fetchall()
        return [self._row_to_edge(r) for r in rows]

    def list_for_asset(self, asset_id: str) -> list[GraphEdge]:
        """Return all edges touching an asset in either direction."""
        rows = self._db.conn.execute(
            "SELECT * FROM edges WHERE upstream_id = ? OR downstream_id = ? ORDER BY id",
            (asset_id, asset_id),
        ).fetchall()
        return [self._row_to_edge(r) for r in rows]

    def list_all(self) -> list[GraphEdge]:
        """Return all known edges."""
        rows = self._db.conn.execute("SELECT * FROM edges").fetchall()
        return [self._row_to_edge(r) for r in rows]

    def _row_to_edge(self, row: sqlite3.Row) -> GraphEdge:
        return GraphEdge(
            upstream_id=row["upstream_id"],
            downstream_id=row["downstream_id"],
            kind=row["kind"],
            metadata=json.loads(row["metadata"] or "{}"),
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
        )
