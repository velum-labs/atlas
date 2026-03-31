"""Query observation repository — SQLite-backed implementation of QueryPort."""

from __future__ import annotations

import json
import sqlite3

from alma_atlas_store.db import Database
from alma_ports.query import QueryObservation


class QueryRepository:
    """SQLite-backed repository for SQL query observations."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, query: QueryObservation) -> None:
        """Insert a new query or increment execution count for existing fingerprint."""
        self._db.conn.execute(
            """
            INSERT INTO queries (fingerprint, sql_text, tables, source, last_seen, execution_count)
            VALUES (:fingerprint, :sql_text, :tables, :source, CURRENT_TIMESTAMP, 1)
            ON CONFLICT(fingerprint) DO UPDATE SET
                last_seen       = CURRENT_TIMESTAMP,
                execution_count = execution_count + 1
            """,
            {
                "fingerprint": query.fingerprint,
                "sql_text": query.sql_text,
                "tables": json.dumps(query.tables),
                "source": query.source,
            },
        )
        self._db.maybe_commit()

    def get_by_fingerprint(self, fingerprint: str) -> QueryObservation | None:
        """Return the query matching a given fingerprint."""
        row = self._db.conn.execute("SELECT * FROM queries WHERE fingerprint = ?", (fingerprint,)).fetchone()
        return self._row_to_query(row) if row else None

    def list_for_asset(self, asset_id: str) -> list[QueryObservation]:
        """Return all queries that reference a given asset ID."""
        rows = self._db.conn.execute(
            "SELECT * FROM queries WHERE tables LIKE ?",
            (f'%"{asset_id}"%',),
        ).fetchall()
        return [self._row_to_query(r) for r in rows if asset_id in json.loads(r["tables"])]

    def list_all(self) -> list[QueryObservation]:
        """Return all known query observations."""
        rows = self._db.conn.execute("SELECT * FROM queries ORDER BY execution_count DESC").fetchall()
        return [self._row_to_query(r) for r in rows]

    def _row_to_query(self, row: sqlite3.Row) -> QueryObservation:
        return QueryObservation(
            fingerprint=row["fingerprint"],
            sql_text=row["sql_text"],
            tables=json.loads(row["tables"]),
            source=row["source"],
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
            execution_count=row["execution_count"],
        )
