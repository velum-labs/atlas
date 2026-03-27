"""Query observation repository — SQLite-backed implementation of QueryPort."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from alma_atlas_store.db import Database


@dataclass
class QueryObservation:
    """A deduplicated SQL query observation."""

    fingerprint: str
    sql_text: str
    tables: list[str]
    source: str
    first_seen: str | None = None
    last_seen: str | None = None
    execution_count: int = 1


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
        self._db.conn.commit()

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

    def list_for_source(self, source: str) -> list[QueryObservation]:
        """Return all queries observed for a specific source ID."""
        return self.list_all(source=source)

    def list_all(self, source: str | None = None) -> list[QueryObservation]:
        """Return all known query observations."""
        if source is None:
            rows = self._db.conn.execute(
                "SELECT * FROM queries ORDER BY execution_count DESC"
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                "SELECT * FROM queries WHERE source = ? ORDER BY execution_count DESC",
                (source,),
            ).fetchall()
        return [self._row_to_query(r) for r in rows]

    def list_top(
        self,
        *,
        limit: int = 20,
        source: str | None = None,
    ) -> list[QueryObservation]:
        """Return the highest-volume query observations."""
        limit = max(1, limit)
        if source is None:
            rows = self._db.conn.execute(
                "SELECT * FROM queries ORDER BY execution_count DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                "SELECT * FROM queries WHERE source = ? ORDER BY execution_count DESC LIMIT ?",
                (source, limit),
            ).fetchall()
        return [self._row_to_query(r) for r in rows]

    def count_all(self, source: str | None = None) -> int:
        """Return the total number of stored query fingerprints."""
        if source is None:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS count FROM queries"
            ).fetchone()
        else:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS count FROM queries WHERE source = ?",
                (source,),
            ).fetchone()
        return int(row["count"]) if row else 0

    def sum_execution_count(self, source: str | None = None) -> int:
        """Return the sum of execution counts across stored fingerprints."""
        if source is None:
            row = self._db.conn.execute(
                "SELECT COALESCE(SUM(execution_count), 0) AS total FROM queries"
            ).fetchone()
        else:
            row = self._db.conn.execute(
                "SELECT COALESCE(SUM(execution_count), 0) AS total FROM queries WHERE source = ?",
                (source,),
            ).fetchone()
        return int(row["total"]) if row else 0

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
