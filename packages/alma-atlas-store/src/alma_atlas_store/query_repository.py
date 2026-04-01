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
                sql_text        = excluded.sql_text,
                tables          = excluded.tables,
                source          = excluded.source,
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
        self._db.conn.execute("DELETE FROM query_tables WHERE fingerprint = ?", (query.fingerprint,))
        query_tables = [
            (query.fingerprint, asset_id)
            for asset_id in dict.fromkeys(query.tables)
        ]
        if query_tables:
            self._db.conn.executemany(
                """
                INSERT OR IGNORE INTO query_tables (fingerprint, asset_id)
                SELECT ?, ?
                WHERE EXISTS (SELECT 1 FROM assets WHERE id = ?)
                """,
                [(fingerprint, asset_id, asset_id) for fingerprint, asset_id in query_tables],
            )
        self._db.maybe_commit()

    def get_by_fingerprint(self, fingerprint: str) -> QueryObservation | None:
        """Return the query matching a given fingerprint."""
        row = self._db.conn.execute("SELECT * FROM queries WHERE fingerprint = ?", (fingerprint,)).fetchone()
        return self._row_to_query(row) if row else None

    def list_for_asset(self, asset_id: str) -> list[QueryObservation]:
        """Return all queries that reference a given asset ID."""
        rows = self._db.conn.execute(
            """
            SELECT q.*
            FROM query_tables qt
            JOIN queries q ON q.fingerprint = qt.fingerprint
            WHERE qt.asset_id = ?
            ORDER BY q.execution_count DESC
            """,
            (asset_id,),
        ).fetchall()
        return [self._row_to_query(r) for r in rows]

    def list_all(self) -> list[QueryObservation]:
        """Return all known query observations."""
        rows = self._db.conn.execute("SELECT * FROM queries ORDER BY execution_count DESC").fetchall()
        return [self._row_to_query(r) for r in rows]

    def list_top(self, limit: int) -> list[QueryObservation]:
        """Return the top-N query observations by execution count."""
        rows = self._db.conn.execute(
            "SELECT * FROM queries ORDER BY execution_count DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_query(r) for r in rows]

    def prune_before(self, cutoff_timestamp: str) -> None:
        """Delete query observations whose last_seen is older than the cutoff."""
        self._db.conn.execute(
            "DELETE FROM queries WHERE last_seen < ?",
            (cutoff_timestamp,),
        )
        self._db.maybe_commit()

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
