"""Business term repository -- SQLite-backed storage for business glossary terms."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from alma_ports.business_term import BusinessTerm

if TYPE_CHECKING:
    from alma_atlas_store.db import Database


class BusinessTermRepository:
    """SQLite-backed repository for business glossary terms."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, term: BusinessTerm) -> None:
        """Insert or update a business term."""
        self._db.conn.execute(
            """
            INSERT INTO business_terms
                (name, definition, formula, referenced_columns, source, created_at)
            VALUES
                (:name, :definition, :formula, :referenced_columns, :source,
                 COALESCE(:created_at, CURRENT_TIMESTAMP))
            ON CONFLICT(name) DO UPDATE SET
                definition         = excluded.definition,
                formula            = excluded.formula,
                referenced_columns = excluded.referenced_columns,
                source             = excluded.source,
                created_at         = COALESCE(excluded.created_at, created_at)
            """,
            {
                "name": term.name,
                "definition": term.definition,
                "formula": term.formula,
                "referenced_columns": json.dumps(term.referenced_columns),
                "source": term.source,
                "created_at": term.created_at,
            },
        )
        self._db.maybe_commit()

    def get(self, name: str) -> BusinessTerm | None:
        """Retrieve a business term by exact name."""
        row = self._db.conn.execute(
            "SELECT * FROM business_terms WHERE name = ?",
            (name,),
        ).fetchone()
        return self._row_to_term(row) if row else None

    def search(self, query: str) -> list[BusinessTerm]:
        """Search business terms by name or definition fragment."""
        pattern = f"%{query}%"
        rows = self._db.conn.execute(
            """
            SELECT * FROM business_terms
            WHERE name LIKE ? OR definition LIKE ?
            ORDER BY name
            """,
            (pattern, pattern),
        ).fetchall()
        return [self._row_to_term(r) for r in rows]

    def list_all(self, limit: int = 100) -> list[BusinessTerm]:
        """Return all business terms ordered by name."""
        rows = self._db.conn.execute(
            "SELECT * FROM business_terms ORDER BY name LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_term(r) for r in rows]

    def _row_to_term(self, row: sqlite3.Row) -> BusinessTerm:
        return BusinessTerm(
            name=row["name"],
            definition=row["definition"],
            formula=row["formula"],
            referenced_columns=json.loads(row["referenced_columns"] or "[]"),
            source=row["source"] or "manual",
            created_at=row["created_at"],
        )
