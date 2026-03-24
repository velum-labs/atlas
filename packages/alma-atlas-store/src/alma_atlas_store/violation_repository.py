"""Violation repository — SQLite-backed store for drift enforcement violations."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Any

from alma_atlas_store.db import Database


@dataclass
class Violation:
    """A recorded enforcement violation for a data asset."""

    asset_id: str
    violation_type: str
    severity: str
    details: dict[str, Any]
    id: str | None = None
    detected_at: str | None = None
    resolved_at: str | None = None

    def __post_init__(self) -> None:
        if self.id is None:
            self.id = str(uuid.uuid4())


class ViolationRepository:
    """SQLite-backed repository for enforcement violations."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def insert(self, violation: Violation) -> None:
        """Persist a new violation record."""
        self._db.conn.execute(
            """
            INSERT INTO violations (id, asset_id, violation_type, severity, details)
            VALUES (:id, :asset_id, :violation_type, :severity, :details)
            """,
            {
                "id": violation.id,
                "asset_id": violation.asset_id,
                "violation_type": violation.violation_type,
                "severity": violation.severity,
                "details": json.dumps(violation.details),
            },
        )
        self._db.conn.commit()

    def list_for_asset(self, asset_id: str, include_resolved: bool = False) -> list[Violation]:
        """Return violations for an asset, newest first."""
        if include_resolved:
            rows = self._db.conn.execute(
                "SELECT * FROM violations WHERE asset_id = ? ORDER BY detected_at DESC",
                (asset_id,),
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                "SELECT * FROM violations WHERE asset_id = ? AND resolved_at IS NULL ORDER BY detected_at DESC",
                (asset_id,),
            ).fetchall()
        return [self._row_to_violation(r) for r in rows]

    def list_recent(self, limit: int = 50) -> list[Violation]:
        """Return the most recent unresolved violations across all assets."""
        rows = self._db.conn.execute(
            "SELECT * FROM violations WHERE resolved_at IS NULL ORDER BY detected_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_violation(r) for r in rows]

    def resolve(self, violation_id: str) -> None:
        """Mark a violation as resolved."""
        self._db.conn.execute(
            "UPDATE violations SET resolved_at = CURRENT_TIMESTAMP WHERE id = ?",
            (violation_id,),
        )
        self._db.conn.commit()

    def _row_to_violation(self, row: sqlite3.Row) -> Violation:
        return Violation(
            id=row["id"],
            asset_id=row["asset_id"],
            violation_type=row["violation_type"],
            severity=row["severity"],
            details=json.loads(row["details"]),
            detected_at=row["detected_at"],
            resolved_at=row["resolved_at"],
        )
