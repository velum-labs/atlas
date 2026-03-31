"""Schema snapshot repository — SQLite-backed implementation of SchemaPort."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from alma_atlas_store.db import Database


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


class SchemaRepository:
    """SQLite-backed repository for schema snapshots."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, snapshot: SchemaSnapshot) -> None:
        """Insert a schema snapshot (no-op if fingerprint already exists for this asset)."""
        self._db.conn.execute(
            """
            INSERT OR IGNORE INTO schema_snapshots (id, asset_id, columns, fingerprint)
            VALUES (:id, :asset_id, :columns, :fingerprint)
            """,
            {
                "id": snapshot.id,
                "asset_id": snapshot.asset_id,
                "columns": json.dumps([vars(c) for c in snapshot.columns]),
                "fingerprint": snapshot.fingerprint,
            },
        )
        self._db.conn.commit()

    def get_latest(self, asset_id: str) -> SchemaSnapshot | None:
        """Return the most recent schema snapshot for an asset."""
        row = self._db.conn.execute(
            "SELECT * FROM schema_snapshots WHERE asset_id = ? ORDER BY captured_at DESC LIMIT 1",
            (asset_id,),
        ).fetchone()
        return self._row_to_snapshot(row) if row else None

    def list_history(self, asset_id: str) -> list[SchemaSnapshot]:
        """Return all historical snapshots for an asset, newest first."""
        rows = self._db.conn.execute(
            "SELECT * FROM schema_snapshots WHERE asset_id = ? ORDER BY captured_at DESC",
            (asset_id,),
        ).fetchall()
        return [self._row_to_snapshot(r) for r in rows]

    def _row_to_snapshot(self, row: sqlite3.Row) -> SchemaSnapshot:
        columns = [ColumnInfo(**c) for c in json.loads(row["columns"])]
        return SchemaSnapshot(
            asset_id=row["asset_id"],
            columns=columns,
            captured_at=row["captured_at"],
        )
