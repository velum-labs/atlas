"""Asset repository — SQLite-backed implementation of AssetPort."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from alma_atlas_store.db import Database


@dataclass
class Asset:
    """A data asset record as stored in the Atlas SQLite database."""

    id: str
    source: str
    kind: str
    name: str
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen: str | None = None
    last_seen: str | None = None


class AssetRepository:
    """SQLite-backed repository for data asset records."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, asset: Asset) -> None:
        """Insert or update an asset record."""
        self._db.conn.execute(
            """
            INSERT INTO assets (id, source, kind, name, description, tags, metadata, last_seen)
            VALUES (:id, :source, :kind, :name, :description, :tags, :metadata, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                source      = excluded.source,
                kind        = excluded.kind,
                name        = excluded.name,
                description = excluded.description,
                tags        = excluded.tags,
                metadata    = excluded.metadata,
                last_seen   = CURRENT_TIMESTAMP
            """,
            {
                "id": asset.id,
                "source": asset.source,
                "kind": asset.kind,
                "name": asset.name,
                "description": asset.description,
                "tags": json.dumps(asset.tags),
                "metadata": json.dumps(asset.metadata),
            },
        )
        self._db.conn.commit()

    def get(self, asset_id: str) -> Asset | None:
        """Retrieve an asset by its fully-qualified ID."""
        row = self._db.conn.execute("SELECT * FROM assets WHERE id = ?", (asset_id,)).fetchone()
        return self._row_to_asset(row) if row else None

    def list_all(self) -> list[Asset]:
        """Return all known assets."""
        rows = self._db.conn.execute("SELECT * FROM assets ORDER BY id").fetchall()
        return [self._row_to_asset(r) for r in rows]

    def search(self, query: str) -> list[Asset]:
        """Full-text search over asset IDs, names, and descriptions."""
        pattern = f"%{query}%"
        rows = self._db.conn.execute(
            "SELECT * FROM assets WHERE id LIKE ? OR name LIKE ? OR description LIKE ? ORDER BY id",
            (pattern, pattern, pattern),
        ).fetchall()
        return [self._row_to_asset(r) for r in rows]

    def delete(self, asset_id: str) -> None:
        """Remove an asset and all associated cascade records."""
        self._db.conn.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
        self._db.conn.commit()

    def _row_to_asset(self, row: sqlite3.Row) -> Asset:
        return Asset(
            id=row["id"],
            source=row["source"],
            kind=row["kind"],
            name=row["name"],
            description=row["description"],
            tags=json.loads(row["tags"] or "[]"),
            metadata=json.loads(row["metadata"] or "{}"),
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
        )
