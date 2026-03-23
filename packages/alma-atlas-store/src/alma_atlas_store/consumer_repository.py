"""Consumer repository — SQLite-backed implementation of ConsumerPort."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from alma_atlas_store.db import Database


@dataclass
class Consumer:
    """A data consumer (user, service, dashboard, notebook, etc.)."""

    id: str
    kind: str
    name: str
    source: str
    asset_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen: str | None = None
    last_seen: str | None = None


class ConsumerRepository:
    """SQLite-backed repository for data consumers."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, consumer: Consumer) -> None:
        """Insert or update a consumer and its asset associations."""
        self._db.conn.execute(
            """
            INSERT INTO consumers (id, kind, name, source, metadata, last_seen)
            VALUES (:id, :kind, :name, :source, :metadata, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                kind      = excluded.kind,
                name      = excluded.name,
                source    = excluded.source,
                metadata  = excluded.metadata,
                last_seen = CURRENT_TIMESTAMP
            """,
            {
                "id": consumer.id,
                "kind": consumer.kind,
                "name": consumer.name,
                "source": consumer.source,
                "metadata": json.dumps(consumer.metadata),
            },
        )
        for asset_id in consumer.asset_ids:
            self._db.conn.execute(
                "INSERT OR IGNORE INTO consumer_assets (consumer_id, asset_id) VALUES (?, ?)",
                (consumer.id, asset_id),
            )
        self._db.conn.commit()

    def get(self, consumer_id: str) -> Consumer | None:
        """Retrieve a consumer by ID."""
        row = self._db.conn.execute("SELECT * FROM consumers WHERE id = ?", (consumer_id,)).fetchone()
        if not row:
            return None
        return self._row_to_consumer(row)

    def list_for_asset(self, asset_id: str) -> list[Consumer]:
        """Return all consumers that depend on a given asset."""
        rows = self._db.conn.execute(
            """
            SELECT c.* FROM consumers c
            JOIN consumer_assets ca ON ca.consumer_id = c.id
            WHERE ca.asset_id = ?
            """,
            (asset_id,),
        ).fetchall()
        return [self._row_to_consumer(r) for r in rows]

    def list_all(self) -> list[Consumer]:
        """Return all known consumers."""
        rows = self._db.conn.execute("SELECT * FROM consumers ORDER BY id").fetchall()
        return [self._row_to_consumer(r) for r in rows]

    def _row_to_consumer(self, row: sqlite3.Row) -> Consumer:
        asset_rows = self._db.conn.execute(
            "SELECT asset_id FROM consumer_assets WHERE consumer_id = ?", (row["id"],)
        ).fetchall()
        return Consumer(
            id=row["id"],
            kind=row["kind"],
            name=row["name"],
            source=row["source"],
            asset_ids=[r["asset_id"] for r in asset_rows],
            metadata=json.loads(row["metadata"] or "{}"),
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
        )
