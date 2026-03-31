"""Consumer repository — SQLite-backed implementation of ConsumerPort."""

from __future__ import annotations

import json
import sqlite3

from alma_atlas_store.db import Database
from alma_ports.consumer import Consumer


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
        self._db.maybe_commit()

    def get(self, consumer_id: str) -> Consumer | None:
        """Retrieve a consumer by ID."""
        row = self._db.conn.execute(
            """
            SELECT c.*, GROUP_CONCAT(ca.asset_id) AS asset_ids
            FROM consumers c
            LEFT JOIN consumer_assets ca ON ca.consumer_id = c.id
            WHERE c.id = ?
            GROUP BY c.id
            """,
            (consumer_id,),
        ).fetchone()
        if not row:
            return None
        return self._row_to_consumer(row)

    def list_for_asset(self, asset_id: str) -> list[Consumer]:
        """Return all consumers that depend on a given asset."""
        rows = self._db.conn.execute(
            """
            SELECT c.*, GROUP_CONCAT(ca2.asset_id) AS asset_ids
            FROM consumers c
            JOIN consumer_assets ca ON ca.consumer_id = c.id AND ca.asset_id = ?
            LEFT JOIN consumer_assets ca2 ON ca2.consumer_id = c.id
            GROUP BY c.id
            """,
            (asset_id,),
        ).fetchall()
        return [self._row_to_consumer(r) for r in rows]

    def list_all(self) -> list[Consumer]:
        """Return all known consumers."""
        rows = self._db.conn.execute(
            """
            SELECT c.*, GROUP_CONCAT(ca.asset_id) AS asset_ids
            FROM consumers c
            LEFT JOIN consumer_assets ca ON ca.consumer_id = c.id
            GROUP BY c.id
            ORDER BY c.id
            """,
        ).fetchall()
        return [self._row_to_consumer(r) for r in rows]

    def _row_to_consumer(self, row: sqlite3.Row) -> Consumer:
        asset_ids_raw: str | None = row["asset_ids"]
        return Consumer(
            id=row["id"],
            kind=row["kind"],
            name=row["name"],
            source=row["source"],
            asset_ids=asset_ids_raw.split(",") if asset_ids_raw else [],
            metadata=json.loads(row["metadata"] or "{}"),
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
        )
