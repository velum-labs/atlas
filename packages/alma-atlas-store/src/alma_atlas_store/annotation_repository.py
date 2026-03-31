"""Annotation repository — SQLite-backed storage for agent-generated asset annotations.

Annotations are supplementary metadata produced by the asset enrichment agent.
They never overwrite schema snapshots or edge data and carry provenance
information (annotated_by, annotated_at) for trust calibration.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from alma_ports.annotation import AnnotationRecord

if TYPE_CHECKING:
    from alma_atlas_store.db import Database


class AnnotationRepository:
    """SQLite-backed repository for asset annotation records."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, annotation: AnnotationRecord) -> None:
        """Insert or update an annotation record for an asset."""
        self._db.conn.execute(
            """
            INSERT INTO asset_annotations
                (asset_id, ownership, granularity, join_keys, freshness_guarantee,
                 business_logic_summary, sensitivity, annotated_at, annotated_by)
            VALUES
                (:asset_id, :ownership, :granularity, :join_keys, :freshness_guarantee,
                 :business_logic_summary, :sensitivity,
                 COALESCE(:annotated_at, CURRENT_TIMESTAMP), :annotated_by)
            ON CONFLICT(asset_id) DO UPDATE SET
                ownership              = excluded.ownership,
                granularity            = excluded.granularity,
                join_keys              = excluded.join_keys,
                freshness_guarantee    = excluded.freshness_guarantee,
                business_logic_summary = excluded.business_logic_summary,
                sensitivity            = excluded.sensitivity,
                annotated_at           = COALESCE(excluded.annotated_at, CURRENT_TIMESTAMP),
                annotated_by           = excluded.annotated_by
            """,
            {
                "asset_id": annotation.asset_id,
                "ownership": annotation.ownership,
                "granularity": annotation.granularity,
                "join_keys": json.dumps(annotation.join_keys),
                "freshness_guarantee": annotation.freshness_guarantee,
                "business_logic_summary": annotation.business_logic_summary,
                "sensitivity": annotation.sensitivity,
                "annotated_at": annotation.annotated_at,
                "annotated_by": annotation.annotated_by,
            },
        )
        self._db.maybe_commit()

    def get(self, asset_id: str) -> AnnotationRecord | None:
        """Retrieve an annotation record by asset ID."""
        row = self._db.conn.execute(
            "SELECT * FROM asset_annotations WHERE asset_id = ?",
            (asset_id,),
        ).fetchone()
        return self._row_to_record(row) if row else None

    def list_unannotated(self, limit: int = 100) -> list[str]:
        """Return asset IDs that have no annotation yet.

        Performs a LEFT JOIN between the assets and asset_annotations tables.

        Args:
            limit: Maximum number of asset IDs to return.

        Returns:
            List of asset IDs without any annotation, ordered by asset ID.
        """
        rows = self._db.conn.execute(
            """
            SELECT a.id FROM assets a
            LEFT JOIN asset_annotations aa ON a.id = aa.asset_id
            WHERE aa.asset_id IS NULL
            ORDER BY a.id
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [row["id"] for row in rows]

    def list_stale(self, max_age_days: int = 7) -> list[AnnotationRecord]:
        """Return annotation records older than *max_age_days* days.

        Args:
            max_age_days: Annotations with annotated_at before this many days
                          ago are considered stale.

        Returns:
            List of stale AnnotationRecord objects, oldest first.
        """
        rows = self._db.conn.execute(
            """
            SELECT * FROM asset_annotations
            WHERE annotated_at < datetime('now', ? || ' days')
            ORDER BY annotated_at ASC
            """,
            (f"-{max_age_days}",),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def list_all(self, limit: int = 500) -> list[AnnotationRecord]:
        """Return all annotation records, newest first.

        Args:
            limit: Maximum number of records to return.

        Returns:
            List of AnnotationRecord objects ordered by annotated_at DESC.
        """
        rows = self._db.conn.execute(
            "SELECT * FROM asset_annotations ORDER BY annotated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def _row_to_record(self, row: sqlite3.Row) -> AnnotationRecord:
        return AnnotationRecord(
            asset_id=row["asset_id"],
            ownership=row["ownership"],
            granularity=row["granularity"],
            join_keys=json.loads(row["join_keys"] or "[]"),
            freshness_guarantee=row["freshness_guarantee"],
            business_logic_summary=row["business_logic_summary"],
            sensitivity=row["sensitivity"],
            annotated_at=row["annotated_at"],
            annotated_by=row["annotated_by"],
        )
