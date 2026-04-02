"""Column profile repository -- SQLite-backed implementation of ProfilingRepository."""

from __future__ import annotations

import json
import sqlite3

from alma_atlas_store.db import Database
from alma_ports.profiling import ColumnProfile


class ProfilingRepository:
    """SQLite-backed repository for column profiling data."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, profile: ColumnProfile) -> None:
        """Insert or replace a column profile."""
        self._db.conn.execute(
            """
            INSERT OR REPLACE INTO column_profiles
                (asset_id, column_name, distinct_count, null_count, null_fraction,
                 min_value, max_value, top_values, sample_values, profiled_at)
            VALUES
                (:asset_id, :column_name, :distinct_count, :null_count, :null_fraction,
                 :min_value, :max_value, :top_values, :sample_values, :profiled_at)
            """,
            {
                "asset_id": profile.asset_id,
                "column_name": profile.column_name,
                "distinct_count": profile.distinct_count,
                "null_count": profile.null_count,
                "null_fraction": profile.null_fraction,
                "min_value": profile.min_value,
                "max_value": profile.max_value,
                "top_values": json.dumps(profile.top_values),
                "sample_values": json.dumps(profile.sample_values),
                "profiled_at": profile.profiled_at,
            },
        )
        self._db.maybe_commit()

    def get(self, asset_id: str, column_name: str) -> ColumnProfile | None:
        """Return the profile for a specific column, or None if not found."""
        row = self._db.conn.execute(
            "SELECT * FROM column_profiles WHERE asset_id = ? AND column_name = ?",
            (asset_id, column_name),
        ).fetchone()
        return self._row_to_profile(row) if row else None

    def list_for_asset(self, asset_id: str) -> list[ColumnProfile]:
        """Return all column profiles for an asset, ordered by column name."""
        rows = self._db.conn.execute(
            "SELECT * FROM column_profiles WHERE asset_id = ? ORDER BY column_name",
            (asset_id,),
        ).fetchall()
        return [self._row_to_profile(r) for r in rows]

    def _row_to_profile(self, row: sqlite3.Row) -> ColumnProfile:
        return ColumnProfile(
            asset_id=row["asset_id"],
            column_name=row["column_name"],
            distinct_count=row["distinct_count"],
            null_count=row["null_count"],
            null_fraction=row["null_fraction"],
            min_value=row["min_value"],
            max_value=row["max_value"],
            top_values=json.loads(row["top_values"]),
            sample_values=json.loads(row["sample_values"]),
            profiled_at=row["profiled_at"],
        )
