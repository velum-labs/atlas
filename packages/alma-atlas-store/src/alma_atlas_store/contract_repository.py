"""Contract repository — SQLite-backed implementation of ContractPort."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from alma_ports.contract import Contract
from alma_atlas_store.db import Database


class ContractRepository:
    """SQLite-backed repository for data contracts."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert(self, contract: Contract) -> None:
        """Insert or update a data contract."""
        self._db.conn.execute(
            """
            INSERT INTO contracts (id, asset_id, version, spec, status, mode, updated_at)
            VALUES (:id, :asset_id, :version, :spec, :status, :mode, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                version    = excluded.version,
                spec       = excluded.spec,
                status     = excluded.status,
                mode       = excluded.mode,
                updated_at = CURRENT_TIMESTAMP
            """,
            {
                "id": contract.id,
                "asset_id": contract.asset_id,
                "version": contract.version,
                "spec": json.dumps(contract.spec),
                "status": contract.status,
                "mode": contract.mode,
            },
        )
        self._db.conn.commit()

    def get(self, contract_id: str) -> Contract | None:
        """Retrieve a contract by ID."""
        row = self._db.conn.execute("SELECT * FROM contracts WHERE id = ?", (contract_id,)).fetchone()
        return self._row_to_contract(row) if row else None

    def list_for_asset(self, asset_id: str) -> list[Contract]:
        """Return all contracts associated with a given asset."""
        rows = self._db.conn.execute(
            "SELECT * FROM contracts WHERE asset_id = ? ORDER BY updated_at DESC", (asset_id,)
        ).fetchall()
        return [self._row_to_contract(r) for r in rows]

    def list_all(self) -> list[Contract]:
        """Return all known data contracts."""
        rows = self._db.conn.execute("SELECT * FROM contracts ORDER BY updated_at DESC").fetchall()
        return [self._row_to_contract(r) for r in rows]

    def _row_to_contract(self, row: sqlite3.Row) -> Contract:
        return Contract(
            id=row["id"],
            asset_id=row["asset_id"],
            version=row["version"],
            spec=json.loads(row["spec"]),
            status=row["status"],
            mode=row["mode"] if row["mode"] in ("shadow", "warn", "enforce") else "shadow",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
