"""Simple SQLite-backed cache for Atlas agent responses (Phase 4)."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alma_atlas_store.db import Database


class AgentCache:
    """Cache Atlas agent responses keyed by (question, db_id, schema_fingerprint).

    Entries expire after 1 day. The cache table is created lazily on first use.
    """

    def __init__(self, db: Database) -> None:
        self._db = db
        self._ensure_table()

    def get(self, question: str, db_id: str, schema_fingerprint: str) -> dict | None:
        """Return a cached response, or None if not found / expired."""
        key = self._make_key(question, db_id, schema_fingerprint)
        row = self._db.conn.execute(
            "SELECT response FROM agent_cache WHERE cache_key = ? AND created_at > datetime('now', '-1 day')",
            (key,),
        ).fetchone()
        return json.loads(row["response"]) if row else None

    def put(self, question: str, db_id: str, schema_fingerprint: str, response: dict) -> None:
        """Store a response in the cache."""
        key = self._make_key(question, db_id, schema_fingerprint)
        self._db.conn.execute(
            "INSERT OR REPLACE INTO agent_cache (cache_key, question, db_id, response, created_at) "
            "VALUES (?, ?, ?, ?, datetime('now'))",
            (key, question, db_id, json.dumps(response)),
        )
        self._db.maybe_commit()

    def _make_key(self, question: str, db_id: str, fp: str) -> str:
        return hashlib.sha256(f"{question}:{db_id}:{fp}".encode()).hexdigest()[:32]

    def _ensure_table(self) -> None:
        self._db.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_cache (
                cache_key TEXT PRIMARY KEY,
                question  TEXT,
                db_id     TEXT,
                response  TEXT,
                created_at TEXT
            )
            """
        )
        self._db.maybe_commit()
