"""SQLite database connection and schema management for alma-atlas-store.

Manages the SQLite connection lifecycle and runs schema migrations from
the ``migrations/`` directory in order. Uses Python's built-in ``sqlite3``
module with WAL mode enabled for concurrent read performance.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


class Database:
    """Thin wrapper around a SQLite connection with auto-migration support."""

    def __init__(self, path: str | Path = "alma.db") -> None:
        """Open (or create) a SQLite database at the given path.

        Args:
            path: Filesystem path for the SQLite database file.
                  Use ``:memory:`` for an in-memory database (tests).
        """
        self.path = Path(path) if path != ":memory:" else Path(path)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    @property
    def conn(self) -> sqlite3.Connection:
        """The underlying SQLite connection."""
        return self._conn

    def _migrate(self) -> None:
        """Run pending migrations from the migrations/ package directory."""
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY, applied_at TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        migrations_path = Path(__file__).parent / "migrations"
        migration_files = sorted(migrations_path.glob("*.sql"))
        for migration_file in migration_files:
            name = migration_file.name
            already_applied = self._conn.execute("SELECT 1 FROM _migrations WHERE name = ?", (name,)).fetchone()
            if already_applied:
                continue
            sql = migration_file.read_text()
            self._conn.executescript(sql)
            self._conn.execute("INSERT INTO _migrations (name) VALUES (?)", (name,))
            self._conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
