"""SQLite database connection and schema management for alma-atlas-store.

Manages the SQLite connection lifecycle and runs schema migrations from
the ``migrations/`` directory in order. Uses Python's built-in ``sqlite3``
module with WAL mode enabled for concurrent read performance.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class Database:
    """Thin wrapper around a SQLite connection with auto-migration support.

    Not thread-safe. ``check_same_thread=False`` is set only to allow
    asyncio coroutines to reuse the connection across ``await`` points on a
    single OS thread. Do not share a ``Database`` instance across OS threads;
    create one instance per thread instead.
    """

    def __init__(self, path: str | Path = "alma.db") -> None:
        """Open (or create) a SQLite database at the given path.

        Args:
            path: Filesystem path for the SQLite database file.
                  Use ``:memory:`` for an in-memory database (tests).
        """
        self.path = Path(path) if path != ":memory:" else None
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._auto_commit = True
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
        self._conn.commit()
        migrations_path = Path(__file__).parent / "migrations"
        for migration_file in sorted(migrations_path.glob("*.sql")):
            name = migration_file.name
            already_applied = self._conn.execute(
                "SELECT 1 FROM _migrations WHERE name = ?", (name,)
            ).fetchone()
            if already_applied:
                continue
            sql = migration_file.read_text()
            # Execute each statement individually to avoid executescript()'s
            # implicit COMMIT. ALTER TABLE ADD COLUMN is checked for existence
            # first so that re-running after a crash (TOCTOU) is idempotent.
            for stmt in _iter_sql_statements(sql):
                if _is_add_column_stmt(stmt):
                    table, column = _parse_add_column(stmt)
                    if self._column_exists(table, column):
                        continue
                self._conn.execute(stmt)
            self._conn.execute("INSERT INTO _migrations (name) VALUES (?)", (name,))
            self._conn.commit()

    def _column_exists(self, table: str, column: str) -> bool:
        """Return True if *column* already exists in *table*."""
        rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(row["name"].lower() == column.lower() for row in rows)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def maybe_commit(self) -> None:
        """Commit immediately unless the caller opened an explicit transaction."""
        if self._auto_commit:
            self._conn.commit()

    @contextmanager
    def transaction(self):
        """Group repository writes into one commit/rollback boundary."""
        previous_auto_commit = self._auto_commit
        self._auto_commit = False
        try:
            yield self
        except Exception:
            self._conn.rollback()
            raise
        else:
            self._conn.commit()
        finally:
            self._auto_commit = previous_auto_commit

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _iter_sql_statements(sql: str):
    """Yield non-empty SQL statements from a migration script."""
    for stmt in sql.split(";"):
        # Drop lines that are blank or comment-only
        content_lines = [
            line for line in stmt.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        if content_lines:
            yield stmt.strip()


def _is_add_column_stmt(stmt: str) -> bool:
    """Return True if *stmt* is an ALTER TABLE … ADD [COLUMN] … statement."""
    return bool(re.match(r"\s*ALTER\s+TABLE\s+\S+\s+ADD\s+(COLUMN\s+)?\S+", stmt, re.IGNORECASE))


def _parse_add_column(stmt: str) -> tuple[str, str]:
    """Extract ``(table_name, column_name)`` from an ALTER TABLE ADD COLUMN statement."""
    m = re.match(r"\s*ALTER\s+TABLE\s+(\S+)\s+ADD\s+(?:COLUMN\s+)?(\S+)", stmt, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)
    return "", ""
