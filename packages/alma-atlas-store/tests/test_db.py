"""Tests for alma_atlas_store.db.Database."""

from __future__ import annotations

from alma_atlas_store.db import Database


def test_tables_created_on_init(db):
    tables = {row[0] for row in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    expected = {
        "assets",
        "edges",
        "schema_snapshots",
        "queries",
        "consumers",
        "consumer_assets",
        "contracts",
        "_migrations",
    }
    assert expected.issubset(tables)


def test_wal_mode_enabled(tmp_path):
    db_path = tmp_path / "test.db"
    with Database(db_path) as d:
        row = d.conn.execute("PRAGMA journal_mode").fetchone()
    assert row[0] == "wal"


def test_foreign_keys_enabled(db):
    row = db.conn.execute("PRAGMA foreign_keys").fetchone()
    assert row[0] == 1


def test_migration_tracked(db):
    row = db.conn.execute("SELECT name FROM _migrations WHERE name = '001_initial.sql'").fetchone()
    assert row is not None


def test_migration_not_rerun(db):
    # Run _migrate again; should not raise and count should still be 1
    db._migrate()
    count = db.conn.execute("SELECT COUNT(*) FROM _migrations WHERE name = '001_initial.sql'").fetchone()[0]
    assert count == 1


def test_context_manager():
    with Database(":memory:") as d:
        row = d.conn.execute("SELECT 1").fetchone()
        assert row[0] == 1


def test_conn_property(db):
    import sqlite3

    assert isinstance(db.conn, sqlite3.Connection)
