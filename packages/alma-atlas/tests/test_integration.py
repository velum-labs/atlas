"""Integration tests for Atlas multi-source pipeline.

These tests validate the full connect → scan → search → lineage pipeline
against realistic data. They use the dbt adapter with a checked-in fixture
manifest (override via ``ALMA_TEST_DBT_MANIFEST``) and optionally test
against a live PostgreSQL database.

Run with: uv run pytest packages/alma-atlas/tests/test_integration.py -v

For live Postgres tests, set:
    PG_TEST_DSN=postgresql://user:pass@host/db
    PG_TEST_SCHEMA=public
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from alma_atlas.config import AtlasConfig, SourceConfig

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

INTEGRATION_DBT_MANIFEST = Path(
    os.environ.get("ALMA_TEST_DBT_MANIFEST")
    or (
        Path(__file__).resolve().parents[3]
        / "testdata"
        / "dbt"
        / "integration_manifest.json"
    )
)


@pytest.fixture
def atlas_dir(tmp_path: Path) -> Path:
    """Create a temporary Atlas config directory."""
    config_dir = tmp_path / "atlas"
    config_dir.mkdir()
    return config_dir


@pytest.fixture
def atlas_config(atlas_dir: Path) -> AtlasConfig:
    """Create an AtlasConfig pointing at the temp directory."""
    return AtlasConfig(config_dir=atlas_dir)


def _has_integration_manifest() -> bool:
    return INTEGRATION_DBT_MANIFEST.exists()


def _has_pg_test() -> bool:
    return "PG_TEST_DSN" in os.environ


# ---------------------------------------------------------------------------
# dbt adapter integration tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_integration_manifest(), reason="Integration dbt manifest not available")
class TestDbtIntegration:
    """Integration tests using the shared dbt integration manifest."""

    def _setup_dbt_source(self, config: AtlasConfig) -> None:
        config.add_source(
            SourceConfig(
                id="dbt:fintual",
                kind="dbt",
                params={"manifest_path": str(INTEGRATION_DBT_MANIFEST)},
            )
        )

    def test_connect_dbt(self, atlas_config: AtlasConfig) -> None:
        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        assert len(sources) == 1
        assert sources[0].id == "dbt:fintual"
        assert sources[0].kind == "dbt"

    def test_scan_dbt(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        result = run_scan(sources[0], atlas_config)
        assert result.asset_count >= 6, f"Expected at least 6 assets, got {result.asset_count}"
        assert result.edge_count >= 5, f"Expected at least 5 edges, got {result.edge_count}"

    def test_scan_creates_store(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        run_scan(sources[0], atlas_config)

        assert atlas_config.db_path.exists()
        db = sqlite3.connect(str(atlas_config.db_path))

        assets = db.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        edges = db.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        assert assets >= 6
        assert edges >= 5

        # Verify asset types
        kinds = dict(db.execute("SELECT kind, COUNT(*) FROM assets GROUP BY kind").fetchall())
        assert "table" in kinds
        assert "view" in kinds or "external_table" in kinds

        db.close()

    def test_search_after_scan(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        run_scan(sources[0], atlas_config)

        db = sqlite3.connect(str(atlas_config.db_path))
        results = db.execute(
            "SELECT id FROM assets WHERE id LIKE '%customer%'"
        ).fetchall()
        assert len(results) > 0, "Expected at least one customer-related asset in integration manifest"
        db.close()

    def test_lineage_edges(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        run_scan(sources[0], atlas_config)

        db = sqlite3.connect(str(atlas_config.db_path))
        # Check that edges are depends_on type
        edge_types = dict(
            db.execute("SELECT kind, COUNT(*) FROM edges GROUP BY kind").fetchall()
        )
        assert "depends_on" in edge_types
        assert edge_types["depends_on"] >= 4

        # Check a known lineage path from the raw source to staged/marts models.
        downstream = db.execute(
            "SELECT COUNT(*) FROM edges WHERE upstream_id LIKE '%raw.orders%'"
        ).fetchone()[0]
        assert downstream > 0, f"Expected downstream edges from raw.orders, got {downstream}"
        db.close()

    def test_store_size_reasonable(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        run_scan(sources[0], atlas_config)

        size_kb = atlas_config.db_path.stat().st_size / 1024
        assert size_kb < 512, f"Store too large for the fixture manifest: {size_kb:.0f} KB"


# ---------------------------------------------------------------------------
# SQLite adapter integration tests
# ---------------------------------------------------------------------------


class TestSQLiteIntegration:
    """Integration tests against a temporary SQLite database file."""

    def _setup_sqlite_source(self, config: AtlasConfig) -> Path:
        db_path = config.config_dir / "sample.sqlite"
        connection = sqlite3.connect(str(db_path))
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                nickname
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                amount REAL
            )
            """
        )
        connection.executemany(
            "INSERT INTO users (name, nickname) VALUES (?, ?)",
            [("Alice", "ally"), ("Bob", None)],
        )
        connection.executemany(
            "INSERT INTO orders (id, user_id, amount) VALUES (?, ?, ?)",
            [(1, 1, 99.9), (2, 2, 49.5)],
        )
        connection.execute("CREATE VIEW user_names AS SELECT id, name FROM users")
        connection.commit()
        connection.close()

        config.add_source(
            SourceConfig(
                id="sqlite:sample",
                kind="sqlite",
                params={"path": str(db_path)},
            )
        )
        return db_path

    def test_connect_sqlite(self, atlas_config: AtlasConfig) -> None:
        self._setup_sqlite_source(atlas_config)
        sources = atlas_config.load_sources()
        assert len(sources) == 1
        assert sources[0].id == "sqlite:sample"
        assert sources[0].kind == "sqlite"

    def test_scan_sqlite(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.learn import get_unannotated_assets
        from alma_atlas.pipeline.scan import run_scan
        from alma_atlas_store.db import Database

        self._setup_sqlite_source(atlas_config)
        sources = atlas_config.load_sources()
        result = run_scan(sources[0], atlas_config)

        assert result.error is None
        assert result.asset_count == 3
        assert result.edge_count == 1

        with Database(atlas_config.db_path) as db:
            assets = db.conn.execute(
                "SELECT name, kind FROM assets ORDER BY name"
            ).fetchall()
            edges = db.conn.execute(
                "SELECT upstream_id, downstream_id, kind FROM edges ORDER BY upstream_id, downstream_id"
            ).fetchall()
            unannotated_assets = get_unannotated_assets(db)

        assert [(row[0], row[1]) for row in assets] == [
            ("_default.orders", "table"),
            ("_default.user_names", "view"),
            ("_default.users", "table"),
        ]
        assert len(edges) == 1
        assert edges[0][2] == "depends_on"
        assert len(unannotated_assets) == 3


# ---------------------------------------------------------------------------
# PostgreSQL adapter integration tests (optional, live DB)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_pg_test(), reason="PG_TEST_DSN not set")
class TestPostgresIntegration:
    """Integration tests against a live PostgreSQL database."""

    def _setup_pg_source(self, config: AtlasConfig) -> None:
        dsn = os.environ["PG_TEST_DSN"]
        schema = os.environ.get("PG_TEST_SCHEMA", "public")
        config.add_source(
            SourceConfig(
                id=f"postgres:test:{schema}",
                kind="postgres",
                params={"dsn": dsn, "schema": schema},
            )
        )

    def test_connect_postgres(self, atlas_config: AtlasConfig) -> None:
        self._setup_pg_source(atlas_config)
        sources = atlas_config.load_sources()
        assert len(sources) == 1
        assert sources[0].kind == "postgres"

    def test_scan_postgres(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_pg_source(atlas_config)
        sources = atlas_config.load_sources()
        result = run_scan(sources[0], atlas_config)
        assert result.asset_count > 0, "Expected at least 1 asset from Postgres"


# ---------------------------------------------------------------------------
# Multi-source integration tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_integration_manifest(), reason="Integration dbt manifest not available")
@pytest.mark.skipif(not _has_pg_test(), reason="PG_TEST_DSN not set")
class TestMultiSourceIntegration:
    """Integration tests with multiple sources (dbt + Postgres)."""

    def test_multi_source_scan(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan_all

        atlas_config.add_source(
            SourceConfig(
                id="dbt:fintual",
                kind="dbt",
                params={"manifest_path": str(INTEGRATION_DBT_MANIFEST)},
            )
        )
        dsn = os.environ["PG_TEST_DSN"]
        schema = os.environ.get("PG_TEST_SCHEMA", "public")
        atlas_config.add_source(
            SourceConfig(
                id=f"postgres:test:{schema}",
                kind="postgres",
                params={"dsn": dsn, "schema": schema},
            )
        )

        sources = atlas_config.load_sources()
        result = run_scan_all(sources, atlas_config)
        assert len(result.results) == 2

        db = sqlite3.connect(str(atlas_config.db_path))
        sources_in_db = db.execute("SELECT DISTINCT source FROM assets").fetchall()
        assert len(sources_in_db) == 2, f"Expected 2 sources in DB, got {len(sources_in_db)}"
        db.close()
