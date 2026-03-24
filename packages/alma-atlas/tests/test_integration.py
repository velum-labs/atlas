"""Integration tests for Atlas multi-source pipeline.

These tests validate the full connect → scan → search → lineage pipeline
against realistic data. They use the dbt adapter with a real manifest and
optionally test against a live PostgreSQL database.

Run with: uv run pytest packages/alma-atlas/tests/test_integration.py -v

For live Postgres tests, set:
    PG_TEST_DSN=postgresql://user:pass@host/db
    PG_TEST_SCHEMA=public
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from alma_atlas.config import AtlasConfig, SourceConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FINTUAL_MANIFEST = Path(
    "/opt/velum/repos/velum-alma-extract/customers/fintual/dbt-bq-main/target/manifest.json"
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


def _has_fintual_manifest() -> bool:
    return FINTUAL_MANIFEST.exists()


def _has_pg_test() -> bool:
    return "PG_TEST_DSN" in os.environ


# ---------------------------------------------------------------------------
# dbt adapter integration tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_fintual_manifest(), reason="Fintual manifest not available")
class TestDbtIntegration:
    """Integration tests using Fintual's dbt manifest."""

    def _setup_dbt_source(self, config: AtlasConfig) -> None:
        config.add_source(
            SourceConfig(
                id="dbt:fintual",
                kind="dbt",
                params={"manifest_path": str(FINTUAL_MANIFEST)},
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
        assert result.asset_count > 200, f"Expected >200 assets, got {result.asset_count}"
        assert result.edge_count > 300, f"Expected >300 edges, got {result.edge_count}"

    def test_scan_creates_store(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        run_scan(sources[0], atlas_config)

        assert atlas_config.db_path.exists()
        db = sqlite3.connect(str(atlas_config.db_path))

        assets = db.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        edges = db.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        assert assets > 200
        assert edges > 300

        # Verify asset types
        kinds = dict(db.execute("SELECT kind, COUNT(*) FROM assets GROUP BY kind").fetchall())
        assert "table" in kinds
        assert "view" in kinds

        db.close()

    def test_search_after_scan(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        run_scan(sources[0], atlas_config)

        db = sqlite3.connect(str(atlas_config.db_path))
        results = db.execute(
            "SELECT id FROM assets WHERE id LIKE '%users%'"
        ).fetchall()
        assert len(results) > 5, "Expected >5 'users' assets in Fintual manifest"
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
        assert edge_types["depends_on"] > 300

        # Check specific known lineage: heroku_views.users should have downstream
        downstream = db.execute(
            "SELECT COUNT(*) FROM edges WHERE upstream_id LIKE '%heroku_views.users%'"
        ).fetchone()[0]
        assert downstream > 0, f"Expected downstream edges from users table, got {downstream}"
        db.close()

    def test_store_size_reasonable(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan

        self._setup_dbt_source(atlas_config)
        sources = atlas_config.load_sources()
        run_scan(sources[0], atlas_config)

        size_kb = atlas_config.db_path.stat().st_size / 1024
        assert size_kb < 10_000, f"Store too large: {size_kb:.0f} KB"


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


@pytest.mark.skipif(not _has_fintual_manifest(), reason="Fintual manifest not available")
@pytest.mark.skipif(not _has_pg_test(), reason="PG_TEST_DSN not set")
class TestMultiSourceIntegration:
    """Integration tests with multiple sources (dbt + Postgres)."""

    def test_multi_source_scan(self, atlas_config: AtlasConfig) -> None:
        from alma_atlas.pipeline.scan import run_scan, run_scan_all

        atlas_config.add_source(
            SourceConfig(
                id="dbt:fintual",
                kind="dbt",
                params={"manifest_path": str(FINTUAL_MANIFEST)},
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
