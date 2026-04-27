"""Connector smoke tests against real trial accounts (Snowflake + dbt).

Per design doc Open Q #7: Fintual is on BigQuery + Postgres, so the Snowflake
and dbt adapters get exercised first by Track 3b's public Wedge C audience.
These smoke tests catch "connector totally crashes on a real account" before
that audience hits it.

Pass criterion (per the design doc):
    "full scan completes, MCP search_assets returns sane results."

Tests are SKIPPED by default. They run automatically when the required
environment variables are set, e.g.:

    # Snowflake trial:
    export ATLAS_SMOKE_SNOWFLAKE_ACCOUNT=xy12345.us-east-1
    export ATLAS_SMOKE_SNOWFLAKE_CONNECTION_JSON='{"user":"...","password":"..."}'
    export ATLAS_SMOKE_SNOWFLAKE_ROLE=ANALYST
    export ATLAS_SMOKE_SNOWFLAKE_SCHEMA=ANALYTICS
    uv run pytest -m connector_smoke -k snowflake

    # dbt sandbox:
    export ATLAS_SMOKE_DBT_PROJECT_DIR=/path/to/local/dbt/project/with/manifest
    uv run pytest -m connector_smoke -k dbt

CI must NOT run these by default — `-m "not connector_smoke"` is the safe
filter. They live here (not under a separate scripts/ directory) so they can
be run alongside the rest of the test suite when credentials are available.

Owner per design doc Open Q #7 / The Assignment additions: Track 3b lead
runs these in week 4 against Snowflake trial + dbt cloud sandbox.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp.tools_search import _handle_search
from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import EdgeRepository

pytestmark = pytest.mark.connector_smoke


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_SNOWFLAKE_ENV_VARS = (
    "ATLAS_SMOKE_SNOWFLAKE_ACCOUNT",
    "ATLAS_SMOKE_SNOWFLAKE_CONNECTION_JSON",
    "ATLAS_SMOKE_SNOWFLAKE_ROLE",
    "ATLAS_SMOKE_SNOWFLAKE_SCHEMA",
)
_DBT_ENV_VARS = ("ATLAS_SMOKE_DBT_PROJECT_DIR",)


def _missing_snowflake_env() -> list[str]:
    return [name for name in _SNOWFLAKE_ENV_VARS if not os.environ.get(name)]


def _missing_dbt_env() -> list[str]:
    return [name for name in _DBT_ENV_VARS if not os.environ.get(name)]


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    with Database(db_path):
        pass
    return cfg


# ---------------------------------------------------------------------------
# Snowflake smoke
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    bool(_missing_snowflake_env()),
    reason=f"Snowflake smoke env vars missing: {_missing_snowflake_env()}",
)
def test_snowflake_scan_completes_and_yields_assets(tmp_path: Path) -> None:
    """Connect + scan a real Snowflake trial; assert assets land in the graph.

    Pass criterion: zero exceptions, AssetRepository.list_all() > 0.
    """
    from alma_connectors.adapters.snowflake import SnowflakeAdapter

    cfg = _make_cfg(tmp_path)
    account = os.environ["ATLAS_SMOKE_SNOWFLAKE_ACCOUNT"]
    role = os.environ["ATLAS_SMOKE_SNOWFLAKE_ROLE"]
    schema = os.environ["ATLAS_SMOKE_SNOWFLAKE_SCHEMA"]
    connection = json.loads(os.environ["ATLAS_SMOKE_SNOWFLAKE_CONNECTION_JSON"])

    adapter = SnowflakeAdapter(
        source_id=f"snowflake:smoke:{account}",
        account=account,
        role=role,
        schema=schema,
        connection=connection,
    )

    # Run the scan via the canonical pipeline
    from alma_atlas.pipeline.scan import scan_source

    scan_source(cfg, adapter)

    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).list_all()

    assert len(assets) > 0, (
        "Snowflake scan completed but produced zero assets. "
        f"Account={account}, schema={schema}. Expected at least one INFORMATION_SCHEMA hit."
    )
    # Sanity: assets should belong to the smoke source we registered
    smoke_assets = [a for a in assets if a.source.startswith("snowflake:smoke")]
    assert smoke_assets, "No assets attributed to the smoke source — adapter wiring issue."


@pytest.mark.skipif(
    bool(_missing_snowflake_env()),
    reason=f"Snowflake smoke env vars missing: {_missing_snowflake_env()}",
)
def test_snowflake_atlas_search_returns_results(tmp_path: Path) -> None:
    """After a real Snowflake scan, `atlas_search` must return at least one hit.

    Smoke test for the design doc's pass criterion. Searches for a generic
    keyword likely to match column or table names in any analytics schema.
    """
    from alma_atlas.pipeline.scan import scan_source
    from alma_connectors.adapters.snowflake import SnowflakeAdapter

    cfg = _make_cfg(tmp_path)
    account = os.environ["ATLAS_SMOKE_SNOWFLAKE_ACCOUNT"]
    role = os.environ["ATLAS_SMOKE_SNOWFLAKE_ROLE"]
    schema = os.environ["ATLAS_SMOKE_SNOWFLAKE_SCHEMA"]
    connection = json.loads(os.environ["ATLAS_SMOKE_SNOWFLAKE_CONNECTION_JSON"])

    scan_source(
        cfg,
        SnowflakeAdapter(
            source_id=f"snowflake:smoke:{account}",
            account=account,
            role=role,
            schema=schema,
            connection=connection,
        ),
    )

    # Pull the first asset's name fragment to use as a search term
    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).list_all()
    assert assets, "No assets to search against; precondition failed."
    needle = assets[0].name.split(".")[-1][:3].lower() or "a"

    result = _handle_search(cfg, {"query": needle})
    assert result and "No assets found" not in result[0].text, (
        f"atlas_search({needle!r}) returned no hits despite {len(assets)} assets in graph."
    )


# ---------------------------------------------------------------------------
# dbt smoke
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    bool(_missing_dbt_env()),
    reason=f"dbt smoke env vars missing: {_missing_dbt_env()}",
)
def test_dbt_scan_completes_and_yields_assets(tmp_path: Path) -> None:
    """Connect + scan a real dbt project; assert assets land in the graph.

    Pass criterion: zero exceptions, AssetRepository.list_all() > 0, model
    assets are discovered.
    """
    from alma_atlas.pipeline.scan import scan_source
    from alma_connectors.adapters.dbt import DbtAdapter

    cfg = _make_cfg(tmp_path)
    project_dir = Path(os.environ["ATLAS_SMOKE_DBT_PROJECT_DIR"])
    assert project_dir.is_dir(), f"ATLAS_SMOKE_DBT_PROJECT_DIR is not a directory: {project_dir}"

    adapter = DbtAdapter(source_id="dbt:smoke", project_dir=project_dir)
    scan_source(cfg, adapter)

    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).list_all()

    assert len(assets) > 0, "dbt scan completed but produced zero assets."
    model_assets = [a for a in assets if a.kind == "model"]
    assert model_assets, "No dbt model assets discovered. manifest.json may be missing or empty."


@pytest.mark.skipif(
    bool(_missing_dbt_env()),
    reason=f"dbt smoke env vars missing: {_missing_dbt_env()}",
)
def test_dbt_scan_produces_lineage_edges(tmp_path: Path) -> None:
    """A real dbt scan must produce at least one edge (model -> upstream)."""
    from alma_atlas.pipeline.scan import scan_source
    from alma_connectors.adapters.dbt import DbtAdapter

    cfg = _make_cfg(tmp_path)
    project_dir = Path(os.environ["ATLAS_SMOKE_DBT_PROJECT_DIR"])
    scan_source(cfg, DbtAdapter(source_id="dbt:smoke", project_dir=project_dir))

    with Database(cfg.db_path) as db:
        edges = list(EdgeRepository(db).list_all()) if hasattr(EdgeRepository(db), "list_all") else []

    assert edges, (
        "dbt scan completed but produced zero edges. "
        "Either the manifest has no `depends_on` references, or the adapter is dropping them."
    )


# ---------------------------------------------------------------------------
# Cross-source smoke (only runs if BOTH Snowflake + dbt are configured)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    bool(_missing_snowflake_env()) or bool(_missing_dbt_env()),
    reason="Cross-source smoke requires BOTH Snowflake + dbt env vars set.",
)
def test_cross_source_smoke_assets_from_both_sources_coexist(tmp_path: Path) -> None:
    """Scan Snowflake AND dbt into the same graph; confirm both sources land.

    The Companion's value proposition is cross-stack lineage. If the two
    adapters can't coexist in one graph, that whole story breaks.
    """
    from alma_atlas.pipeline.scan import scan_source
    from alma_connectors.adapters.dbt import DbtAdapter
    from alma_connectors.adapters.snowflake import SnowflakeAdapter

    cfg = _make_cfg(tmp_path)
    account = os.environ["ATLAS_SMOKE_SNOWFLAKE_ACCOUNT"]
    role = os.environ["ATLAS_SMOKE_SNOWFLAKE_ROLE"]
    schema = os.environ["ATLAS_SMOKE_SNOWFLAKE_SCHEMA"]
    connection = json.loads(os.environ["ATLAS_SMOKE_SNOWFLAKE_CONNECTION_JSON"])
    project_dir = Path(os.environ["ATLAS_SMOKE_DBT_PROJECT_DIR"])

    scan_source(
        cfg,
        SnowflakeAdapter(
            source_id=f"snowflake:smoke:{account}",
            account=account,
            role=role,
            schema=schema,
            connection=connection,
        ),
    )
    scan_source(cfg, DbtAdapter(source_id="dbt:smoke", project_dir=project_dir))

    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).list_all()

    sources = {a.source for a in assets}
    snowflake_present = any(s.startswith("snowflake:smoke") for s in sources)
    dbt_present = any(s.startswith("dbt:smoke") for s in sources)
    assert snowflake_present and dbt_present, (
        f"Expected assets from both Snowflake and dbt sources. Got sources: {sources}"
    )
