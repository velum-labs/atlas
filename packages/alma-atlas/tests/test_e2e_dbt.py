"""End-to-end integration test for the dbt scan pipeline.

Flow: create fake dbt project → connect → scan → verify store → verify MCP tools.
No network access required — DbtAdapter reads local JSON files only.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from alma_atlas.config import AtlasConfig, SourceConfig
from alma_atlas.pipeline.scan import _build_adapter, run_scan
from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import EdgeRepository


# ---------------------------------------------------------------------------
# Minimal dbt manifest fixture
# ---------------------------------------------------------------------------

MANIFEST_V12 = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json",
        "project_name": "my_project",
    },
    "nodes": {
        "model.my_project.stg_orders": {
            "unique_id": "model.my_project.stg_orders",
            "resource_type": "model",
            "schema": "analytics",
            "name": "stg_orders",
            "description": "Staged orders",
            "config": {"materialized": "view"},
            "columns": {
                "id": {"name": "id", "description": "Primary key", "data_type": "integer"},
                "amount": {"name": "amount", "description": "Order total", "data_type": "numeric"},
            },
            "depends_on": {"nodes": ["source.my_project.raw.orders"]},
        }
    },
    "sources": {
        "source.my_project.raw.orders": {
            "unique_id": "source.my_project.raw.orders",
            "resource_type": "source",
            "schema": "raw",
            "name": "orders",
            "source_name": "raw",
            "columns": {
                "id": {"name": "id", "data_type": "integer"},
                "amount": {"name": "amount", "data_type": "numeric"},
                "created_at": {"name": "created_at", "data_type": "timestamp"},
            },
        }
    },
}


@pytest.fixture
def dbt_project(tmp_path: Path) -> Path:
    """Create a minimal dbt project directory with manifest.json."""
    target_dir = tmp_path / "my_dbt_project" / "target"
    target_dir.mkdir(parents=True)
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST_V12), encoding="utf-8")
    return tmp_path / "my_dbt_project"


@pytest.fixture
def dbt_source(dbt_project: Path) -> SourceConfig:
    """Return a SourceConfig pointing at the fake dbt project manifest."""
    manifest_path = str(dbt_project / "target" / "manifest.json")
    return SourceConfig(
        id="dbt:project",
        kind="dbt",
        params={"manifest_path": manifest_path},
    )


@pytest.fixture
def scan_cfg(tmp_path: Path) -> AtlasConfig:
    """AtlasConfig with a writable on-disk database."""
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")


# ---------------------------------------------------------------------------
# _build_adapter — dbt
# ---------------------------------------------------------------------------


def test_build_adapter_dbt_returns_dbt_adapter(dbt_source: SourceConfig) -> None:
    from alma_connectors.adapters.dbt import DbtAdapter

    adapter, persisted = _build_adapter(dbt_source)
    assert isinstance(adapter, DbtAdapter)
    assert persisted.key == "dbt-project"  # colon sanitized to hyphen for key validation
    assert persisted.kind.value == "dbt"


# ---------------------------------------------------------------------------
# Full scan pipeline
# ---------------------------------------------------------------------------


def test_run_scan_dbt_assets(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    result = run_scan(dbt_source, scan_cfg)

    assert result.error is None, f"Unexpected scan error: {result.error}"
    # 1 model + 1 source = 2 assets
    assert result.asset_count == 2


def test_run_scan_dbt_assets_in_store(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    run_scan(dbt_source, scan_cfg)

    with Database(scan_cfg.db_path) as db:
        assets = AssetRepository(db).list_all()

    asset_names = {a.name for a in assets}
    assert "analytics.stg_orders" in asset_names
    assert "raw.orders" in asset_names


def test_run_scan_dbt_asset_kinds(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    run_scan(dbt_source, scan_cfg)

    with Database(scan_cfg.db_path) as db:
        assets = AssetRepository(db).list_all()

    by_name = {a.name: a for a in assets}
    # stg_orders is materialized as a view
    assert by_name["analytics.stg_orders"].kind == "view"
    # raw source defaults to table
    assert by_name["raw.orders"].kind == "table"


def test_run_scan_dbt_lineage_edges(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    result = run_scan(dbt_source, scan_cfg)

    # 1 dependency edge from manifest: raw.orders → analytics.stg_orders
    assert result.edge_count == 1

    with Database(scan_cfg.db_path) as db:
        edges = EdgeRepository(db).list_all()

    assert len(edges) == 1
    edge = edges[0]
    assert edge.upstream_id == "dbt:project::raw.orders"
    assert edge.downstream_id == "dbt:project::analytics.stg_orders"
    assert edge.kind == "depends_on"


# ---------------------------------------------------------------------------
# MCP tool responses
# ---------------------------------------------------------------------------


def test_mcp_atlas_status(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    run_scan(dbt_source, scan_cfg)

    from alma_atlas.mcp.tools import _handle_status

    result = _handle_status(scan_cfg)
    assert len(result) == 1
    text = result[0].text
    assert "2 assets" in text
    assert "1 edges" in text


def test_mcp_atlas_search_finds_orders(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    run_scan(dbt_source, scan_cfg)

    from alma_atlas.mcp.tools import _handle_search

    result = _handle_search(scan_cfg, {"query": "orders"})
    assert len(result) == 1
    text = result[0].text
    assert "orders" in text.lower()


def test_mcp_atlas_get_asset(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    run_scan(dbt_source, scan_cfg)

    from alma_atlas.mcp.tools import _handle_get_asset

    asset_id = "dbt:project::analytics.stg_orders"
    result = _handle_get_asset(scan_cfg, {"asset_id": asset_id})
    assert len(result) == 1
    data = json.loads(result[0].text)
    assert data["id"] == asset_id
    assert data["kind"] == "view"
    assert data["source"] == "dbt:project"


def test_mcp_atlas_lineage_upstream(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    run_scan(dbt_source, scan_cfg)

    from alma_atlas.mcp.tools import _handle_lineage

    result = _handle_lineage(
        scan_cfg,
        {"asset_id": "dbt:project::analytics.stg_orders", "direction": "upstream"},
    )
    assert len(result) == 1
    text = result[0].text
    assert "dbt:project::raw.orders" in text


def test_mcp_atlas_lineage_downstream(dbt_source: SourceConfig, scan_cfg: AtlasConfig) -> None:
    run_scan(dbt_source, scan_cfg)

    from alma_atlas.mcp.tools import _handle_lineage

    result = _handle_lineage(
        scan_cfg,
        {"asset_id": "dbt:project::raw.orders", "direction": "downstream"},
    )
    assert len(result) == 1
    text = result[0].text
    assert "dbt:project::analytics.stg_orders" in text


# ---------------------------------------------------------------------------
# connect --project-dir
# ---------------------------------------------------------------------------


def test_connect_dbt_project_dir(dbt_project: Path, scan_cfg: AtlasConfig, tmp_path: Path) -> None:
    """connect_dbt with --project-dir resolves target/manifest.json automatically."""
    import alma_atlas.config as config_module

    # Patch global config to use our temp dir
    original = config_module._config
    config_module._config = scan_cfg
    try:
        from alma_atlas.cli.connect import connect_dbt

        connect_dbt(manifest=None, project_dir=str(dbt_project), project=None)
    finally:
        config_module._config = original

    sources = scan_cfg.load_sources()
    assert len(sources) == 1
    assert sources[0].kind == "dbt"
    assert "manifest.json" in sources[0].params["manifest_path"]
    assert "target" in sources[0].params["manifest_path"]
