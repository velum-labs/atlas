"""Tests for alma_atlas.mcp.tools — handler functions and register()."""

from __future__ import annotations

from pathlib import Path

import pytest

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp.tools import (
    _handle_get_asset,
    _handle_get_schema,
    _handle_impact,
    _handle_lineage,
    _handle_search,
    _handle_status,
)
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    # create the db
    with Database(db_path):
        pass
    return cfg


def _seed_assets(db_path: Path) -> None:
    with Database(db_path) as db:
        repo = AssetRepository(db)
        repo.upsert(Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders"))
        repo.upsert(Asset(id="pg::public.customers", source="pg:test", kind="table", name="public.customers"))
        repo.upsert(Asset(id="pg::public.order_items", source="pg:test", kind="view", name="public.order_items"))


def _seed_edges(db_path: Path) -> None:
    with Database(db_path) as db:
        repo = EdgeRepository(db)
        repo.upsert(Edge(upstream_id="pg::public.orders", downstream_id="pg::public.order_items", kind="reads"))
        repo.upsert(Edge(upstream_id="pg::public.customers", downstream_id="pg::public.order_items", kind="reads"))


# ---------------------------------------------------------------------------
# _handle_search
# ---------------------------------------------------------------------------


def test_search_returns_matching_assets(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_search(cfg, {"query": "orders"})
    assert len(result) == 1
    assert "orders" in result[0].text


def test_search_no_results(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_search(cfg, {"query": "nonexistent_xyz"})
    assert len(result) == 1
    assert "No assets found" in result[0].text


def test_search_respects_limit(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_search(cfg, {"query": "public", "limit": 1})
    assert len(result) == 1
    # Should mention 1 asset returned
    assert "1" in result[0].text


# ---------------------------------------------------------------------------
# _handle_get_asset
# ---------------------------------------------------------------------------


def test_get_asset_returns_json(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_get_asset(cfg, {"asset_id": "pg::public.orders"})
    import json
    data = json.loads(result[0].text)
    assert data["id"] == "pg::public.orders"
    assert data["kind"] == "table"


def test_get_asset_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_get_asset(cfg, {"asset_id": "nonexistent"})
    assert "not found" in result[0].text.lower()


# ---------------------------------------------------------------------------
# _handle_lineage
# ---------------------------------------------------------------------------


def test_lineage_downstream(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_edges(cfg.db_path)
    result = _handle_lineage(cfg, {"asset_id": "pg::public.orders", "direction": "downstream"})
    assert "order_items" in result[0].text


def test_lineage_upstream(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_edges(cfg.db_path)
    result = _handle_lineage(cfg, {"asset_id": "pg::public.order_items", "direction": "upstream"})
    assert "orders" in result[0].text or "customers" in result[0].text


def test_lineage_asset_not_in_graph(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    # no edges seeded
    result = _handle_lineage(cfg, {"asset_id": "pg::public.orders", "direction": "downstream"})
    assert "not found" in result[0].text.lower() or "no downstream" in result[0].text.lower()


def test_lineage_with_depth(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_edges(cfg.db_path)
    result = _handle_lineage(cfg, {"asset_id": "pg::public.orders", "direction": "downstream", "depth": 1})
    assert len(result) == 1


# ---------------------------------------------------------------------------
# _handle_status
# ---------------------------------------------------------------------------


def test_status_empty_db(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_status(cfg)
    assert "0 assets" in result[0].text


def test_status_with_data(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_edges(cfg.db_path)
    result = _handle_status(cfg)
    text = result[0].text
    assert "assets" in text
    assert "edges" in text


def test_status_shows_kind_breakdown(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_status(cfg)
    assert "table" in result[0].text or "view" in result[0].text


def test_status_shows_source_breakdown(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_status(cfg)
    assert "pg:test" in result[0].text


# ---------------------------------------------------------------------------
# _handle_get_schema
# ---------------------------------------------------------------------------


def test_get_schema_asset_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_get_schema(cfg, {"asset_id": "missing"})
    assert "not found" in result[0].text.lower()


def test_get_schema_no_snapshot_no_metadata(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    result = _handle_get_schema(cfg, {"asset_id": "pg::public.orders"})
    assert "no schema" in result[0].text.lower()


def test_get_schema_with_snapshot(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    with Database(cfg.db_path) as db:
        SchemaRepository(db).upsert(
            SchemaSnapshot(
                asset_id="pg::public.orders",
                columns=[ColumnInfo(name="id", type="INTEGER", nullable=False)],
            )
        )
    result = _handle_get_schema(cfg, {"asset_id": "pg::public.orders"})
    assert "id" in result[0].text


def test_get_schema_falls_back_to_metadata(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(
                id="pg::public.meta_table",
                source="pg:test",
                kind="table",
                name="public.meta_table",
                metadata={"columns": [{"name": "col1", "type": "TEXT", "nullable": True}]},
            )
        )
    result = _handle_get_schema(cfg, {"asset_id": "pg::public.meta_table"})
    assert "col1" in result[0].text


# ---------------------------------------------------------------------------
# _handle_impact
# ---------------------------------------------------------------------------


def test_impact_asset_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_impact(cfg, {"asset_id": "missing"})
    assert "not found" in result[0].text.lower()


def test_impact_no_downstream(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    # seed edges but check a leaf node
    _seed_edges(cfg.db_path)
    result = _handle_impact(cfg, {"asset_id": "pg::public.order_items"})
    # order_items has no downstream
    assert "no downstream" in result[0].text.lower() or "not found" in result[0].text.lower()


def test_impact_with_downstream(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_edges(cfg.db_path)
    result = _handle_impact(cfg, {"asset_id": "pg::public.orders"})
    assert "order_items" in result[0].text


def test_impact_recommendation_text(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_edges(cfg.db_path)
    result = _handle_impact(cfg, {"asset_id": "pg::public.orders"})
    assert "Recommendation" in result[0].text


# ---------------------------------------------------------------------------
# register() — smoke test
# ---------------------------------------------------------------------------


def test_register_does_not_raise(tmp_path: Path) -> None:
    from mcp.server import Server

    from alma_atlas.mcp.tools import register

    server = Server("test")
    cfg = _make_cfg(tmp_path)
    register(server, cfg)  # should not raise
