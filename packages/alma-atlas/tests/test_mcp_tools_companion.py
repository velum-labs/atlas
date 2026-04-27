"""Tests for the Atlas Companion MCP tools (companion_search_assets,
companion_get_schema_and_owner, companion_explain_lineage_and_contract).

Each handler composes existing repos (AssetRepository, SchemaRepository,
AnnotationRepository, EdgeRepository) and renders a CompanionBundle as
prompt-ready text. Tests cover happy path + the edge cases flagged in the
eng review test diagram.
"""

from __future__ import annotations

from pathlib import Path

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp.tools_companion import (
    _handle_companion_explain_lineage_and_contract,
    _handle_companion_get_schema_and_owner,
    _handle_companion_search,
)
from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot
from alma_ports.annotation import AnnotationRecord

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    with Database(db_path):
        pass
    return cfg


def _seed_assets(db_path: Path) -> None:
    with Database(db_path) as db:
        repo = AssetRepository(db)
        repo.upsert(
            Asset(
                id="bq::analytics.orders",
                source="bigquery:fintual",
                kind="table",
                name="analytics.orders",
                description="Order facts table.",
            )
        )
        repo.upsert(
            Asset(
                id="bq::analytics.users",
                source="bigquery:fintual",
                kind="table",
                name="analytics.users",
            )
        )
        repo.upsert(
            Asset(
                id="bq::marts.fct_revenue",
                source="bigquery:fintual",
                kind="view",
                name="marts.fct_revenue",
            )
        )


def _seed_schema(db_path: Path) -> None:
    with Database(db_path) as db:
        SchemaRepository(db).upsert(
            SchemaSnapshot(
                asset_id="bq::analytics.orders",
                captured_at="2026-04-26T12:00:00Z",
                columns=[
                    ColumnInfo(name="order_id", type="STRING", nullable=False, description="primary key"),
                    ColumnInfo(name="user_id", type="STRING", nullable=False),
                    ColumnInfo(name="amount_clp", type="NUMERIC", nullable=True),
                ],
            )
        )


def _seed_annotation(db_path: Path) -> None:
    with Database(db_path) as db:
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="bq::analytics.orders",
                ownership="data-eng@fintual.com",
                granularity=None,
                join_keys=[],
                freshness_guarantee=None,
                business_logic_summary=None,
                sensitivity=None,
                annotated_at="2026-04-26T12:00:00Z",
                annotated_by="manual",
                properties={},
            )
        )


def _seed_edges(db_path: Path) -> None:
    with Database(db_path) as db:
        repo = EdgeRepository(db)
        repo.upsert(
            Edge(
                upstream_id="bq::analytics.orders",
                downstream_id="bq::marts.fct_revenue",
                kind="reads",
            )
        )
        repo.upsert(
            Edge(
                upstream_id="bq::analytics.users",
                downstream_id="bq::marts.fct_revenue",
                kind="reads",
            )
        )


# ---------------------------------------------------------------------------
# companion_search_assets
# ---------------------------------------------------------------------------


def test_companion_search_returns_bundle_with_related_assets(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_annotation(cfg.db_path)

    result = _handle_companion_search(cfg, {"query": "orders"})

    assert len(result) == 1
    text = result[0].text
    assert "Found 1 asset(s) matching 'orders'." in text
    assert "bq::analytics.orders" in text
    assert "data-eng@fintual.com" in text  # owner from annotation


def test_companion_search_no_matches_returns_no_results_message(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)

    result = _handle_companion_search(cfg, {"query": "nonexistent_xyz"})

    text = result[0].text
    assert "No assets found matching 'nonexistent_xyz'." in text


def test_companion_search_respects_limit(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)

    result = _handle_companion_search(cfg, {"query": "analytics", "limit": 1})

    text = result[0].text
    # Limit 1 should yield at most 1 asset in the bundle (header reflects count)
    assert "Found 1 asset(s)" in text or "Found 0 asset(s)" in text


# ---------------------------------------------------------------------------
# companion_get_schema_and_owner
# ---------------------------------------------------------------------------


def test_companion_get_schema_and_owner_returns_full_bundle(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_schema(cfg.db_path)
    _seed_annotation(cfg.db_path)

    result = _handle_companion_get_schema_and_owner(cfg, {"asset_id": "bq::analytics.orders"})

    text = result[0].text
    assert "bq::analytics.orders [table -> bigquery:fintual]" in text
    assert "Owner: data-eng@fintual.com" in text
    # SchemaRepository auto-sets captured_at at insert; we don't assert the exact value.
    assert "Updated:" in text
    assert "Description: Order facts table." in text
    assert "Schema (3 columns):" in text
    assert "order_id (STRING, NOT NULL) -- primary key" in text
    assert "amount_clp (NUMERIC)" in text


def test_companion_get_schema_and_owner_partial_data_no_annotation(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_schema(cfg.db_path)
    # Deliberately skip annotation seed

    result = _handle_companion_get_schema_and_owner(cfg, {"asset_id": "bq::analytics.orders"})

    text = result[0].text
    assert "bq::analytics.orders" in text
    assert "Owner:" not in text  # no annotation -> no owner line
    assert "Schema (3 columns):" in text  # schema still rendered


def test_companion_get_schema_and_owner_no_snapshot(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_annotation(cfg.db_path)
    # Deliberately skip schema seed

    result = _handle_companion_get_schema_and_owner(cfg, {"asset_id": "bq::analytics.orders"})

    text = result[0].text
    assert "bq::analytics.orders" in text
    assert "Owner: data-eng@fintual.com" in text
    # No snapshot AND no metadata.columns means no Schema section
    assert "Schema (" not in text


def test_companion_get_schema_and_owner_asset_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)

    result = _handle_companion_get_schema_and_owner(cfg, {"asset_id": "bq::missing.table"})

    text = result[0].text
    assert "Asset not found: bq::missing.table" in text


# ---------------------------------------------------------------------------
# companion_explain_lineage_and_contract
# ---------------------------------------------------------------------------


def test_companion_explain_lineage_includes_downstream_edges(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    _seed_edges(cfg.db_path)

    result = _handle_companion_explain_lineage_and_contract(
        cfg, {"asset_id": "bq::analytics.orders"}
    )

    text = result[0].text
    assert "bq::analytics.orders" in text
    # Downstream edge should appear: orders -> fct_revenue
    assert "bq::analytics.orders -> bq::marts.fct_revenue" in text
    # Lineage summary count is reflected
    assert "Lineage:" in text


def test_companion_explain_lineage_no_edges(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)
    # No edges seeded

    result = _handle_companion_explain_lineage_and_contract(
        cfg, {"asset_id": "bq::analytics.orders"}
    )

    text = result[0].text
    assert "Lineage: no related assets found." in text


def test_companion_explain_lineage_asset_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)

    result = _handle_companion_explain_lineage_and_contract(
        cfg, {"asset_id": "bq::missing.table"}
    )

    text = result[0].text
    assert "Asset not found: bq::missing.table" in text


def test_companion_explain_lineage_no_contracts_summary(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_assets(cfg.db_path)

    result = _handle_companion_explain_lineage_and_contract(
        cfg, {"asset_id": "bq::analytics.orders"}
    )

    text = result[0].text
    # No contracts defined for this asset; summary should reflect that
    assert "No contracts defined" in text or "Contract check" in text
