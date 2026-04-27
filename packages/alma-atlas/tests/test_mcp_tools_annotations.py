"""Tests for atlas_get_annotations and Phase 2 MCP handlers."""

from __future__ import annotations

import json
from pathlib import Path

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp.tools_lineage import _handle_describe_relationship
from alma_atlas.mcp.tools_schema import (
    _handle_explain_column,
    _handle_get_annotations,
    _handle_profile_column,
)
from alma_atlas.mcp.tools_search import _handle_find_term, _handle_search
from alma_atlas_store.annotation_repository import AnnotationRecord, AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.profiling_repository import ProfilingRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot
from alma_ports.profiling import ColumnProfile


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    with Database(db_path):
        pass
    return cfg


def test_get_annotations_empty(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_get_annotations(cfg, {"limit": 10})
    payload = json.loads(result[0].text)
    assert payload["annotations"] == []


def test_get_annotations_for_asset(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        # Need an asset present for FK join in list_unannotated (not used here, but good hygiene)
        AssetRepository(db).upsert(Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders"))
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="pg::public.orders",
                ownership="data",
                granularity="one row per order",
                join_keys=["order_id"],
                freshness_guarantee="hourly",
                business_logic_summary="orders table",
                sensitivity="financial",
                annotated_by="agent:mock",
            )
        )

    result = _handle_get_annotations(cfg, {"asset_id": "pg::public.orders"})
    payload = json.loads(result[0].text)
    assert payload["asset_id"] == "pg::public.orders"
    assert payload["ownership"] == "data"
    assert payload["join_keys"] == ["order_id"]


def test_get_annotations_missing_asset(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_get_annotations(cfg, {"asset_id": "missing"})
    assert "no annotation" in result[0].text.lower()


# ---------------------------------------------------------------------------
# atlas_explain_column
# ---------------------------------------------------------------------------


def _seed_explain_data(db_path: Path) -> None:
    with Database(db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders")
        )
        SchemaRepository(db).upsert(
            SchemaSnapshot(
                asset_id="pg::public.orders",
                columns=[
                    ColumnInfo(name="order_id", type="integer", nullable=False),
                    ColumnInfo(name="total_cents", type="integer", nullable=True),
                ],
            )
        )
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="pg::public.orders",
                ownership="data",
                granularity="one row per order",
                join_keys=["order_id"],
                freshness_guarantee="hourly",
                business_logic_summary="orders table",
                sensitivity="financial",
                annotated_by="agent:mock",
                properties={"column_notes": {"total_cents": "Order total in cents before tax"}},
            )
        )
        ProfilingRepository(db).upsert(
            ColumnProfile(
                asset_id="pg::public.orders",
                column_name="total_cents",
                distinct_count=42,
                null_count=3,
                null_fraction=0.05,
                min_value="100",
                max_value="99900",
                top_values=[{"value": "1000", "count": 10}, {"value": "2000", "count": 8}],
                sample_values=["500", "1000", "2500"],
                profiled_at="2026-04-01T00:00:00",
            )
        )


def test_explain_column_assembles_all_sources(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_explain_data(cfg.db_path)

    result = _handle_explain_column(cfg, {"asset_id": "pg::public.orders", "column": "total_cents"})
    text = result[0].text

    assert "total_cents" in text
    assert "integer" in text.lower()
    assert "Order total in cents before tax" in text
    assert "distinct_count: 42" in text
    assert "min: 100" in text
    assert "max: 99900" in text
    assert "1000 (10)" in text


def test_explain_column_schema_only(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_explain_data(cfg.db_path)

    result = _handle_explain_column(cfg, {"asset_id": "pg::public.orders", "column": "order_id"})
    text = result[0].text

    assert "order_id" in text
    assert "integer" in text.lower()
    assert "NOT NULL" in text


def test_explain_column_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_explain_column(cfg, {"asset_id": "missing", "column": "id"})
    assert "no information" in result[0].text.lower()


# ---------------------------------------------------------------------------
# atlas_profile_column
# ---------------------------------------------------------------------------


def test_profile_column_returns_stats(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders")
        )
        ProfilingRepository(db).upsert(
            ColumnProfile(
                asset_id="pg::public.orders",
                column_name="status",
                distinct_count=5,
                null_count=0,
                null_fraction=0.0,
                min_value="cancelled",
                max_value="shipped",
                top_values=[{"value": "pending", "count": 100}],
                sample_values=["pending", "shipped"],
                profiled_at="2026-04-01T00:00:00",
            )
        )

    result = _handle_profile_column(cfg, {"asset_id": "pg::public.orders", "column": "status"})
    payload = json.loads(result[0].text)

    assert payload["column_name"] == "status"
    assert payload["distinct_count"] == 5
    assert payload["null_count"] == 0
    assert payload["min_value"] == "cancelled"
    assert payload["top_values"] == [{"value": "pending", "count": 100}]


def test_profile_column_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_profile_column(cfg, {"asset_id": "missing", "column": "id"})
    assert "no profile" in result[0].text.lower()


# ---------------------------------------------------------------------------
# atlas_describe_relationship
# ---------------------------------------------------------------------------


def _seed_relationship_data(db_path: Path) -> None:
    with Database(db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders")
        )
        AssetRepository(db).upsert(
            Asset(id="pg::public.customers", source="pg:test", kind="table", name="public.customers")
        )
        EdgeRepository(db).upsert(
            Edge(
                upstream_id="pg::public.customers",
                downstream_id="pg::public.orders",
                kind="fk",
                metadata={"join_guidance": "JOIN on customers.id = orders.customer_id"},
            )
        )


def test_describe_relationship_finds_direct_edge(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    _seed_relationship_data(cfg.db_path)

    result = _handle_describe_relationship(
        cfg, {"asset_a": "pg::public.customers", "asset_b": "pg::public.orders"}
    )
    text = result[0].text

    assert "customers" in text
    assert "orders" in text
    assert "fk" in text
    assert "JOIN on customers.id = orders.customer_id" in text


def test_describe_relationship_no_edges(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders")
        )
        AssetRepository(db).upsert(
            Asset(id="pg::public.customers", source="pg:test", kind="table", name="public.customers")
        )

    result = _handle_describe_relationship(
        cfg, {"asset_a": "pg::public.orders", "asset_b": "pg::public.customers"}
    )
    assert "no" in result[0].text.lower() and ("relationship" in result[0].text.lower() or "edges" in result[0].text.lower())


def test_describe_relationship_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_describe_relationship(cfg, {"asset_a": "missing_a", "asset_b": "missing_b"})
    assert "no direct edges" in result[0].text.lower() or "no relationship" in result[0].text.lower()


# ---------------------------------------------------------------------------
# atlas_search upgrade (FTS)
# ---------------------------------------------------------------------------


def test_search_includes_fts_results(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.transactions", source="pg:test", kind="table", name="public.transactions")
        )
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="pg::public.transactions",
                ownership="finance",
                granularity="one row per txn",
                join_keys=[],
                freshness_guarantee="daily",
                business_logic_summary="Tracks EUR currency conversion for each transaction",
                sensitivity="financial",
                annotated_by="agent:mock",
            )
        )

    result = _handle_search(cfg, {"query": "currency"})
    text = result[0].text

    assert "transactions" in text
    assert "annotation match" in text


def test_search_deduplicates_fts_and_name(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders")
        )
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="pg::public.orders",
                ownership="data",
                granularity="row per order",
                join_keys=[],
                freshness_guarantee="hourly",
                business_logic_summary="orders table with order details",
                sensitivity="internal",
                annotated_by="agent:mock",
            )
        )

    result = _handle_search(cfg, {"query": "orders"})
    text = result[0].text
    assert text.count("pg::public.orders") == 1


# ---------------------------------------------------------------------------
# atlas_find_term
# ---------------------------------------------------------------------------


def test_find_term_returns_fts_matches(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders")
        )
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="pg::public.orders",
                ownership="data",
                granularity="row per order",
                join_keys=[],
                freshness_guarantee="hourly",
                business_logic_summary="Tracks revenue and refund amounts per order",
                sensitivity="financial",
                annotated_by="agent:mock",
            )
        )

    result = _handle_find_term(cfg, {"term": "revenue"})
    text = result[0].text

    assert "orders" in text
    assert "revenue" in text.lower()


def test_find_term_returns_name_matches(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(id="pg::public.revenue_summary", source="pg:test", kind="view", name="public.revenue_summary")
        )

    result = _handle_find_term(cfg, {"term": "revenue"})
    text = result[0].text

    assert "revenue_summary" in text


def test_find_term_not_found(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_find_term(cfg, {"term": "nonexistent_xyz_concept"})
    assert "no assets" in result[0].text.lower()
