"""Tests for Atlas analysis snapshot builders."""

from __future__ import annotations

from pathlib import Path

from alma_atlas.analysis.snapshot import build_analysis_snapshot, build_analysis_summary
from alma_atlas.testing.analysis_seed import seed_analysis_data
from alma_atlas_store.db import Database
from alma_atlas_store.query_repository import QueryObservation, QueryRepository


def test_build_analysis_snapshot_contains_graph_and_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "atlas.db"
    seed_analysis_data(db_path)

    with Database(db_path) as db:
        snapshot = build_analysis_snapshot(db, source="postgres:demo", top_n=5)

    assert snapshot.source_filter == "postgres:demo"
    assert len(snapshot.graph.assets) == 3
    assert len(snapshot.graph.edges) == 2
    assert len(snapshot.graph.queries) == 2
    assert len(snapshot.graph.consumers) == 1
    assert len(snapshot.graph.consumer_assets) == 1
    assert len(snapshot.graph.schema_snapshots) == 2
    assert snapshot.traffic_summary.query_fingerprint_count == 2
    assert snapshot.traffic_summary.total_query_executions == 3
    assert snapshot.traffic_summary.asset_count == 3
    assert snapshot.traffic_summary.edge_count == 2
    assert snapshot.traffic_summary.top_fingerprints[0].fingerprint == "fp-orders-customers"
    assert snapshot.traffic_summary.top_fingerprints[0].execution_count == 2
    assert snapshot.traffic_summary.top_tables_by_execution_count[0].asset_id == "postgres:demo::public.orders"
    assert snapshot.traffic_summary.top_tables_by_execution_count[0].execution_count == 3


def test_build_analysis_snapshot_filters_by_source(tmp_path: Path) -> None:
    db_path = tmp_path / "atlas.db"
    seed_analysis_data(db_path)

    with Database(db_path) as db:
        snapshot = build_analysis_snapshot(db, source="snowflake:warehouse")

    assert len(snapshot.graph.assets) == 1
    assert len(snapshot.graph.edges) == 0
    assert len(snapshot.graph.queries) == 1
    assert snapshot.traffic_summary.query_fingerprint_count == 1
    assert snapshot.traffic_summary.total_query_executions == 1


def test_build_analysis_summary_computes_aggregate_counts(tmp_path: Path) -> None:
    db_path = tmp_path / "atlas.db"
    seed_analysis_data(db_path)

    with Database(db_path) as db:
        summary = build_analysis_summary(db, source="postgres:demo", top_n=2)

    assert summary.asset_count == 3
    assert summary.edge_count == 2
    assert summary.query_fingerprint_count == 2
    assert summary.total_query_executions == 3
    assert len(summary.top_fingerprints) == 2
    assert len(summary.top_tables_by_execution_count) == 2


def test_build_analysis_summary_deduplicates_tables_per_query(tmp_path: Path) -> None:
    db_path = tmp_path / "atlas.db"

    with Database(db_path) as db:
        queries = QueryRepository(db)
        queries.upsert(
            QueryObservation(
                fingerprint="fp-dup",
                sql_text="SELECT * FROM public.orders o1 JOIN public.orders o2 ON o1.parent_id = o2.id",
                tables=[
                    "postgres:demo::public.orders",
                    "postgres:demo::public.orders",
                ],
                source="postgres:demo",
            )
        )

        summary = build_analysis_summary(db, source="postgres:demo", top_n=5)

    assert summary.query_fingerprint_count == 1
    assert summary.total_query_executions == 1
    assert summary.top_tables_by_execution_count[0].asset_id == "postgres:demo::public.orders"
    assert summary.top_tables_by_execution_count[0].query_fingerprint_count == 1
    assert summary.top_tables_by_execution_count[0].execution_count == 1

