"""Compatibility tests for legacy Alma observatory analysis semantics."""

from __future__ import annotations

from alma_atlas.analysis import run_analysis
from alma_atlas.analysis.snapshot import (
    AnalysisGraph,
    AnalysisSnapshot,
    QueryRecord,
    TrafficSummary,
)


def test_run_analysis_filters_bootstrap_queries_like_legacy_pipeline() -> None:
    snapshot = AnalysisSnapshot(
        snapshot_version="1",
        exported_at="2026-03-27T00:00:00+00:00",
        atlas_version="0.1.0",
        source_filter="postgres:demo",
        graph=AnalysisGraph(
            queries=[
                QueryRecord(
                    fingerprint="fp-bootstrap",
                    sql_text="SELECT id FROM public.orders LIMIT 10",
                    tables=["postgres:demo::public.orders"],
                    source="postgres:demo",
                    first_seen=None,
                    last_seen=None,
                    execution_count=5,
                ),
                QueryRecord(
                    fingerprint="fp-join-1",
                    sql_text=(
                        "SELECT public.orders.id, public.customers.email "
                        "FROM public.orders "
                        "JOIN public.customers ON public.orders.customer_id = public.customers.id "
                        "WHERE public.orders.status = 'paid'"
                    ),
                    tables=[
                        "postgres:demo::public.orders",
                        "postgres:demo::public.customers",
                    ],
                    source="postgres:demo",
                    first_seen=None,
                    last_seen=None,
                    execution_count=3,
                ),
                QueryRecord(
                    fingerprint="fp-join-2",
                    sql_text=(
                        "SELECT public.orders.id, public.customers.email "
                        "FROM public.orders "
                        "JOIN public.customers ON public.orders.customer_id = public.customers.id "
                        "WHERE public.orders.status = 'pending'"
                    ),
                    tables=[
                        "postgres:demo::public.orders",
                        "postgres:demo::public.customers",
                    ],
                    source="postgres:demo",
                    first_seen=None,
                    last_seen=None,
                    execution_count=4,
                ),
            ]
        ),
        traffic_summary=TrafficSummary(
            query_fingerprint_count=3,
            total_query_executions=12,
            asset_count=0,
            edge_count=0,
        ),
    )

    result = run_analysis(snapshot, min_cluster_size=2)

    assert result.parsed_query_count == 2
    assert len(result.skipped_queries) == 1
    assert result.skipped_queries[0].reason == "bootstrap_or_trivial"


def test_run_analysis_avoids_trivial_single_table_candidates() -> None:
    snapshot = AnalysisSnapshot(
        snapshot_version="1",
        exported_at="2026-03-27T00:00:00+00:00",
        atlas_version="0.1.0",
        source_filter="postgres:demo",
        graph=AnalysisGraph(
            queries=[
                QueryRecord(
                    fingerprint="fp-single-1",
                    sql_text="SELECT public.orders.id FROM public.orders WHERE public.orders.id = 42",
                    tables=["postgres:demo::public.orders"],
                    source="postgres:demo",
                    first_seen=None,
                    last_seen=None,
                    execution_count=2,
                ),
                QueryRecord(
                    fingerprint="fp-single-2",
                    sql_text="SELECT public.orders.id FROM public.orders WHERE public.orders.id = 7",
                    tables=["postgres:demo::public.orders"],
                    source="postgres:demo",
                    first_seen=None,
                    last_seen=None,
                    execution_count=2,
                ),
            ]
        ),
        traffic_summary=TrafficSummary(
            query_fingerprint_count=2,
            total_query_executions=4,
            asset_count=0,
            edge_count=0,
        ),
    )

    result = run_analysis(snapshot, min_cluster_size=2)

    assert result.cluster_count >= 1
    assert result.candidate_count == 0
