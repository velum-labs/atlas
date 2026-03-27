"""Tests for Atlas-native candidate derivation."""

from __future__ import annotations

from alma_atlas.analysis import run_analysis
from alma_atlas.analysis.snapshot import (
    AnalysisGraph,
    AnalysisSnapshot,
    QueryRecord,
    TrafficSummary,
)


def test_run_analysis_derives_candidates_from_join_workload() -> None:
    snapshot = AnalysisSnapshot(
        snapshot_version="1",
        exported_at="2026-03-27T00:00:00+00:00",
        atlas_version="0.1.0",
        source_filter="postgres:demo",
        graph=AnalysisGraph(
            queries=[
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
                    execution_count=4,
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
                    execution_count=3,
                ),
                QueryRecord(
                    fingerprint="fp-bootstrap",
                    sql_text="SELECT id FROM public.orders LIMIT 10",
                    tables=["postgres:demo::public.orders"],
                    source="postgres:demo",
                    first_seen=None,
                    last_seen=None,
                    execution_count=9,
                ),
            ]
        ),
        traffic_summary=TrafficSummary(
            query_fingerprint_count=3,
            total_query_executions=16,
            asset_count=0,
            edge_count=0,
        ),
    )

    result = run_analysis(snapshot, min_cluster_size=2)

    assert result.parsed_query_count == 2
    assert result.cluster_count >= 1
    assert result.candidate_count >= 1
    assert any(query.reason == "bootstrap_or_trivial" for query in result.skipped_queries)
    candidate = result.candidates[0]
    assert set(candidate.core_relations) == {"customers", "orders"}
    assert candidate.coverage_weight == 7.0
    assert candidate.support_score > 0.0
