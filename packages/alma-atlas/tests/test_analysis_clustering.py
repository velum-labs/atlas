"""Tests for Atlas workload clustering."""

from __future__ import annotations

from alma_atlas.analysis.clustering import cluster_workload, to_analysis_clusters
from alma_atlas.analysis.snapshot import QueryRecord
from alma_atlas.analysis.workload import build_workload


def test_cluster_workload_groups_similar_join_patterns() -> None:
    queries = [
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
            execution_count=2,
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
            fingerprint="fp-orders-only",
            sql_text="SELECT public.orders.id FROM public.orders WHERE public.orders.id = 42",
            tables=["postgres:demo::public.orders"],
            source="postgres:demo",
            first_seen=None,
            last_seen=None,
            execution_count=1,
        ),
    ]

    workload_result = build_workload(queries)
    raw_clusters = cluster_workload(workload_result.workload, min_cluster_size=2)
    clusters = sorted(
        to_analysis_clusters(raw_clusters),
        key=lambda cluster: cluster.total_weight,
        reverse=True,
    )

    assert len(clusters) == 2
    assert clusters[0].total_weight == 5.0
    assert set(clusters[0].common_relations) == {"customers", "orders"}
    assert clusters[0].pattern_ids == ["fp-join-1", "fp-join-2"]
