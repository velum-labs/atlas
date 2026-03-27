"""Tests for Atlas workload construction from snapshot queries."""

from __future__ import annotations

from alma_atlas.analysis.workload import build_workload
from alma_atlas.analysis.snapshot import QueryRecord


def test_build_workload_parses_weighted_patterns_and_skips_invalid_queries() -> None:
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
            execution_count=5,
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
            execution_count=10,
        ),
        QueryRecord(
            fingerprint="fp-invalid",
            sql_text="SELECT FROM",
            tables=["postgres:demo::public.orders"],
            source="postgres:demo",
            first_seen=None,
            last_seen=None,
            execution_count=1,
        ),
    ]

    result = build_workload(queries)

    assert result.total_input_queries == 4
    assert result.parsed_query_count == 2
    assert len(result.workload.patterns) == 2
    assert [pattern.id for pattern in result.workload.patterns] == [
        "fp-join-1",
        "fp-join-2",
    ]
    assert [pattern.weight for pattern in result.workload.patterns] == [5.0, 3.0]
    assert {query.reason.split(":", 1)[0] for query in result.skipped_queries} == {
        "bootstrap_or_trivial",
        "parse_error",
    }
