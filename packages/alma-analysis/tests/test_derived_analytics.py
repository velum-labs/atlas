"""Tests for the derived analytics engine."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from alma_connectors.source_adapter import ObservedQueryEvent

from alma_analysis.derived_analytics import (
    DerivedAnalytics,
    _classify_source_type,
    _normalize_sql,
    _query_hash,
    compute_analytics,
    compute_analytics_server_side,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, tzinfo=UTC)


def _event(
    sql: str = "SELECT 1",
    query_type: str = "select",
    database_user: str | None = "alice",
    duration_ms: float | None = 10.0,
    source_name: str = "app",
    metadata: dict | None = None,
) -> ObservedQueryEvent:
    return ObservedQueryEvent(
        captured_at=_NOW,
        sql=sql,
        source_name=source_name,
        query_type=query_type,
        database_user=database_user,
        duration_ms=duration_ms,
        metadata=metadata or {},
    )


# ---------------------------------------------------------------------------
# _classify_source_type
# ---------------------------------------------------------------------------


class TestClassifySourceType:
    def test_airflow_dag_id_in_metadata(self) -> None:
        ev = _event(metadata={"dag_id": "my_dag"})
        assert _classify_source_type(ev) == "scheduled"

    def test_airflow_consumer_source_type(self) -> None:
        ev = _event(metadata={"consumer_source_type": "airflow"})
        assert _classify_source_type(ev) == "scheduled"

    def test_airflow_label_in_labels_dict(self) -> None:
        ev = _event(metadata={"labels": {"dag_id": "etl_dag"}})
        assert _classify_source_type(ev) == "scheduled"

    def test_batch_from_source_name(self) -> None:
        ev = _event(source_name="etl-worker")
        assert _classify_source_type(ev) == "batch"

    def test_batch_from_pipeline_user(self) -> None:
        ev = _event(source_name="loader", database_user="pipeline_user")
        assert _classify_source_type(ev) == "batch"

    def test_service_account_from_application_consumer_type(self) -> None:
        ev = _event(source_name="reporting-api", metadata={"consumer_source_type": "application"})
        assert _classify_source_type(ev) == "service_account"

    def test_service_account_from_svc_prefix_user(self) -> None:
        ev = _event(source_name="app", database_user="svc_metrics")
        assert _classify_source_type(ev) == "service_account"

    def test_service_account_from_robot_source_name(self) -> None:
        ev = _event(source_name="robot-deploy")
        assert _classify_source_type(ev) == "service_account"

    def test_ad_hoc_default(self) -> None:
        ev = _event(source_name="psql", database_user="alice", metadata={})
        assert _classify_source_type(ev) == "ad_hoc"

    def test_user_consumer_type_is_ad_hoc(self) -> None:
        ev = _event(metadata={"consumer_source_type": "user"})
        assert _classify_source_type(ev) == "ad_hoc"


# ---------------------------------------------------------------------------
# _normalize_sql / _query_hash
# ---------------------------------------------------------------------------


class TestNormalizeSql:
    def test_strips_whitespace(self) -> None:
        assert _normalize_sql("SELECT  1") == "select ?"

    def test_replaces_integer_literal(self) -> None:
        norm = _normalize_sql("SELECT * FROM t WHERE id = 42")
        assert "42" not in norm
        assert "?" in norm

    def test_replaces_string_literal(self) -> None:
        norm = _normalize_sql("SELECT * FROM t WHERE name = 'alice'")
        assert "alice" not in norm

    def test_equivalent_queries_same_hash(self) -> None:
        h1 = _query_hash(_normalize_sql("SELECT * FROM t WHERE id = 1"))
        h2 = _query_hash(_normalize_sql("SELECT * FROM t WHERE id = 99"))
        assert h1 == h2

    def test_different_queries_different_hash(self) -> None:
        h1 = _query_hash(_normalize_sql("SELECT * FROM t"))
        h2 = _query_hash(_normalize_sql("SELECT * FROM other_table"))
        assert h1 != h2


# ---------------------------------------------------------------------------
# QuerySourceBreakdown
# ---------------------------------------------------------------------------


class TestQuerySourceBreakdown:
    def test_counts_each_source_type(self) -> None:
        events = [
            _event(source_name="psql", database_user="alice"),
            _event(metadata={"dag_id": "daily"}),
            _event(metadata={"consumer_source_type": "application"}, source_name="api"),
            _event(source_name="etl-loader"),
        ]
        result = compute_analytics(events)
        bd = result.source_breakdown
        assert bd.ad_hoc == 1
        assert bd.scheduled == 1
        assert bd.service_account == 1
        assert bd.batch == 1
        assert bd.total == 4

    def test_fractions_sum_to_one(self) -> None:
        events = [_event()] * 4
        bd = compute_analytics(events).source_breakdown
        assert abs(sum(bd.fractions.values()) - 1.0) < 1e-9

    def test_empty_fractions_all_zero(self) -> None:
        bd = compute_analytics([]).source_breakdown
        assert all(v == 0.0 for v in bd.fractions.values())

    def test_all_ad_hoc(self) -> None:
        events = [_event() for _ in range(5)]
        bd = compute_analytics(events).source_breakdown
        assert bd.ad_hoc == 5
        assert bd.scheduled == bd.service_account == bd.batch == 0


# ---------------------------------------------------------------------------
# FrequentQuery
# ---------------------------------------------------------------------------


class TestFrequentQuery:
    def test_groups_identical_normalised_queries(self) -> None:
        events = [
            _event(sql="SELECT * FROM t WHERE id = 1", duration_ms=10.0),
            _event(sql="SELECT * FROM t WHERE id = 2", duration_ms=20.0),
            _event(sql="SELECT * FROM t WHERE id = 3", duration_ms=30.0),
        ]
        result = compute_analytics(events)
        assert len(result.frequent_queries) == 1
        fq = result.frequent_queries[0]
        assert fq.execution_count == 3
        assert abs(fq.avg_duration_ms - 20.0) < 1e-9

    def test_different_queries_different_groups(self) -> None:
        events = [
            _event(sql="SELECT 1"),
            _event(sql="SELECT 2"),  # same normalised form
            _event(sql="INSERT INTO t VALUES (1)"),
        ]
        result = compute_analytics(events)
        assert len(result.frequent_queries) == 2

    def test_sorted_by_execution_count_descending(self) -> None:
        events = [
            _event(sql="SELECT a FROM t"),
            _event(sql="SELECT b FROM t"),
            _event(sql="SELECT b FROM t"),
            _event(sql="SELECT b FROM t"),
        ]
        result = compute_analytics(events)
        counts = [fq.execution_count for fq in result.frequent_queries]
        assert counts == sorted(counts, reverse=True)

    def test_avg_duration_none_when_no_duration(self) -> None:
        ev = _event(sql="SELECT 1", duration_ms=None)
        fq = compute_analytics([ev]).frequent_queries[0]
        assert fq.avg_duration_ms is None

    def test_avg_bytes_from_metadata(self) -> None:
        events = [
            _event(sql="SELECT 1", metadata={"bytes_processed": 100}),
            _event(sql="SELECT 2", metadata={"bytes_processed": 200}),
        ]
        fq = compute_analytics(events).frequent_queries[0]
        assert fq.avg_bytes_processed == 150.0

    def test_source_types_collected(self) -> None:
        events = [
            _event(sql="SELECT 1", source_name="psql"),
            _event(sql="SELECT 2", metadata={"dag_id": "dag"}),
        ]
        result = compute_analytics(events)
        assert len(result.frequent_queries) == 1
        assert "ad_hoc" in result.frequent_queries[0].source_types
        assert "scheduled" in result.frequent_queries[0].source_types


# ---------------------------------------------------------------------------
# TableAccessSummary
# ---------------------------------------------------------------------------


class TestTableAccessSummary:
    def test_counts_table_references(self) -> None:
        events = [
            _event(metadata={"referenced_tables": ["schema.orders"]}),
            _event(metadata={"referenced_tables": ["schema.orders", "schema.customers"]}),
        ]
        result = compute_analytics(events)
        by_name = {t.table_name: t for t in result.table_access}
        assert by_name["schema.orders"].access_count == 2
        assert by_name["schema.customers"].access_count == 1

    def test_distinct_users_collected(self) -> None:
        events = [
            _event(metadata={"referenced_tables": ["t"]}, database_user="alice"),
            _event(metadata={"referenced_tables": ["t"]}, database_user="bob"),
            _event(metadata={"referenced_tables": ["t"]}, database_user="alice"),
        ]
        ta = compute_analytics(events).table_access[0]
        assert ta.distinct_users == {"alice", "bob"}

    def test_query_types_collected(self) -> None:
        events = [
            _event(sql="SELECT 1", query_type="select", metadata={"referenced_tables": ["t"]}),
            _event(sql="INSERT INTO t VALUES (1)", query_type="insert", metadata={"referenced_tables": ["t"]}),  # noqa: E501
        ]
        ta = compute_analytics(events).table_access[0]
        assert "select" in ta.query_types
        assert "insert" in ta.query_types

    def test_sorted_by_access_count_descending(self) -> None:
        events = [
            _event(metadata={"referenced_tables": ["a", "b", "b", "b"]}),
        ]
        result = compute_analytics(events)
        counts = [t.access_count for t in result.table_access]
        assert counts == sorted(counts, reverse=True)

    def test_no_referenced_tables_produces_no_summary(self) -> None:
        events = [_event(metadata={})]
        assert compute_analytics(events).table_access == []

    def test_empty_referenced_tables_list(self) -> None:
        events = [_event(metadata={"referenced_tables": []})]
        assert compute_analytics(events).table_access == []


# ---------------------------------------------------------------------------
# UserActivitySummary
# ---------------------------------------------------------------------------


class TestUserActivitySummary:
    def test_counts_total_queries_per_user(self) -> None:
        events = [
            _event(database_user="alice"),
            _event(database_user="alice"),
            _event(database_user="bob"),
        ]
        by_user = {u.user: u for u in compute_analytics(events).user_activity}
        assert by_user["alice"].total_queries == 2
        assert by_user["bob"].total_queries == 1

    def test_select_count(self) -> None:
        events = [
            _event(query_type="select", database_user="alice"),
            _event(query_type="SELECT", database_user="alice"),
            _event(query_type="insert", database_user="alice"),
        ]
        u = compute_analytics(events).user_activity[0]
        assert u.select_count == 2

    def test_dml_count(self) -> None:
        events = [
            _event(query_type="insert", database_user="alice"),
            _event(query_type="update", database_user="alice"),
            _event(query_type="delete", database_user="alice"),
            _event(query_type="select", database_user="alice"),
        ]
        u = compute_analytics(events).user_activity[0]
        assert u.dml_count == 3
        assert u.select_count == 1

    def test_unknown_user_label(self) -> None:
        events = [_event(database_user=None)]
        u = compute_analytics(events).user_activity[0]
        assert u.user == "<unknown>"

    def test_bytes_processed_summed(self) -> None:
        events = [
            _event(database_user="alice", metadata={"bytes_processed": 100}),
            _event(database_user="alice", metadata={"bytes_processed": 200}),
        ]
        u = compute_analytics(events).user_activity[0]
        assert u.total_bytes_processed == 300.0

    def test_sorted_by_total_queries_descending(self) -> None:
        events = (
            [_event(database_user="power")] * 5
            + [_event(database_user="casual")] * 2
        )
        result = compute_analytics(events)
        counts = [u.total_queries for u in result.user_activity]
        assert counts == sorted(counts, reverse=True)


# ---------------------------------------------------------------------------
# DerivedAnalytics (end-to-end)
# ---------------------------------------------------------------------------


class TestComputeAnalytics:
    def test_empty_events(self) -> None:
        result = compute_analytics([])
        assert result.event_count == 0
        assert result.source_breakdown.total == 0
        assert result.frequent_queries == []
        assert result.table_access == []
        assert result.user_activity == []

    def test_event_count(self) -> None:
        events = [_event() for _ in range(7)]
        assert compute_analytics(events).event_count == 7

    def test_returns_derived_analytics_type(self) -> None:
        result = compute_analytics([_event()])
        assert isinstance(result, DerivedAnalytics)


# ---------------------------------------------------------------------------
# compute_analytics_server_side
# ---------------------------------------------------------------------------


class TestComputeAnalyticsServerSide:
    @pytest.mark.asyncio
    async def test_returns_same_as_client_side_without_adapter_io(self) -> None:
        adapter = AsyncMock()
        adapter.execute_query = AsyncMock(return_value=None)
        persisted = object()
        events = [_event()]

        result = await compute_analytics_server_side(adapter, persisted, events)

        expected = compute_analytics(events)
        assert result.event_count == expected.event_count
        assert result.source_breakdown.total == expected.source_breakdown.total
        adapter.execute_query.assert_not_called()

    @pytest.mark.asyncio
    async def test_ignores_adapter_execute_query_even_when_present(self) -> None:
        adapter = AsyncMock()
        adapter.execute_query = AsyncMock(side_effect=RuntimeError("not supported"))
        persisted = object()
        events = [_event(), _event(source_name="etl-job")]

        result = await compute_analytics_server_side(adapter, persisted, events)

        assert result.event_count == 2
        assert isinstance(result, DerivedAnalytics)
        adapter.execute_query.assert_not_called()
