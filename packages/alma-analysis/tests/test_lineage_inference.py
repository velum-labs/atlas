"""Tests for alma_analysis.lineage_inference — InferredLineageEngine and infer_lineage()."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from alma_connectors.source_adapter import ObservedQueryEvent
from alma_connectors.source_adapter_v2 import (
    LineageEdge,
    LineageEdgeKind,
)

from alma_analysis.lineage_inference import (
    InferredLineageEngine,
    _normalize_table_name,
    _recency_multiplier,
    infer_lineage,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def _event(
    sql: str,
    *,
    source_name: str = "pg_prod",
    captured_at: datetime | None = None,
    event_id: str = "evt-1",
) -> ObservedQueryEvent:
    return ObservedQueryEvent(
        captured_at=captured_at or _NOW,
        sql=sql,
        source_name=source_name,
        query_type="SELECT",
        event_id=event_id,
    )


def _declared_edge(src: str, tgt: str) -> LineageEdge:
    return LineageEdge(
        source_object=src,
        target_object=tgt,
        edge_kind=LineageEdgeKind.DECLARED,
        confidence=1.0,
    )


# ---------------------------------------------------------------------------
# _normalize_table_name
# ---------------------------------------------------------------------------


def test_normalize_strips_schema() -> None:
    assert _normalize_table_name("public.orders") == "orders"


def test_normalize_strips_multiple_dots() -> None:
    assert _normalize_table_name("myproject.dataset.orders") == "orders"


def test_normalize_lowercases() -> None:
    assert _normalize_table_name("Public.Orders") == "orders"


def test_normalize_bare_name() -> None:
    assert _normalize_table_name("orders") == "orders"


# ---------------------------------------------------------------------------
# _recency_multiplier
# ---------------------------------------------------------------------------


def test_recency_recent() -> None:
    assert _recency_multiplier(timedelta(days=0)) == 1.0
    assert _recency_multiplier(timedelta(days=29)) == 1.0


def test_recency_mid() -> None:
    assert _recency_multiplier(timedelta(days=31)) == 0.9
    assert _recency_multiplier(timedelta(days=89)) == 0.9


def test_recency_old() -> None:
    assert _recency_multiplier(timedelta(days=91)) == 0.7
    assert _recency_multiplier(timedelta(days=365)) == 0.7


# ---------------------------------------------------------------------------
# SQL parsing → correct edges
# ---------------------------------------------------------------------------


def test_insert_select_produces_edge() -> None:
    sql = "INSERT INTO public.summary SELECT id, amount FROM public.orders"
    engine = InferredLineageEngine([_event(sql)], dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    assert len(edges) == 1
    assert edges[0].source_object == "public.orders"
    assert edges[0].target_object == "public.summary"
    assert edges[0].edge_kind == LineageEdgeKind.INFERRED_SQL


def test_plain_select_uses_source_name_as_target() -> None:
    sql = "SELECT id FROM public.customers"
    engine = InferredLineageEngine(
        [_event(sql, source_name="analytics")], dialect="postgres", now=_NOW
    )
    edges = engine.build_edges()
    assert len(edges) == 1
    assert edges[0].source_object == "public.customers"
    assert edges[0].target_object == "analytics"


def test_join_produces_multiple_edges() -> None:
    sql = (
        "INSERT INTO analytics.report "
        "SELECT o.id, c.name FROM public.orders o JOIN public.customers c ON o.cid = c.id"
    )
    engine = InferredLineageEngine([_event(sql)], dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    sources = {e.source_object for e in edges}
    assert "public.orders" in sources
    assert "public.customers" in sources
    for e in edges:
        assert e.target_object == "analytics.report"


def test_cte_tables_are_excluded() -> None:
    # Plain SELECT with CTE: extract_lineage properly filters out the CTE alias.
    # "cte" should not appear as a source; only the real base table is emitted.
    sql = "WITH cte AS (SELECT id FROM public.base) SELECT id FROM cte"
    engine = InferredLineageEngine([_event(sql, source_name="pg_prod")], dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    sources = {e.source_object for e in edges}
    assert not any("cte" in s.lower() for s in sources)


def test_empty_events_returns_empty() -> None:
    engine = InferredLineageEngine([], dialect="postgres", now=_NOW)
    assert engine.build_edges() == []


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------


def test_single_event_max_freq_boost() -> None:
    """Single event → max freq_boost (0.1), recent → multiplier 1.0.

    algebrakit parses INSERT...SELECT successfully → column-level, base=0.9.
    """
    sql = "INSERT INTO public.target SELECT * FROM public.source"
    engine = InferredLineageEngine([_event(sql)], dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    assert len(edges) == 1
    # column-level: base=0.9, freq_boost=0.1, recency=1.0 → min((0.9+0.1)*1.0, 1.0)=1.0
    assert abs(edges[0].confidence - 1.0) < 1e-9


def test_confidence_varies_with_frequency() -> None:
    """Higher-frequency edges should have higher confidence than lower-frequency ones."""
    sql_a = "INSERT INTO public.target SELECT * FROM public.high_freq"
    sql_b = "INSERT INTO public.target SELECT * FROM public.low_freq"
    events = [_event(sql_a, event_id=f"a-{i}") for i in range(10)] + [
        _event(sql_b, event_id="b-1")
    ]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    edges = engine.build_edges()

    by_source = {e.source_object: e.confidence for e in edges}
    assert by_source["public.high_freq"] > by_source["public.low_freq"]


def test_confidence_max_count_edge_gets_full_boost() -> None:
    """The edge with max frequency gets freq_boost = 0.1."""
    sql = "INSERT INTO public.target SELECT * FROM public.source"
    events = [_event(sql, event_id=f"e-{i}") for i in range(5)]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    assert len(edges) == 1
    # max_count=5, count=5 → log(5)/log(5)=1 → freq_boost=0.1
    # column-level: base=0.9, recency=1.0 → min((0.9+0.1)*1.0, 1.0)=1.0
    assert abs(edges[0].confidence - 1.0) < 1e-9


def test_confidence_multi_edge_frequency_boost() -> None:
    """Edge with count < max_count gets a proportionally smaller boost."""
    sql_common = "INSERT INTO public.target SELECT * FROM public.common"
    sql_rare = "INSERT INTO public.target SELECT * FROM public.rare"
    # common: count=4, rare: count=2, max_count=4
    events = [_event(sql_common, event_id=f"c-{i}") for i in range(4)] + [
        _event(sql_rare, event_id=f"r-{i}") for i in range(2)
    ]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    by_source = {e.source_object: e.confidence for e in edges}

    # Both are column-level (algebrakit parses INSERT...SELECT successfully), base=0.9
    # common: freq_boost = log(4)/log(4)*0.1 = 0.1 → min((0.9+0.1)*1.0, 1.0) = 1.0
    expected_common = min((0.9 + 0.1) * 1.0, 1.0)
    # rare: freq_boost = log(2)/log(4)*0.1 = 0.05 → (0.9+0.05)*1.0 = 0.95
    expected_rare = (0.9 + math.log(2) / math.log(4) * 0.1) * 1.0

    assert abs(by_source["public.common"] - expected_common) < 1e-9
    assert abs(by_source["public.rare"] - expected_rare) < 1e-9


# ---------------------------------------------------------------------------
# Recency decay
# ---------------------------------------------------------------------------


def test_recency_decay_30_days() -> None:
    """Events just over 30 days old get 0.9x recency multiplier."""
    old_time = _NOW - timedelta(days=31)
    sql = "INSERT INTO public.target SELECT * FROM public.source"
    engine = InferredLineageEngine(
        [_event(sql, captured_at=old_time)], dialect="postgres", now=_NOW
    )
    edges = engine.build_edges()
    # column-level: base=0.9, freq_boost=0.1, recency=0.9
    assert abs(edges[0].confidence - (0.9 + 0.1) * 0.9) < 1e-9


def test_recency_decay_90_days() -> None:
    """Events over 90 days old get 0.7x recency multiplier."""
    old_time = _NOW - timedelta(days=91)
    sql = "INSERT INTO public.target SELECT * FROM public.source"
    engine = InferredLineageEngine(
        [_event(sql, captured_at=old_time)], dialect="postgres", now=_NOW
    )
    edges = engine.build_edges()
    # column-level: base=0.9, freq_boost=0.1, recency=0.7
    assert abs(edges[0].confidence - (0.9 + 0.1) * 0.7) < 1e-9


def test_recency_uses_most_recent_event() -> None:
    """When a pair has multiple events, most recent timestamp drives recency."""
    sql = "INSERT INTO public.target SELECT * FROM public.source"
    recent = _NOW - timedelta(days=5)
    old = _NOW - timedelta(days=91)
    events = [
        _event(sql, captured_at=recent, event_id="recent"),
        _event(sql, captured_at=old, event_id="old"),
    ]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    # Most recent is 5 days ago → recency=1.0; count=2, max_count=2 → freq_boost=0.1
    # column-level: base=0.9 → min((0.9+0.1)*1.0, 1.0) = 1.0
    assert abs(edges[0].confidence - min((0.9 + 0.1) * 1.0, 1.0)) < 1e-9


# ---------------------------------------------------------------------------
# Declared edges take precedence
# ---------------------------------------------------------------------------


def test_declared_edges_suppress_inferred() -> None:
    sql = "INSERT INTO public.target SELECT * FROM public.source"
    events = [_event(sql)]
    declared = [_declared_edge("public.source", "public.target")]
    snapshot = infer_lineage(events, declared, "postgres", now=_NOW)

    edges_by_kind = {e.edge_kind: e for e in snapshot.edges}
    # Declared edge is kept
    assert LineageEdgeKind.DECLARED in edges_by_kind
    # Inferred edge for the same pair is dropped
    inferred_pairs = [
        (e.source_object, e.target_object)
        for e in snapshot.edges
        if e.edge_kind == LineageEdgeKind.INFERRED_SQL
    ]
    assert ("public.source", "public.target") not in inferred_pairs


def test_declared_edges_pass_through_unchanged() -> None:
    declared = [_declared_edge("public.source", "public.target")]
    snapshot = infer_lineage([], declared, "postgres", now=_NOW)
    assert len(snapshot.edges) == 1
    assert snapshot.edges[0].edge_kind == LineageEdgeKind.DECLARED
    assert snapshot.edges[0].confidence == 1.0


def test_inferred_edge_kept_when_no_declared_overlap() -> None:
    sql = "INSERT INTO public.b SELECT * FROM public.a"
    declared = [_declared_edge("other.x", "other.y")]
    snapshot = infer_lineage([_event(sql)], declared, "postgres", now=_NOW)

    kinds = {e.edge_kind for e in snapshot.edges}
    assert LineageEdgeKind.DECLARED in kinds
    assert LineageEdgeKind.INFERRED_SQL in kinds


# ---------------------------------------------------------------------------
# Cross-system matching
# ---------------------------------------------------------------------------


def test_cross_system_matching_emits_heuristic_edge() -> None:
    sql_pg = "SELECT id FROM public.orders"
    sql_bq = "SELECT id FROM analytics.orders"
    events = [
        _event(sql_pg, source_name="postgres_prod"),
        _event(sql_bq, source_name="bigquery_prod"),
    ]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    cross = engine.build_cross_system_edges()
    assert len(cross) == 1
    assert cross[0].edge_kind == LineageEdgeKind.HEURISTIC
    assert cross[0].confidence == 0.5
    sources = {cross[0].source_object, cross[0].target_object}
    assert "public.orders" in sources
    assert "analytics.orders" in sources


def test_cross_system_metadata_contains_normalized_name() -> None:
    sql_pg = "SELECT id FROM public.users"
    sql_bq = "SELECT id FROM dataset.users"
    events = [
        _event(sql_pg, source_name="pg"),
        _event(sql_bq, source_name="bq"),
    ]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    cross = engine.build_cross_system_edges()
    assert len(cross) == 1
    assert cross[0].metadata.get("normalized_name") == "users"
    assert cross[0].metadata.get("cross_system") is True


def test_no_cross_system_edge_for_single_system() -> None:
    sql1 = "SELECT id FROM public.orders"
    sql2 = "SELECT id FROM public.orders"
    events = [
        _event(sql1, source_name="pg_prod", event_id="e1"),
        _event(sql2, source_name="pg_prod", event_id="e2"),
    ]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    assert engine.build_cross_system_edges() == []


def test_cross_system_suppressed_by_declared() -> None:
    sql_pg = "SELECT id FROM public.orders"
    sql_bq = "SELECT id FROM analytics.orders"
    events = [
        _event(sql_pg, source_name="postgres_prod"),
        _event(sql_bq, source_name="bigquery_prod"),
    ]
    declared = [_declared_edge("public.orders", "analytics.orders")]
    snapshot = infer_lineage(events, declared, "postgres", now=_NOW)
    heuristic_edges = [e for e in snapshot.edges if e.edge_kind == LineageEdgeKind.HEURISTIC]
    assert all(
        (e.source_object, e.target_object) != ("public.orders", "analytics.orders")
        for e in heuristic_edges
    )


# ---------------------------------------------------------------------------
# Graceful handling of unparseable SQL
# ---------------------------------------------------------------------------


def test_unparseable_sql_is_skipped() -> None:
    bad_sql = "THIS IS NOT VALID SQL $$$$"
    good_sql = "INSERT INTO public.target SELECT * FROM public.source"
    events = [
        _event(bad_sql, event_id="bad"),
        _event(good_sql, event_id="good"),
    ]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    edges = engine.build_edges()
    # Should have edges from good_sql only, no crash
    assert len(edges) >= 1
    sources = {e.source_object for e in edges}
    assert "public.source" in sources


def test_all_unparseable_sql_returns_empty() -> None:
    events = [_event("$$$ GARBAGE $$$", event_id=f"bad-{i}") for i in range(5)]
    engine = InferredLineageEngine(events, dialect="postgres", now=_NOW)
    assert engine.build_edges() == []
    assert engine.build_cross_system_edges() == []


# ---------------------------------------------------------------------------
# LineageSnapshot structure
# ---------------------------------------------------------------------------


def test_infer_lineage_returns_snapshot() -> None:
    sql = "INSERT INTO public.target SELECT * FROM public.source"
    snapshot = infer_lineage([_event(sql)], [], "postgres", now=_NOW)
    from alma_connectors.source_adapter_v2 import LineageSnapshot

    assert isinstance(snapshot, LineageSnapshot)
    assert snapshot.meta.row_count == len(snapshot.edges)


def test_infer_lineage_meta_capability_is_lineage() -> None:
    from alma_connectors.source_adapter_v2 import AdapterCapability

    snapshot = infer_lineage([], [], "postgres", now=_NOW)
    assert snapshot.meta.capability == AdapterCapability.LINEAGE


def test_infer_lineage_empty_inputs() -> None:
    snapshot = infer_lineage([], [], "postgres", now=_NOW)
    assert snapshot.edges == ()
    assert snapshot.meta.row_count == 0
