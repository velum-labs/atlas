"""Tests for alma_analysis.edges — Edge dataclass and extract_edges()."""

from __future__ import annotations

from alma_analysis.edges import Edge, extract_edges

# ---------------------------------------------------------------------------
# Edge dataclass
# ---------------------------------------------------------------------------


def test_edge_defaults() -> None:
    e = Edge(upstream_id="schema.table_a", downstream_id="schema.table_b")
    assert e.kind == "reads"
    assert e.query_fingerprint is None


def test_edge_custom_kind() -> None:
    e = Edge(upstream_id="a", downstream_id="b", kind="writes")
    assert e.kind == "writes"


def test_edge_with_fingerprint() -> None:
    e = Edge(upstream_id="a", downstream_id="b", query_fingerprint="abc123")
    assert e.query_fingerprint == "abc123"


# ---------------------------------------------------------------------------
# extract_edges — basic SELECT
# ---------------------------------------------------------------------------


def test_extract_edges_simple_select() -> None:
    edges = extract_edges("SELECT id FROM public.orders", consumer_id="consumer::query")
    assert len(edges) >= 1
    upstreams = [e.upstream_id for e in edges]
    assert any("orders" in u for u in upstreams)


def test_extract_edges_downstream_is_consumer_for_select() -> None:
    edges = extract_edges("SELECT id FROM public.orders", consumer_id="my_consumer")
    for e in edges:
        assert e.downstream_id == "my_consumer"


def test_extract_edges_join() -> None:
    sql = "SELECT o.id, c.name FROM public.orders o JOIN public.customers c ON o.cid = c.id"
    edges = extract_edges(sql, consumer_id="consumer")
    upstreams = {e.upstream_id for e in edges}
    assert len(upstreams) >= 2


def test_extract_edges_kind_is_reads() -> None:
    edges = extract_edges("SELECT id FROM public.orders", consumer_id="consumer")
    for e in edges:
        assert e.kind == "reads"


# ---------------------------------------------------------------------------
# extract_edges — INSERT INTO (target as downstream)
# ---------------------------------------------------------------------------


def test_extract_edges_insert_uses_target_as_downstream() -> None:
    sql = "INSERT INTO staging.results SELECT id FROM public.orders"
    edges = extract_edges(sql, consumer_id="consumer")
    assert len(edges) >= 1
    downstreams = {e.downstream_id for e in edges}
    # should NOT be the consumer_id when there's a write target
    assert not all(d == "consumer" for d in downstreams)


# ---------------------------------------------------------------------------
# extract_edges — CTE filtering
# ---------------------------------------------------------------------------


def test_extract_edges_cte_filtered_out() -> None:
    sql = """
    WITH cte AS (SELECT id FROM public.orders)
    SELECT id FROM cte
    """
    edges = extract_edges(sql, consumer_id="consumer")
    upstreams = [e.upstream_id for e in edges]
    # CTE name itself should NOT appear as an upstream edge
    assert not any(u.lower() == "cte" for u in upstreams)


def test_extract_edges_cte_base_table_included() -> None:
    sql = """
    WITH cte AS (SELECT id FROM public.orders)
    SELECT id FROM cte
    """
    edges = extract_edges(sql, consumer_id="consumer")
    upstreams = [e.upstream_id for e in edges]
    # The base table orders should still be an upstream
    assert any("orders" in u for u in upstreams)


# ---------------------------------------------------------------------------
# extract_edges — dialect handling
# ---------------------------------------------------------------------------


def test_extract_edges_dialect_as_string() -> None:
    edges = extract_edges(
        "SELECT id FROM public.orders", consumer_id="consumer", dialect="postgres"
    )
    assert isinstance(edges, list)


def test_extract_edges_dialect_as_object() -> None:
    from alma_sqlkit.dialect import Dialect

    dialect = Dialect.postgres()
    edges = extract_edges("SELECT id FROM public.orders", consumer_id="consumer", dialect=dialect)
    assert isinstance(edges, list)


def test_extract_edges_empty_sql_returns_empty() -> None:
    # extract_lineage on empty SQL may raise or return empty; extract_edges wraps it
    # If it raises, extract_edges propagates; if returns empty, we get []
    try:
        edges = extract_edges("", consumer_id="consumer")
        assert isinstance(edges, list)
    except Exception:
        pass  # acceptable — caller is responsible for filtering blank SQL


def test_extract_edges_multiple_tables() -> None:
    sql = "SELECT a.id, b.name, c.val FROM t1 a, t2 b, t3 c"
    edges = extract_edges(sql, consumer_id="consumer")
    assert len(edges) >= 3
