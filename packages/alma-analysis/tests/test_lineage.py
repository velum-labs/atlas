"""Tests for alma_analysis.lineage — LineageGraph and compute_lineage()."""

from __future__ import annotations

from alma_analysis.edges import Edge
from alma_analysis.lineage import LineageGraph, compute_lineage

# ---------------------------------------------------------------------------
# compute_lineage — graph construction
# ---------------------------------------------------------------------------


def test_compute_lineage_empty() -> None:
    graph = compute_lineage([])
    assert isinstance(graph, LineageGraph)


def test_compute_lineage_single_edge() -> None:
    edges = [Edge(upstream_id="table_a", downstream_id="table_b")]
    graph = compute_lineage(edges)
    assert graph.has_asset("table_a")
    assert graph.has_asset("table_b")


def test_compute_lineage_multiple_edges() -> None:
    edges = [
        Edge(upstream_id="raw", downstream_id="staging"),
        Edge(upstream_id="staging", downstream_id="mart"),
    ]
    graph = compute_lineage(edges)
    assert graph.has_asset("raw")
    assert graph.has_asset("staging")
    assert graph.has_asset("mart")


# ---------------------------------------------------------------------------
# LineageGraph.has_asset
# ---------------------------------------------------------------------------


def test_has_asset_present() -> None:
    graph = compute_lineage([Edge(upstream_id="a", downstream_id="b")])
    assert graph.has_asset("a") is True
    assert graph.has_asset("b") is True


def test_has_asset_absent() -> None:
    graph = compute_lineage([])
    assert graph.has_asset("nonexistent") is False


# ---------------------------------------------------------------------------
# LineageGraph.downstream
# ---------------------------------------------------------------------------


def test_downstream_direct() -> None:
    graph = compute_lineage([Edge(upstream_id="a", downstream_id="b")])
    result = graph.downstream("a")
    assert "b" in result


def test_downstream_multi_hop() -> None:
    edges = [
        Edge(upstream_id="a", downstream_id="b"),
        Edge(upstream_id="b", downstream_id="c"),
    ]
    graph = compute_lineage(edges)
    result = graph.downstream("a")
    assert "b" in result
    assert "c" in result


def test_downstream_depth_1_limits_to_direct() -> None:
    edges = [
        Edge(upstream_id="a", downstream_id="b"),
        Edge(upstream_id="b", downstream_id="c"),
    ]
    graph = compute_lineage(edges)
    result = graph.downstream("a", depth=1)
    assert "b" in result
    assert "c" not in result


def test_downstream_leaf_returns_empty() -> None:
    graph = compute_lineage([Edge(upstream_id="a", downstream_id="b")])
    result = graph.downstream("b")
    assert result == []


def test_downstream_does_not_include_start_node() -> None:
    graph = compute_lineage([Edge(upstream_id="a", downstream_id="b")])
    result = graph.downstream("a")
    assert "a" not in result


def test_downstream_handles_fan_out() -> None:
    edges = [
        Edge(upstream_id="a", downstream_id="b"),
        Edge(upstream_id="a", downstream_id="c"),
        Edge(upstream_id="a", downstream_id="d"),
    ]
    graph = compute_lineage(edges)
    result = graph.downstream("a")
    assert set(result) == {"b", "c", "d"}


# ---------------------------------------------------------------------------
# LineageGraph.upstream
# ---------------------------------------------------------------------------


def test_upstream_direct() -> None:
    graph = compute_lineage([Edge(upstream_id="a", downstream_id="b")])
    result = graph.upstream("b")
    assert "a" in result


def test_upstream_multi_hop() -> None:
    edges = [
        Edge(upstream_id="a", downstream_id="b"),
        Edge(upstream_id="b", downstream_id="c"),
    ]
    graph = compute_lineage(edges)
    result = graph.upstream("c")
    assert "b" in result
    assert "a" in result


def test_upstream_depth_1_limits_to_direct() -> None:
    edges = [
        Edge(upstream_id="a", downstream_id="b"),
        Edge(upstream_id="b", downstream_id="c"),
    ]
    graph = compute_lineage(edges)
    result = graph.upstream("c", depth=1)
    assert "b" in result
    assert "a" not in result


def test_upstream_root_returns_empty() -> None:
    graph = compute_lineage([Edge(upstream_id="a", downstream_id="b")])
    result = graph.upstream("a")
    assert result == []


def test_upstream_does_not_include_start_node() -> None:
    graph = compute_lineage([Edge(upstream_id="a", downstream_id="b")])
    result = graph.upstream("b")
    assert "b" not in result


def test_upstream_handles_fan_in() -> None:
    edges = [
        Edge(upstream_id="a", downstream_id="d"),
        Edge(upstream_id="b", downstream_id="d"),
        Edge(upstream_id="c", downstream_id="d"),
    ]
    graph = compute_lineage(edges)
    result = graph.upstream("d")
    assert set(result) == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# BFS cycle safety
# ---------------------------------------------------------------------------


def test_no_infinite_loop_with_cycle() -> None:
    # Deliberately create a cycle — BFS should not loop forever
    edges = [
        Edge(upstream_id="a", downstream_id="b"),
        Edge(upstream_id="b", downstream_id="a"),
    ]
    graph = compute_lineage(edges)
    result = graph.downstream("a")
    # Should terminate and return exactly {"b"} (a is visited=start)
    assert "b" in result
    assert "a" not in result


# ---------------------------------------------------------------------------
# depth=0 edge case
# ---------------------------------------------------------------------------


def test_depth_zero_returns_empty() -> None:
    edges = [Edge(upstream_id="a", downstream_id="b")]
    graph = compute_lineage(edges)
    result = graph.downstream("a", depth=0)
    assert result == []
