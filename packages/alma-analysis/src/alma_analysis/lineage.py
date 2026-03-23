"""Lineage graph construction for asset traversal.

Builds a directed NetworkX graph from Edge objects and exposes BFS-based
upstream/downstream traversal via LineageGraph.
"""

from __future__ import annotations

from collections import deque

import networkx as nx

from alma_analysis.edges import Edge


class LineageGraph:
    """Directed graph of data asset lineage.

    Nodes are asset ID strings. Edges point from upstream -> downstream.
    """

    def __init__(self, graph: nx.DiGraph) -> None:
        self._graph = graph

    def has_asset(self, asset_id: str) -> bool:
        """Return True if asset_id is present in the lineage graph."""
        return asset_id in self._graph

    def upstream(self, asset_id: str, depth: int | None = None) -> list[str]:
        """Return all upstream asset IDs (ancestors) reachable from asset_id."""
        return _bfs(self._graph, asset_id, depth, predecessors=True)

    def downstream(self, asset_id: str, depth: int | None = None) -> list[str]:
        """Return all downstream asset IDs (descendants) reachable from asset_id."""
        return _bfs(self._graph, asset_id, depth, predecessors=False)


def _bfs(graph: nx.DiGraph, start: str, depth: int | None, *, predecessors: bool) -> list[str]:
    visited: set[str] = {start}
    queue: deque[tuple[str, int]] = deque([(start, 0)])
    result: list[str] = []
    while queue:
        node, level = queue.popleft()
        if depth is not None and level >= depth:
            continue
        neighbors = graph.predecessors(node) if predecessors else graph.successors(node)
        for neighbor in neighbors:
            if neighbor not in visited:
                visited.add(neighbor)
                result.append(neighbor)
                queue.append((neighbor, level + 1))
    return result


def compute_lineage(edges: list[Edge]) -> LineageGraph:
    """Build a LineageGraph from a list of Edge objects.

    Args:
        edges: Directed lineage edges (upstream_id -> downstream_id).

    Returns:
        A traversable LineageGraph backed by a NetworkX DiGraph.
    """
    g: nx.DiGraph = nx.DiGraph()
    for edge in edges:
        g.add_edge(edge.upstream_id, edge.downstream_id)
    return LineageGraph(g)
