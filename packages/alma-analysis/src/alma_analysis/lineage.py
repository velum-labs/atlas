"""Lineage computation for alma-analysis.

Builds a directed acyclic graph (DAG) of asset dependencies from a set of
edges. Provides traversal utilities for upstream/downstream impact analysis.

Uses networkx for graph operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx

from alma_analysis.edges import Edge


@dataclass
class LineageGraph:
    """A directed lineage graph over data assets.

    Wraps a networkx DiGraph to provide lineage-specific traversal methods.
    Nodes are asset ID strings; edges carry a ``kind`` attribute.
    """

    _graph: nx.DiGraph = field(default_factory=nx.DiGraph)

    @classmethod
    def from_edges(cls, edges: list[Edge]) -> LineageGraph:
        """Build a LineageGraph from a list of Edge objects."""
        g = nx.DiGraph()
        for edge in edges:
            g.add_edge(edge.upstream_id, edge.downstream_id, kind=edge.kind)
        return cls(_graph=g)

    def upstream(self, asset_id: str, depth: int | None = None) -> list[str]:
        """Return all upstream ancestors of an asset.

        Args:
            asset_id: The asset to trace from.
            depth: Maximum traversal depth. None means unlimited.

        Returns:
            List of asset IDs that are upstream of asset_id.
        """
        if asset_id not in self._graph:
            return []
        # Ancestors in a directed graph = nodes that have paths TO asset_id
        # In networkx: ancestors traverses reversed edges
        ancestors = nx.ancestors(self._graph, asset_id)
        if depth is not None:
            ancestors = {
                n for n in ancestors if nx.shortest_path_length(self._graph, n, asset_id) <= depth
            }
        return sorted(ancestors)

    def downstream(self, asset_id: str, depth: int | None = None) -> list[str]:
        """Return all downstream descendants of an asset.

        Args:
            asset_id: The asset to trace from.
            depth: Maximum traversal depth. None means unlimited.

        Returns:
            List of asset IDs that are downstream of asset_id.
        """
        if asset_id not in self._graph:
            return []
        descendants = nx.descendants(self._graph, asset_id)
        if depth is not None:
            descendants = {
                n for n in descendants if nx.shortest_path_length(self._graph, asset_id, n) <= depth
            }
        return sorted(descendants)

    def has_asset(self, asset_id: str) -> bool:
        """Return True if the asset is present in the lineage graph."""
        return asset_id in self._graph

    def asset_count(self) -> int:
        """Return the number of assets (nodes) in the graph."""
        return self._graph.number_of_nodes()

    def edge_count(self) -> int:
        """Return the number of edges in the graph."""
        return self._graph.number_of_edges()


def compute_lineage(edges: list[Edge]) -> LineageGraph:
    """Build a LineageGraph from a list of dependency edges.

    Args:
        edges: All known dependency edges between assets.

    Returns:
        A LineageGraph ready for traversal queries.
    """
    return LineageGraph.from_edges(edges)
