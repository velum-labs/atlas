"""Topology-based view learning.

This module provides a view learner that focuses purely on join topology,
ignoring filter predicates. This produces maximally reusable views that
serve any query on a given join structure.

Key differences from HybridViewLearner:
- No SPJ check needed (aggregations still have underlying topology)
- No predicate handling at all
- Simpler similarity metric (Jaccard on relations + edges)
- Works with any RA expression type

All table names are normalized to fully qualified form (schema.table) using
a configurable default_schema parameter.

Usage:
    from alma_algebrakit.learning.topology_learner import TopologyViewLearner

    learner = TopologyViewLearner(default_schema="public")
    result = learner.learn_views(expressions)

    for view in result.views:
        print(f"View: {view.name}")
        print(f"  Relations: {view.relations}")
        print(f"  Joins: {view.join_edges}")
"""

from __future__ import annotations

from collections import defaultdict
from uuid import uuid4

from pydantic import BaseModel, Field

from alma_algebrakit.learning.topology import (
    TableRef,
    Topology,
    TopologyNormalization,
    TopologyResult,
    extract_topology,
    topology_gcs,
    topology_to_ra,
)
from alma_algebrakit.models.algebra import RAExpression

# =============================================================================
# Configuration
# =============================================================================


class TopologyClusteringConfig(BaseModel):
    """Configuration for topology-based clustering."""

    similarity_threshold: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Minimum topology similarity for clustering",
    )
    min_cluster_size: int = Field(default=2, ge=1, description="Minimum expressions in a cluster")
    relation_weight: float = Field(
        default=0.4, ge=0.0, description="Weight for relation similarity"
    )
    edge_weight: float = Field(default=0.6, ge=0.0, description="Weight for join edge similarity")


# =============================================================================
# Result Types
# =============================================================================


class TopologyCluster(BaseModel):
    """A cluster of expressions with similar topology."""

    model_config = {"arbitrary_types_allowed": True}

    id: str = Field(default_factory=lambda: str(uuid4()))
    expression_indices: list[int] = Field(
        default_factory=list, description="Indices of expressions in this cluster"
    )
    representative_topology: Topology | None = Field(
        default=None, description="Common topology for this cluster"
    )
    total_weight: float = Field(default=0.0)


class LearnedTopologyView(BaseModel):
    """A view learned from topology analysis.

    Unlike predicate-based views, these are purely structural -
    they capture join patterns without any filter conditions.
    """

    model_config = {"arbitrary_types_allowed": True}

    id: str = Field(description="View ID")
    name: str = Field(description="Suggested view name")
    expression: RAExpression | None = Field(default=None, description="The view RA expression")
    topology: Topology | None = Field(default=None, description="The view topology")

    # Cluster info
    cluster_id: str = Field(description="Source cluster ID")
    pattern_count: int = Field(default=0, description="Number of patterns in cluster")
    total_weight: float = Field(default=0.0, description="Total weight of patterns")

    # Topology details
    relations: list[str] = Field(default_factory=list, description="Relations in the view")
    join_edges: list[str] = Field(default_factory=list, description="Join edge fingerprints")

    # Analysis
    is_connected: bool = Field(default=True, description="Whether topology is fully connected")
    is_star: bool = Field(default=False, description="Whether topology has star schema pattern")
    hub_table: str | None = Field(default=None, description="Central hub table if star pattern")

    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Additional metadata"
    )


class TopologyLearningResult(BaseModel):
    """Result of topology-based view learning."""

    views: list[LearnedTopologyView] = Field(
        default_factory=list, description="Learned topology views"
    )
    clusters: list[TopologyCluster] = Field(default_factory=list, description="Topology clusters")
    total_expressions: int = Field(default=0)
    unique_topologies: int = Field(default=0)


# =============================================================================
# Topology View Learner
# =============================================================================


class TopologyViewLearner:
    """Learns views based purely on join topology.

    This learner:
    1. Extracts topology from each expression (relations + joins only)
    2. Clusters expressions by topology similarity
    3. Computes common topology per cluster
    4. Analyzes topology for patterns (star schema, hubs)

    All normalizations (schema, alias, self-join) are controlled via
    TopologyNormalization config.

    Unlike HybridViewLearner:
    - No SPJ restriction (aggregations have topology too)
    - No predicate handling
    - Simpler, faster computation
    - Maximum view reusability
    """

    def __init__(
        self,
        config: TopologyClusteringConfig | None = None,
        normalization: TopologyNormalization | None = None,
        *,
        default_schema: str | None = None,
    ) -> None:
        self.config = config or TopologyClusteringConfig()
        # Resolve normalization config with backwards compatibility
        if normalization is None:
            normalization = TopologyNormalization(
                default_schema=default_schema or "public",
            )
        self._normalization = normalization

    def learn_views(
        self,
        expressions: list[RAExpression],
        weights: list[float] | None = None,
    ) -> TopologyLearningResult:
        """Learn topology views from expressions.

        Args:
            expressions: List of RA expressions
            weights: Optional weights for each expression

        Returns:
            TopologyLearningResult with learned views
        """
        if not expressions:
            return TopologyLearningResult()

        if weights is None:
            weights = [1.0] * len(expressions)

        # Step 1: Extract topologies (with normalization)
        topologies = [extract_topology(e, normalization=self._normalization) for e in expressions]
        unique_topos = len({t.fingerprint() for t in topologies})

        # Step 2: Cluster by topology similarity
        clusters = self._cluster_topologies(topologies, weights)

        # Step 3: Learn views from clusters
        views: list[LearnedTopologyView] = []
        for cluster in clusters:
            view = self._learn_view_from_cluster(cluster, expressions, topologies, weights)
            if view is not None:
                views.append(view)

        return TopologyLearningResult(
            views=views,
            clusters=clusters,
            total_expressions=len(expressions),
            unique_topologies=unique_topos,
        )

    def _cluster_topologies(
        self,
        topologies: list[Topology],
        weights: list[float],
    ) -> list[TopologyCluster]:
        """Cluster topologies by similarity.

        Uses agglomerative clustering with topology similarity metric.
        """
        n = len(topologies)
        if n == 0:
            return []

        if n == 1:
            return [
                TopologyCluster(
                    expression_indices=[0],
                    representative_topology=topologies[0],
                    total_weight=weights[0],
                )
            ]

        # Initialize each topology as its own cluster
        clusters: list[set[int]] = [{i} for i in range(n)]

        # Compute pairwise similarities
        similarities: dict[tuple[int, int], float] = {}
        for i in range(n):
            for j in range(i + 1, n):
                sim = self._compute_weighted_similarity(topologies[i], topologies[j])
                similarities[(i, j)] = sim

        # Agglomerative clustering
        while len(clusters) > 1:
            # Find most similar pair of clusters
            best_sim = -1.0
            best_pair: tuple[int, int] | None = None

            for i, c1 in enumerate(clusters):
                for j, c2 in enumerate(clusters):
                    if i >= j:
                        continue

                    # Average linkage
                    total_sim = 0.0
                    count = 0
                    for idx1 in c1:
                        for idx2 in c2:
                            key = (min(idx1, idx2), max(idx1, idx2))
                            if key in similarities:
                                total_sim += similarities[key]
                                count += 1

                    if count > 0:
                        avg_sim = total_sim / count
                        if avg_sim > best_sim:
                            best_sim = avg_sim
                            best_pair = (i, j)

            if best_pair is None or best_sim < self.config.similarity_threshold:
                break

            # Merge best pair
            i, j = best_pair
            clusters[i] = clusters[i] | clusters[j]
            clusters.pop(j)

        # Convert to TopologyCluster objects
        result: list[TopologyCluster] = []
        for indices in clusters:
            if len(indices) >= self.config.min_cluster_size:
                idx_list = sorted(indices)
                # Compute common topology
                cluster_topos = [topologies[i] for i in idx_list]
                common_topo = self._compute_common_topology(cluster_topos)

                result.append(
                    TopologyCluster(
                        expression_indices=idx_list,
                        representative_topology=common_topo,
                        total_weight=sum(weights[i] for i in idx_list),
                    )
                )

        return result

    def _compute_weighted_similarity(self, t1: Topology, t2: Topology) -> float:
        """Compute weighted similarity between topologies.

        Uses physical table names for comparison, making it alias-insensitive.
        When both sets are empty (e.g., no edges), treats them as identical (1.0).
        """
        # Get physical names for comparison
        t1_physical = {r.physical for r in t1.relations}
        t2_physical = {r.physical for r in t2.relations}

        # Relation similarity (Jaccard)
        rel_intersection = len(t1_physical & t2_physical)
        rel_union = len(t1_physical | t2_physical)
        rel_sim = rel_intersection / rel_union if rel_union > 0 else 1.0

        # Edge similarity (Jaccard) - edges already compare by physical names
        # When both have no edges, they're identical (1.0)
        edge_intersection = len(t1.join_edges & t2.join_edges)
        edge_union = len(t1.join_edges | t2.join_edges)
        edge_sim = edge_intersection / edge_union if edge_union > 0 else 1.0

        return self.config.relation_weight * rel_sim + self.config.edge_weight * edge_sim

    def _compute_common_topology(self, topologies: list[Topology]) -> Topology:
        """Compute the intersection topology."""
        if not topologies:
            return Topology()

        # Intersect relations by physical name
        # Create a mapping from physical -> TableRef (use first one found)
        physical_to_ref: dict[str, TableRef] = {}
        for topo in topologies:
            for ref in topo.relations:
                if ref.physical not in physical_to_ref:
                    physical_to_ref[ref.physical] = ref

        # Find common physical names
        relation_physical_sets = [{r.physical for r in t.relations} for t in topologies]
        common_physical = set.intersection(*relation_physical_sets)

        # Get TableRefs for common physical names
        common_relations = frozenset(physical_to_ref[p] for p in common_physical)

        # Intersect edges
        edge_sets = [set(t.join_edges) for t in topologies]
        common_edges = set.intersection(*edge_sets) if edge_sets else set()

        # Filter edges to common relations
        filtered_edges = frozenset(
            e
            for e in common_edges
            if e.left.physical in common_physical and e.right.physical in common_physical
        )

        return Topology(
            relations=common_relations,
            join_edges=filtered_edges,
        )

    def _learn_view_from_cluster(
        self,
        cluster: TopologyCluster,
        expressions: list[RAExpression],
        topologies: list[Topology],
        weights: list[float],
    ) -> LearnedTopologyView | None:
        """Learn a view from a cluster."""
        if cluster.representative_topology is None or cluster.representative_topology.is_empty():
            return None

        topo = cluster.representative_topology

        # Convert to RA expression
        try:
            ra_expr = topology_to_ra(topo)
        except ValueError:
            ra_expr = None

        # Analyze topology
        is_star, hub = self._detect_star_pattern(topo)

        # Generate name
        name = self._suggest_name(topo, is_star, hub)

        return LearnedTopologyView(
            id=f"topo_view_{cluster.id[:8]}",
            name=name,
            expression=ra_expr,
            topology=topo,
            cluster_id=cluster.id,
            pattern_count=len(cluster.expression_indices),
            total_weight=cluster.total_weight,
            relations=sorted(r.physical for r in topo.relations),
            join_edges=[e.fingerprint() for e in topo.join_edges],
            is_connected=topo.is_connected(),
            is_star=is_star,
            hub_table=hub,
        )

    def _detect_star_pattern(self, topo: Topology) -> tuple[bool, str | None]:
        """Detect if topology has a star schema pattern.

        Star schema: one central table (fact) connected to multiple
        leaf tables (dimensions) that don't connect to each other.
        """
        if len(topo.relations) < 3:
            return False, None

        # Compute degree of each table (by physical name)
        degrees: dict[str, int] = defaultdict(int)
        for edge in topo.join_edges:
            degrees[edge.left.physical] += 1
            degrees[edge.right.physical] += 1

        if not degrees:
            return False, None

        # Find table with highest degree
        max_degree = max(degrees.values())
        if max_degree < 2:
            return False, None

        hub_candidates = [t for t, d in degrees.items() if d == max_degree]

        # Check if it's actually a star (hub connects to all, dimensions are leaves)
        for hub in hub_candidates:
            neighbors: set[str] = set()
            for edge in topo.join_edges:
                if edge.left.physical == hub:
                    neighbors.add(edge.right.physical)
                elif edge.right.physical == hub:
                    neighbors.add(edge.left.physical)

            # Check if all neighbors are leaves (degree 1)
            all_leaves = all(degrees.get(n, 0) == 1 for n in neighbors)
            if all_leaves and len(neighbors) >= 2:
                return True, hub

        return False, None

    def _suggest_name(self, topo: Topology, is_star: bool, hub: str | None) -> str:
        """Suggest a name for the view."""
        if is_star and hub:
            # Extract just the table name from qualified name for cleaner view names
            hub_name = hub.split(".")[-1] if "." in hub else hub
            return f"v_{hub_name}_star"

        # Get physical table names
        physical_names = sorted(r.physical for r in topo.relations)

        # Extract just table names for cleaner view names
        def short_name(qualified: str) -> str:
            return qualified.split(".")[-1] if "." in qualified else qualified

        if len(physical_names) == 1:
            return f"v_{short_name(physical_names[0])}"

        if len(physical_names) == 2:
            return f"v_{short_name(physical_names[0])}_{short_name(physical_names[1])}"

        # Multiple relations - use first + "joined"
        primary = short_name(physical_names[0])
        return f"v_{primary}_joined_{len(physical_names) - 1}"


# =============================================================================
# Convenience Functions
# =============================================================================


def learn_topology_views(
    expressions: list[RAExpression],
    weights: list[float] | None = None,
    config: TopologyClusteringConfig | None = None,
    normalization: TopologyNormalization | None = None,
    *,
    default_schema: str | None = None,
) -> TopologyLearningResult:
    """Learn topology views from expressions.

    Convenience function that creates a TopologyViewLearner and
    runs the learning process.

    Args:
        expressions: List of RA expressions
        weights: Optional weights for each expression
        config: Optional clustering configuration
        normalization: Normalization configuration (preferred)
        default_schema: Schema for unqualified names (deprecated, use normalization)

    Returns:
        TopologyLearningResult with learned views
    """
    learner = TopologyViewLearner(
        config=config,
        normalization=normalization,
        default_schema=default_schema,
    )
    return learner.learn_views(expressions, weights)


def extract_all_topologies(
    expressions: list[RAExpression],
    normalization: TopologyNormalization | None = None,
    *,
    default_schema: str | None = None,
) -> list[Topology]:
    """Extract topologies from all expressions.

    Args:
        expressions: List of RA expressions
        normalization: Normalization configuration (preferred)
        default_schema: Schema for unqualified names (deprecated, use normalization)

    Returns:
        List of topologies
    """
    # Resolve normalization once for all expressions
    if normalization is None:
        normalization = TopologyNormalization(
            default_schema=default_schema or "public",
        )
    return [extract_topology(e, normalization=normalization) for e in expressions]


def find_common_topology(
    expressions: list[RAExpression],
    normalization: TopologyNormalization | None = None,
    *,
    default_schema: str | None = None,
) -> TopologyResult:
    """Find the common topology across expressions.

    Convenience function wrapping topology_gcs.

    Args:
        expressions: List of RA expressions
        normalization: Normalization configuration (preferred)
        default_schema: Schema for unqualified names (deprecated, use normalization)

    Returns:
        TopologyResult with common topology
    """
    return topology_gcs(expressions, normalization=normalization, default_schema=default_schema)
