"""Evidence computation for pattern analysis and view derivation.

Computes deterministic evidence signals:
- Join evidence: frequency, direction, key-likeness
- Filter evidence: common predicates, invariants
- Grain evidence: group-by patterns
- Global feature support: cross-cluster feature analysis
- Join graph evidence: hub tables, view boundaries

This module provides SQL-agnostic evidence computation operating purely on
algebrakit's RA primitives.
"""

from __future__ import annotations

from collections import defaultdict

from pydantic import BaseModel, Field

from alma_algebrakit.learning.clustering import PatternCluster, PatternInstance

# =============================================================================
# Constants
# =============================================================================

# Feature prefixes
FEATURE_PREFIX_RELATION = "rel:"
FEATURE_PREFIX_JOIN = "join:"
FEATURE_PREFIX_PREDICATE = "pred:"
FEATURE_PREFIX_PROJECTION = "proj:"
FEATURE_PREFIX_GROUP_BY = "group:"
FEATURE_PREFIX_SEPARATOR = ":"

# Threshold defaults
DEFAULT_HIGH_SUPPORT_THRESHOLD = 0.7
DEFAULT_INVARIANT_THRESHOLD = 0.8
DEFAULT_GLOBAL_THRESHOLD = 0.3
DEFAULT_LOCAL_THRESHOLD = 0.7
DEFAULT_KEY_THRESHOLD = 0.7
DEFAULT_MIN_CLUSTERS = 2
DEFAULT_MIN_CLUSTERS_FOR_BASE = 3


# =============================================================================
# Feature Support Models
# =============================================================================


class FeatureDistribution(BaseModel):
    """Distribution of a feature across clusters."""

    global_support: float = Field(
        description="Weighted support across all patterns: Σ(wᵢ for patterns with f) / Σ(wᵢ)"
    )
    cluster_supports: dict[str, float] = Field(
        default_factory=dict, description="Support score per cluster: {cluster_id: support}"
    )
    pattern_ids: list[str] = Field(
        default_factory=list, description="IDs of patterns containing this feature"
    )
    cluster_coverage: float = Field(default=0.0, description="|clusters using f| / |clusters|")
    cross_cluster_score: float = Field(
        default=0.0, description="Score indicating how universally used this feature is"
    )


class GlobalFeatureSupport(BaseModel):
    """Global feature support across all patterns and clusters."""

    features: dict[str, FeatureDistribution] = Field(
        default_factory=dict, description="Feature distributions keyed by feature string"
    )
    total_patterns: int = Field(default=0)
    total_weight: float = Field(default=0.0)
    total_clusters: int = Field(default=0)

    def get_high_support_features(
        self,
        threshold: float = DEFAULT_HIGH_SUPPORT_THRESHOLD,
        min_clusters: int = DEFAULT_MIN_CLUSTERS,
    ) -> list[tuple[str, FeatureDistribution]]:
        """Get features with high global support used by multiple clusters."""
        return [
            (f, dist)
            for f, dist in self.features.items()
            if dist.global_support >= threshold and len(dist.cluster_supports) >= min_clusters
        ]

    def get_cluster_specific_features(
        self,
        cluster_id: str,
        global_threshold: float = DEFAULT_GLOBAL_THRESHOLD,
    ) -> list[tuple[str, FeatureDistribution]]:
        """Get features that are strong in a cluster but weak globally."""
        return [
            (f, dist)
            for f, dist in self.features.items()
            if dist.global_support < global_threshold
            and dist.cluster_supports.get(cluster_id, 0) >= DEFAULT_LOCAL_THRESHOLD
        ]


# =============================================================================
# Join Graph Models
# =============================================================================


class JoinGraphNode(BaseModel):
    """A node (table) in the join graph."""

    table_name: str = Field(description="Table name")
    degree: int = Field(default=0, description="Number of edges")
    weighted_degree: float = Field(default=0.0, description="Sum of edge weights")
    neighbors: list[str] = Field(default_factory=list, description="Connected tables")
    is_hub: bool = Field(default=False, description="Whether this is a hub table")
    hub_score: float = Field(default=0.0, description="Hub score (centrality)")


class JoinGraphEdge(BaseModel):
    """An edge (join) in the join graph."""

    left_table: str = Field(description="Left table")
    right_table: str = Field(description="Right table")
    left_column: str = Field(description="Left join column")
    right_column: str = Field(description="Right join column")
    weight: float = Field(default=1.0, description="Sum of pattern weights using this join")
    pattern_count: int = Field(default=0, description="Number of patterns using this join")

    def fingerprint(self) -> str:
        """Canonical fingerprint for the edge."""
        if (self.left_table, self.left_column) > (self.right_table, self.right_column):
            return f"{self.right_table}.{self.right_column}={self.left_table}.{self.left_column}"
        return f"{self.left_table}.{self.left_column}={self.right_table}.{self.right_column}"


class JoinChain(BaseModel):
    """A chain of joins (A→B→C pattern)."""

    tables: list[str] = Field(description="Ordered list of tables in the chain")
    edges: list[str] = Field(description="Edge fingerprints in the chain")
    total_weight: float = Field(default=0.0, description="Minimum edge weight in chain")


class ViewBoundary(BaseModel):
    """Suggested boundary for a view based on join graph analysis."""

    core_tables: list[str] = Field(description="Tables that should be in the view")
    hub_table: str | None = Field(default=None, description="Central hub table if any")
    boundary_joins: list[str] = Field(
        default_factory=list,
        description="Join edges at the boundary (for connecting to other views)",
    )
    internal_joins: list[str] = Field(
        default_factory=list, description="Join edges inside the view"
    )
    score: float = Field(default=0.0, description="Quality score for this boundary")


class JoinGraphEvidence(BaseModel):
    """Evidence from join graph analysis."""

    nodes: dict[str, JoinGraphNode] = Field(
        default_factory=dict, description="Nodes (tables) in the join graph"
    )
    edges: list[JoinGraphEdge] = Field(
        default_factory=list, description="Edges (joins) in the graph"
    )
    hub_tables: list[str] = Field(
        default_factory=list, description="Tables identified as hubs (high degree)"
    )
    join_chains: list[JoinChain] = Field(default_factory=list, description="Identified join chains")
    suggested_boundaries: list[ViewBoundary] = Field(
        default_factory=list, description="Suggested view boundaries"
    )
    is_star_schema: bool = Field(
        default=False, description="Whether the graph resembles a star schema"
    )
    star_center: str | None = Field(default=None, description="Center table if star schema")


# =============================================================================
# Feature Support Types
# =============================================================================


class FeatureSupport(BaseModel):
    """Support statistics for a feature."""

    feature: str = Field(description="The feature string (e.g., 'rel:orders')")
    global_support: float = Field(default=0.0, description="Support across all patterns")
    cluster_support: float = Field(default=0.0, description="Support within a specific cluster")
    pattern_count: int = Field(default=0, description="Number of patterns containing this feature")
    total_weight: float = Field(
        default=0.0, description="Total weight of patterns with this feature"
    )


# =============================================================================
# Evidence Computer
# =============================================================================


class EvidenceComputer:
    """Computes evidence signals for pattern analysis and view derivation.

    This is the SQL-agnostic core of evidence computation, operating purely
    on algebrakit primitives.
    """

    def compute_cluster_feature_support(
        self,
        cluster: PatternCluster,
        patterns: list[PatternInstance],
    ) -> dict[str, float]:
        """Compute support for each feature in a cluster.

        Support_C(x) = Σ(w_i for patterns containing x) / Σ(w_i for all patterns in cluster)
        """
        cluster_patterns = [p for p in patterns if p.id in cluster.pattern_ids]
        total_weight = sum(p.weight for p in cluster_patterns)

        if total_weight == 0:
            return {}

        feature_weight: dict[str, float] = defaultdict(float)
        for p in cluster_patterns:
            for feature in p.signature.feature_set():
                feature_weight[feature] += p.weight

        return {f: w / total_weight for f, w in feature_weight.items()}

    def classify_predicates(
        self,
        cluster: PatternCluster,
        patterns: list[PatternInstance],
        invariant_threshold: float = DEFAULT_INVARIANT_THRESHOLD,
    ) -> tuple[list[str], list[str]]:
        """Classify predicates as invariant or optional.

        Invariant predicates: support >= threshold
        Optional predicates: support < threshold

        Returns:
            (invariant_predicates, optional_predicates)
        """
        support = self.compute_cluster_feature_support(cluster, patterns)

        invariant: list[str] = []
        optional: list[str] = []

        for feature, score in support.items():
            if feature.startswith(FEATURE_PREFIX_PREDICATE):
                pred = feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1]
                if score >= invariant_threshold:
                    invariant.append(pred)
                else:
                    optional.append(pred)

        return invariant, optional

    def identify_candidate_keys(
        self,
        cluster: PatternCluster,
        patterns: list[PatternInstance],
        key_threshold: float = DEFAULT_KEY_THRESHOLD,
    ) -> list[list[str]]:
        """Identify candidate key columns for a cluster.

        Candidate keys are columns that:
        1. Frequently appear in JOIN conditions (as the "target" side)
        2. Frequently appear in GROUP BY
        3. Have high uniqueness evidence

        Returns list of candidate key combinations, ordered by strength.
        """
        support = self.compute_cluster_feature_support(cluster, patterns)

        # Score each column for key-likeness
        key_scores: dict[str, float] = defaultdict(float)

        # Group-by evidence
        for feature, score in support.items():
            if feature.startswith(FEATURE_PREFIX_GROUP_BY):
                col = feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1]
                key_scores[col] += score * 2.0  # Higher weight

        # Join evidence
        for feature, score in support.items():
            if feature.startswith(FEATURE_PREFIX_JOIN):
                # Parse join fingerprint: "table.col=table.col"
                join_str = feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1]
                if "=" in join_str:
                    parts = join_str.split("=")
                    for part in parts:
                        key_scores[part.strip()] += score * 1.5

        # Filter to high-scoring candidates
        candidates = [col for col, score in key_scores.items() if score >= key_threshold]

        # Return as single-column keys for simplicity
        return [[col] for col in sorted(candidates, key=lambda c: -key_scores[c])]

    def compute_join_evidence(
        self,
        patterns: list[PatternInstance],
    ) -> dict[str, FeatureSupport]:
        """Compute join evidence from patterns.

        Returns evidence for columns frequently used in joins.
        """
        join_counts: dict[str, int] = defaultdict(int)
        join_weight: dict[str, float] = defaultdict(float)
        total_weight = sum(p.weight for p in patterns)

        for pattern in patterns:
            for edge in pattern.signature.join_edges:
                left_col = f"{edge.left_table}.{edge.left_column}"
                right_col = f"{edge.right_table}.{edge.right_column}"

                join_counts[left_col] += 1
                join_counts[right_col] += 1
                join_weight[left_col] += pattern.weight
                join_weight[right_col] += pattern.weight

        evidence: dict[str, FeatureSupport] = {}
        for col, count in join_counts.items():
            support = join_weight[col] / max(total_weight, 1)
            evidence[col] = FeatureSupport(
                feature=f"join:{col}",
                global_support=support,
                pattern_count=count,
                total_weight=join_weight[col],
            )

        return evidence

    def compute_filter_evidence(
        self,
        patterns: list[PatternInstance],
    ) -> dict[str, FeatureSupport]:
        """Compute filter evidence from patterns.

        Returns evidence for columns frequently used in predicates.
        """
        filter_counts: dict[str, int] = defaultdict(int)
        filter_weight: dict[str, float] = defaultdict(float)
        total_weight = sum(p.weight for p in patterns)

        for pattern in patterns:
            for pred in pattern.signature.predicates:
                # Extract column from predicate fingerprint
                parts = pred.split(" ", 1)
                if parts:
                    col = parts[0]
                    filter_counts[col] += 1
                    filter_weight[col] += pattern.weight

        evidence: dict[str, FeatureSupport] = {}
        for col, count in filter_counts.items():
            support = filter_weight[col] / max(total_weight, 1)
            evidence[col] = FeatureSupport(
                feature=f"filter:{col}",
                global_support=support,
                pattern_count=count,
                total_weight=filter_weight[col],
            )

        return evidence

    def compute_grain_evidence(
        self,
        patterns: list[PatternInstance],
    ) -> dict[str, FeatureSupport]:
        """Compute grain evidence from GROUP BY patterns.

        Returns evidence for columns frequently used as group-by keys.
        """
        groupby_counts: dict[str, int] = defaultdict(int)
        groupby_weight: dict[str, float] = defaultdict(float)
        total_weight = sum(p.weight for p in patterns)

        for pattern in patterns:
            for col in pattern.signature.group_by_columns:
                groupby_counts[col] += 1
                groupby_weight[col] += pattern.weight

        evidence: dict[str, FeatureSupport] = {}
        for col, count in groupby_counts.items():
            support = groupby_weight[col] / max(total_weight, 1)
            evidence[col] = FeatureSupport(
                feature=f"grain:{col}",
                global_support=support,
                pattern_count=count,
                total_weight=groupby_weight[col],
            )

        return evidence


# =============================================================================
# Global Evidence Computer
# =============================================================================


class GlobalEvidenceComputer:
    """Computes evidence across all patterns and clusters.

    This extends the cluster-local analysis to enable:
    - Global feature support computation
    - Cross-cluster feature distribution
    - Identification of base vs specialized view opportunities
    """

    def __init__(self) -> None:
        self.local_computer = EvidenceComputer()

    def compute_global_feature_support(
        self,
        patterns: list[PatternInstance],
        clusters: list[PatternCluster],
    ) -> GlobalFeatureSupport:
        """Compute feature support across all clusters.

        For each feature f:
        - global_support[f] = Σ(wᵢ for patterns containing f) / Σ(wᵢ for all patterns)
        - cluster_coverage[f] = |clusters using f| / |clusters|
        - cross_cluster_score[f] = weighted score of cross-cluster usage
        """
        total_weight = sum(p.weight for p in patterns)
        total_clusters = len(clusters)

        if total_weight == 0 or total_clusters == 0:
            return GlobalFeatureSupport(
                total_patterns=len(patterns),
                total_weight=total_weight,
                total_clusters=total_clusters,
            )

        # Track feature distributions
        feature_pattern_weight: dict[str, float] = defaultdict(float)
        feature_pattern_ids: dict[str, list[str]] = defaultdict(list)
        feature_cluster_supports: dict[str, dict[str, float]] = defaultdict(dict)

        # First pass: compute cluster-local supports
        for cluster in clusters:
            cluster_support = self.local_computer.compute_cluster_feature_support(cluster, patterns)
            for feature, support in cluster_support.items():
                feature_cluster_supports[feature][cluster.id] = support

        # Second pass: compute global support from patterns
        for pattern in patterns:
            for feature in pattern.signature.feature_set():
                feature_pattern_weight[feature] += pattern.weight
                feature_pattern_ids[feature].append(pattern.id)

        # Build feature distributions
        features: dict[str, FeatureDistribution] = {}
        for feature in set(feature_pattern_weight.keys()) | set(feature_cluster_supports.keys()):
            global_support = feature_pattern_weight.get(feature, 0.0) / total_weight
            cluster_supports = feature_cluster_supports.get(feature, {})
            cluster_coverage = len(cluster_supports) / total_clusters

            # Cross-cluster score: high if feature is used consistently across clusters
            if cluster_supports:
                cluster_weights = {
                    c.id: c.total_weight for c in clusters if c.id in cluster_supports
                }
                total_cluster_weight = sum(cluster_weights.values())
                if total_cluster_weight > 0:
                    cross_cluster_score = (
                        sum(
                            cluster_supports[cid] * cluster_weights[cid] for cid in cluster_supports
                        )
                        / total_cluster_weight
                    )
                else:
                    cross_cluster_score = 0.0
            else:
                cross_cluster_score = 0.0

            features[feature] = FeatureDistribution(
                global_support=global_support,
                cluster_supports=cluster_supports,
                pattern_ids=feature_pattern_ids.get(feature, []),
                cluster_coverage=cluster_coverage,
                cross_cluster_score=cross_cluster_score,
            )

        return GlobalFeatureSupport(
            features=features,
            total_patterns=len(patterns),
            total_weight=total_weight,
            total_clusters=total_clusters,
        )

    def identify_base_view_features(
        self,
        global_support: GlobalFeatureSupport,
        support_threshold: float = DEFAULT_HIGH_SUPPORT_THRESHOLD,
        min_clusters: int = DEFAULT_MIN_CLUSTERS_FOR_BASE,
    ) -> dict[str, list[str]]:
        """Identify features that should go into base views.

        Base view features have:
        - High global support (>= threshold)
        - Used by multiple clusters (>= min_clusters)

        Returns:
            Dictionary with 'relations', 'joins', 'predicates', 'attributes'
        """
        base_features: dict[str, list[str]] = {
            "relations": [],
            "joins": [],
            "predicates": [],
            "attributes": [],
        }

        high_support = global_support.get_high_support_features(
            threshold=support_threshold,
            min_clusters=min_clusters,
        )

        for feature, _dist in high_support:
            if feature.startswith(FEATURE_PREFIX_RELATION):
                base_features["relations"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])
            elif feature.startswith(FEATURE_PREFIX_JOIN):
                base_features["joins"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])
            elif feature.startswith(FEATURE_PREFIX_PREDICATE):
                base_features["predicates"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])
            elif feature.startswith(FEATURE_PREFIX_PROJECTION):
                base_features["attributes"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])

        return base_features

    def identify_specialized_features(
        self,
        global_support: GlobalFeatureSupport,
        cluster_id: str,
        global_threshold: float = DEFAULT_GLOBAL_THRESHOLD,
        local_threshold: float = DEFAULT_LOCAL_THRESHOLD,
    ) -> dict[str, list[str]]:
        """Identify features unique to a cluster for specialized views.

        Specialized features have:
        - Low global support (< global_threshold)
        - High local support in the cluster (>= local_threshold)

        Returns:
            Dictionary with 'relations', 'joins', 'predicates', 'attributes'
        """
        specialized: dict[str, list[str]] = {
            "relations": [],
            "joins": [],
            "predicates": [],
            "attributes": [],
        }

        cluster_specific = global_support.get_cluster_specific_features(
            cluster_id=cluster_id,
            global_threshold=global_threshold,
        )

        for feature, dist in cluster_specific:
            local_support = dist.cluster_supports.get(cluster_id, 0)
            if local_support < local_threshold:
                continue

            if feature.startswith(FEATURE_PREFIX_RELATION):
                specialized["relations"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])
            elif feature.startswith(FEATURE_PREFIX_JOIN):
                specialized["joins"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])
            elif feature.startswith(FEATURE_PREFIX_PREDICATE):
                specialized["predicates"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])
            elif feature.startswith(FEATURE_PREFIX_PROJECTION):
                specialized["attributes"].append(feature.split(FEATURE_PREFIX_SEPARATOR, 1)[1])

        return specialized

    def compute_feature_overlap(
        self,
        global_support: GlobalFeatureSupport,
        cluster_ids: list[str],
    ) -> dict[str, float]:
        """Compute feature overlap between clusters.

        For each feature, compute how many of the given clusters use it.

        Returns:
            Dictionary of feature -> overlap_ratio (0 to 1)
        """
        if not cluster_ids:
            return {}

        overlap: dict[str, float] = {}
        for feature, dist in global_support.features.items():
            count = sum(1 for cid in cluster_ids if cid in dist.cluster_supports)
            overlap[feature] = count / len(cluster_ids)

        return overlap


# =============================================================================
# Join Graph Analyzer
# =============================================================================


class JoinGraphAnalyzer:
    """Analyzes the join graph across patterns to identify optimal view boundaries.

    The join graph represents:
    - Nodes: Tables used in patterns
    - Edges: Join relationships between tables (weighted by pattern usage)

    Analysis identifies:
    - Hub tables (high degree/centrality)
    - Star schemas (single central table)
    - Join chains (A→B→C patterns)
    - View boundaries (natural groupings)
    """

    def __init__(
        self,
        hub_threshold: float = 0.3,
        min_edge_weight: float = 1.0,
    ) -> None:
        """Initialize the analyzer.

        Args:
            hub_threshold: Fraction of total degree above which a node is a hub
            min_edge_weight: Minimum edge weight to consider
        """
        self.hub_threshold = hub_threshold
        self.min_edge_weight = min_edge_weight

    def analyze(self, patterns: list[PatternInstance]) -> JoinGraphEvidence:
        """Analyze the join graph from patterns.

        Args:
            patterns: List of patterns to analyze

        Returns:
            JoinGraphEvidence with graph structure and analysis results
        """
        # Build the graph
        nodes: dict[str, JoinGraphNode] = {}
        edge_map: dict[str, JoinGraphEdge] = {}  # fingerprint -> edge

        for pattern in patterns:
            # Add nodes for relations
            for rel in pattern.signature.relations:
                if rel not in nodes:
                    nodes[rel] = JoinGraphNode(table_name=rel)

            # Add/update edges for joins
            for join_edge in pattern.signature.join_edges:
                fp = join_edge.fingerprint()
                if fp not in edge_map:
                    edge_map[fp] = JoinGraphEdge(
                        left_table=join_edge.left_table,
                        right_table=join_edge.right_table,
                        left_column=join_edge.left_column,
                        right_column=join_edge.right_column,
                        weight=0.0,
                        pattern_count=0,
                    )
                edge_map[fp].weight += pattern.weight
                edge_map[fp].pattern_count += 1

        edges = list(edge_map.values())

        # Compute node degrees
        for edge in edges:
            if edge.weight < self.min_edge_weight:
                continue

            for table in [edge.left_table, edge.right_table]:
                if table in nodes:
                    nodes[table].degree += 1
                    nodes[table].weighted_degree += edge.weight

            # Track neighbors
            if edge.left_table in nodes:
                if edge.right_table not in nodes[edge.left_table].neighbors:
                    nodes[edge.left_table].neighbors.append(edge.right_table)
            if edge.right_table in nodes:
                if edge.left_table not in nodes[edge.right_table].neighbors:
                    nodes[edge.right_table].neighbors.append(edge.left_table)

        # Identify hubs
        if nodes:
            total_degree = sum(n.weighted_degree for n in nodes.values())
            if total_degree > 0:
                for node in nodes.values():
                    node.hub_score = node.weighted_degree / total_degree
                    node.is_hub = node.hub_score >= self.hub_threshold

        hub_tables = [n.table_name for n in nodes.values() if n.is_hub]

        # Detect star schema
        is_star_schema, star_center = self._detect_star_schema(nodes, edges)

        # Find join chains
        join_chains = self._find_join_chains(nodes, edges)

        # Suggest view boundaries
        boundaries = self._suggest_boundaries(nodes, edges, hub_tables)

        return JoinGraphEvidence(
            nodes=nodes,
            edges=edges,
            hub_tables=hub_tables,
            join_chains=join_chains,
            suggested_boundaries=boundaries,
            is_star_schema=is_star_schema,
            star_center=star_center,
        )

    def _detect_star_schema(
        self,
        nodes: dict[str, JoinGraphNode],
        edges: list[JoinGraphEdge],
    ) -> tuple[bool, str | None]:
        """Detect if the graph resembles a star schema."""
        if len(nodes) < 3:
            return False, None

        # Find candidate center (highest degree)
        center_candidates = sorted(
            nodes.values(),
            key=lambda n: n.weighted_degree,
            reverse=True,
        )

        if not center_candidates:
            return False, None

        center = center_candidates[0]

        # Check if other nodes only connect to center
        non_center_nodes = [n for n in nodes.values() if n.table_name != center.table_name]

        if not non_center_nodes:
            return False, None

        # For star schema: at least 70% of non-center nodes should only connect to center
        star_count = sum(
            1
            for n in non_center_nodes
            if len(n.neighbors) == 1 and center.table_name in n.neighbors
        )

        star_ratio = star_count / len(non_center_nodes)

        if star_ratio >= 0.7 and center.degree >= 3:
            return True, center.table_name

        return False, None

    def _find_join_chains(
        self,
        nodes: dict[str, JoinGraphNode],
        edges: list[JoinGraphEdge],
    ) -> list[JoinChain]:
        """Find join chains (linear paths through the graph)."""
        chains: list[JoinChain] = []

        # Build adjacency for efficient traversal
        adj: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for edge in edges:
            if edge.weight < self.min_edge_weight:
                continue
            adj[edge.left_table].append((edge.right_table, edge.fingerprint()))
            adj[edge.right_table].append((edge.left_table, edge.fingerprint()))

        # Find chains starting from nodes with degree 1 or 2
        visited_edges: set[str] = set()

        for start_node in nodes.values():
            if start_node.degree not in [1, 2]:
                continue

            # Try to build a chain from this node
            for _neighbor, edge_fp in adj[start_node.table_name]:
                if edge_fp in visited_edges:
                    continue

                chain_tables = [start_node.table_name]
                chain_edges: list[str] = []
                current = start_node.table_name

                while True:
                    # Find unvisited edge to continue
                    next_hop = None
                    for n, fp in adj[current]:
                        if fp not in visited_edges and n not in chain_tables:
                            next_hop = (n, fp)
                            break

                    if not next_hop:
                        break

                    next_table, next_edge = next_hop
                    chain_tables.append(next_table)
                    chain_edges.append(next_edge)
                    visited_edges.add(next_edge)
                    current = next_table

                    # Stop at hub or if path branches
                    if nodes[current].is_hub or nodes[current].degree > 2:
                        break

                if len(chain_tables) >= 3:
                    min_weight = (
                        min(e.weight for e in edges if e.fingerprint() in chain_edges)
                        if chain_edges
                        else 0.0
                    )

                    chains.append(
                        JoinChain(
                            tables=chain_tables,
                            edges=chain_edges,
                            total_weight=min_weight,
                        )
                    )

        return chains

    def _suggest_boundaries(
        self,
        nodes: dict[str, JoinGraphNode],
        edges: list[JoinGraphEdge],
        hub_tables: list[str],
    ) -> list[ViewBoundary]:
        """Suggest view boundaries based on graph structure."""
        boundaries: list[ViewBoundary] = []

        if not hub_tables:
            # No hubs: suggest one view with all tables
            all_tables = list(nodes.keys())
            if all_tables:
                boundaries.append(
                    ViewBoundary(
                        core_tables=all_tables,
                        hub_table=None,
                        internal_joins=[e.fingerprint() for e in edges],
                        boundary_joins=[],
                        score=1.0,
                    )
                )
            return boundaries

        # Build adjacency for BFS
        adj: dict[str, set[str]] = defaultdict(set)
        for edge in edges:
            if edge.weight >= self.min_edge_weight:
                adj[edge.left_table].add(edge.right_table)
                adj[edge.right_table].add(edge.left_table)

        # Assign each non-hub table to nearest hub (BFS)
        table_to_hub: dict[str, str] = {}

        for hub in hub_tables:
            table_to_hub[hub] = hub

        # BFS from each hub
        for hub in hub_tables:
            queue = [hub]
            visited = {hub}

            while queue:
                current = queue.pop(0)
                for neighbor in adj[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        if neighbor not in table_to_hub:
                            table_to_hub[neighbor] = hub
                            queue.append(neighbor)

        # Group tables by hub
        hub_groups: dict[str, list[str]] = defaultdict(list)
        for table, hub in table_to_hub.items():
            hub_groups[hub].append(table)

        # Create boundaries
        for hub, tables in hub_groups.items():
            internal_joins: list[str] = []
            boundary_joins: list[str] = []

            table_set = set(tables)
            for edge in edges:
                if edge.weight < self.min_edge_weight:
                    continue

                left_in = edge.left_table in table_set
                right_in = edge.right_table in table_set

                if left_in and right_in:
                    internal_joins.append(edge.fingerprint())
                elif left_in or right_in:
                    boundary_joins.append(edge.fingerprint())

            # Score based on internal cohesion
            total_edges = len(internal_joins) + len(boundary_joins)
            score = len(internal_joins) / total_edges if total_edges > 0 else 0.0

            boundaries.append(
                ViewBoundary(
                    core_tables=tables,
                    hub_table=hub,
                    internal_joins=internal_joins,
                    boundary_joins=boundary_joins,
                    score=score,
                )
            )

        # Sort by score
        boundaries.sort(key=lambda b: -b.score)

        return boundaries

    def compute_table_centrality(
        self,
        evidence: JoinGraphEvidence,
    ) -> dict[str, float]:
        """Compute centrality scores for each table."""
        total = sum(n.weighted_degree for n in evidence.nodes.values())
        if total == 0:
            return {}

        return {name: node.weighted_degree / total for name, node in evidence.nodes.items()}

    def detect_star_schema_detailed(
        self,
        evidence: JoinGraphEvidence,
    ) -> StarSchemaResult:
        """Detect star schema with detailed analysis.

        Provides comprehensive star schema detection with:
        - Confidence score
        - Fact table identification
        - Dimension table classification
        - Join relationship mapping

        Args:
            evidence: JoinGraphEvidence from analyze()

        Returns:
            StarSchemaResult with detailed analysis
        """
        nodes = evidence.nodes
        edges = evidence.edges

        if len(nodes) < 3:
            return StarSchemaResult(
                is_star=False,
                confidence=0.0,
                explanation="Too few tables for star schema",
            )

        # Find candidate fact table (highest weighted degree)
        sorted_nodes = sorted(
            nodes.values(),
            key=lambda n: (n.weighted_degree, n.degree),
            reverse=True,
        )

        if not sorted_nodes:
            return StarSchemaResult(
                is_star=False,
                confidence=0.0,
                explanation="No nodes in graph",
            )

        candidate_fact = sorted_nodes[0]

        # Identify dimension candidates (connect only to fact)
        dimension_candidates: list[str] = []
        non_dimension_neighbors: list[str] = []

        for node in nodes.values():
            if node.table_name == candidate_fact.table_name:
                continue

            neighbors = set(node.neighbors)
            if neighbors == {candidate_fact.table_name}:
                dimension_candidates.append(node.table_name)
            elif candidate_fact.table_name in neighbors:
                non_dimension_neighbors.append(node.table_name)

        # Calculate confidence
        total_neighbors = len(candidate_fact.neighbors)
        if total_neighbors == 0:
            return StarSchemaResult(
                is_star=False,
                confidence=0.0,
                explanation="Candidate fact has no neighbors",
            )

        dimension_ratio = len(dimension_candidates) / total_neighbors
        min_dimensions = 2

        # Star schema criteria:
        # 1. Fact table has degree >= 3
        # 2. At least 70% of fact's neighbors are pure dimensions
        # 3. At least 2 dimension tables
        is_star = (
            candidate_fact.degree >= min_dimensions + 1
            and dimension_ratio >= 0.7
            and len(dimension_candidates) >= min_dimensions
        )

        confidence = (
            min(1.0, dimension_ratio * (len(dimension_candidates) / min_dimensions))
            if is_star
            else dimension_ratio * 0.5
        )

        # Map join relationships
        fact_to_dim_joins: dict[str, list[str]] = defaultdict(list)
        for edge in edges:
            if edge.left_table == candidate_fact.table_name:
                fact_to_dim_joins[edge.right_table].append(
                    f"{edge.left_column}={edge.right_column}"
                )
            elif edge.right_table == candidate_fact.table_name:
                fact_to_dim_joins[edge.left_table].append(f"{edge.right_column}={edge.left_column}")

        explanation = (
            f"Fact: {candidate_fact.table_name} (degree={candidate_fact.degree}), "
            f"Dimensions: {len(dimension_candidates)}, "
            f"Non-pure neighbors: {len(non_dimension_neighbors)}"
        )

        return StarSchemaResult(
            is_star=is_star,
            confidence=confidence,
            fact_table=candidate_fact.table_name if is_star else None,
            dimension_tables=dimension_candidates if is_star else [],
            non_dimension_tables=non_dimension_neighbors,
            fact_to_dimension_joins=dict(fact_to_dim_joins) if is_star else {},
            explanation=explanation,
        )

    def find_canonical_paths(
        self,
        evidence: JoinGraphEvidence,
        source: str | None = None,
        target: str | None = None,
        max_length: int = 5,
    ) -> list[JoinChain]:
        """Find canonical join paths through the graph.

        If source and target are specified, finds paths between them.
        Otherwise, finds all significant paths.

        Args:
            evidence: JoinGraphEvidence from analyze()
            source: Optional source table
            target: Optional target table
            max_length: Maximum path length

        Returns:
            List of JoinChain representing canonical paths
        """
        nodes = evidence.nodes
        edges = evidence.edges

        # Build adjacency
        adj: dict[str, list[tuple[str, JoinGraphEdge]]] = defaultdict(list)
        for edge in edges:
            if edge.weight < self.min_edge_weight:
                continue
            adj[edge.left_table].append((edge.right_table, edge))
            adj[edge.right_table].append((edge.left_table, edge))

        paths: list[JoinChain] = []

        if source and target:
            # BFS for shortest path between source and target
            if source not in nodes or target not in nodes:
                return []

            # BFS
            queue: list[tuple[str, list[str], list[JoinGraphEdge]]] = [(source, [source], [])]
            visited: set[str] = {source}

            while queue:
                current, path, path_edges = queue.pop(0)

                if len(path) > max_length:
                    continue

                if current == target:
                    paths.append(
                        JoinChain(
                            tables=path,
                            edges=[e.fingerprint() for e in path_edges],
                            total_weight=min(e.weight for e in path_edges) if path_edges else 0.0,
                        )
                    )
                    continue

                for neighbor, edge in adj[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, path + [neighbor], path_edges + [edge]))

        else:
            # Find all significant paths (between hubs, or long chains)
            hub_tables = [n.table_name for n in nodes.values() if n.is_hub]

            # Paths between hubs
            for i, hub1 in enumerate(hub_tables):
                for hub2 in hub_tables[i + 1 :]:
                    hub_paths = self.find_canonical_paths(evidence, hub1, hub2, max_length)
                    paths.extend(hub_paths)

            # Also include the chains already found in analyze()
            paths.extend(evidence.join_chains)

        # Sort by weight (higher is better)
        paths.sort(key=lambda p: -p.total_weight)

        return paths

    def suggest_topology_views(
        self,
        evidence: JoinGraphEvidence,
    ) -> list[TopologySuggestion]:
        """Suggest views based purely on graph structure.

        Suggests views for:
        - Hub tables + their direct neighbors
        - Star schema patterns
        - Maximal connected subgraphs around hubs
        - Frequently co-occurring table pairs

        Args:
            evidence: JoinGraphEvidence from analyze()

        Returns:
            List of TopologySuggestion with suggested views
        """
        suggestions: list[TopologySuggestion] = []
        nodes = evidence.nodes
        edges = evidence.edges

        # 1. Star schema view (if detected)
        star_result = self.detect_star_schema_detailed(evidence)
        if star_result.is_star and star_result.fact_table:
            all_star_tables = [star_result.fact_table] + star_result.dimension_tables
            star_joins = [
                e.fingerprint()
                for e in edges
                if e.left_table in all_star_tables and e.right_table in all_star_tables
            ]
            suggestions.append(
                TopologySuggestion(
                    name=f"star_{star_result.fact_table}",
                    tables=all_star_tables,
                    joins=star_joins,
                    pattern_type="star_schema",
                    hub_table=star_result.fact_table,
                    score=star_result.confidence,
                    explanation=f"Star schema with {star_result.fact_table} as fact table",
                )
            )

        # 2. Hub + neighbors views
        for hub_name in evidence.hub_tables:
            hub = nodes[hub_name]
            hub_tables = [hub_name] + hub.neighbors
            hub_joins = [
                e.fingerprint()
                for e in edges
                if e.left_table in hub_tables and e.right_table in hub_tables
            ]

            # Skip if same as star schema
            if star_result.is_star and set(hub_tables) == set(
                [star_result.fact_table] + star_result.dimension_tables
            ):
                continue

            suggestions.append(
                TopologySuggestion(
                    name=f"hub_{hub_name}",
                    tables=hub_tables,
                    joins=hub_joins,
                    pattern_type="hub_neighbors",
                    hub_table=hub_name,
                    score=hub.hub_score,
                    explanation=f"Hub {hub_name} with {len(hub.neighbors)} neighbors",
                )
            )

        # 3. High-weight edge pairs (frequently co-occurring)
        sorted_edges = sorted(edges, key=lambda e: -e.weight)
        for edge in sorted_edges[:5]:  # Top 5 edges
            if edge.weight < self.min_edge_weight * 2:
                continue

            pair_tables = [edge.left_table, edge.right_table]
            suggestions.append(
                TopologySuggestion(
                    name=f"pair_{edge.left_table}_{edge.right_table}",
                    tables=pair_tables,
                    joins=[edge.fingerprint()],
                    pattern_type="high_frequency_pair",
                    hub_table=None,
                    score=edge.weight / max(e.weight for e in edges),
                    explanation=f"High-frequency join (weight={edge.weight:.1f})",
                )
            )

        # Sort by score
        suggestions.sort(key=lambda s: -s.score)

        return suggestions


# =============================================================================
# Additional Result Types for Star Schema
# =============================================================================


class StarSchemaResult(BaseModel):
    """Result of detailed star schema detection."""

    is_star: bool = Field(description="Whether a star schema was detected")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Confidence score")
    fact_table: str | None = Field(default=None, description="Central fact table")
    dimension_tables: list[str] = Field(
        default_factory=list, description="Dimension tables (pure leaves)"
    )
    non_dimension_tables: list[str] = Field(
        default_factory=list,
        description="Tables connected to fact but also to other tables",
    )
    fact_to_dimension_joins: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Join columns from fact to each dimension",
    )
    explanation: str = Field(default="")


class TopologySuggestion(BaseModel):
    """A suggested topology view from graph analysis."""

    name: str = Field(description="Suggested view name")
    tables: list[str] = Field(description="Tables in the view")
    joins: list[str] = Field(description="Join edge fingerprints")
    pattern_type: str = Field(
        description="Type: star_schema, hub_neighbors, high_frequency_pair, chain"
    )
    hub_table: str | None = Field(default=None, description="Hub table if applicable")
    score: float = Field(default=0.0, description="Quality score")
    explanation: str = Field(default="")
