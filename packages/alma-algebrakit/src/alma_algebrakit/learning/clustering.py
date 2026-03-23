"""Pattern clustering based on signature similarity.

Clusters patterns by signature similarity using weighted Jaccard over:
- relations
- joins
- selected columns
- filters
- group keys

This module provides SQL-agnostic pattern clustering operating purely on
algebrakit's RA primitives.
"""

from __future__ import annotations

from collections import defaultdict
from uuid import uuid4

from pydantic import BaseModel, Field

from alma_algebrakit.learning.patterns import PatternSignature
from alma_algebrakit.models.algebra import RAExpression


class ClusteringConfig(BaseModel):
    """Configuration for pattern clustering."""

    min_cluster_size: int = Field(default=2, description="Minimum patterns in a cluster")
    similarity_threshold: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Minimum Jaccard similarity for clustering",
    )
    relation_weight: float = Field(default=1.0, ge=0.0, description="Weight for relation features")
    join_weight: float = Field(default=2.0, ge=0.0, description="Weight for join features")
    predicate_weight: float = Field(
        default=1.5, ge=0.0, description="Weight for predicate features"
    )
    column_weight: float = Field(default=1.0, ge=0.0, description="Weight for column features")
    groupby_weight: float = Field(default=2.0, ge=0.0, description="Weight for group-by features")

    @classmethod
    def join_focused(cls) -> ClusteringConfig:
        """Create a join-focused clustering config.

        This configuration emphasizes join patterns over projected columns,
        which is useful for discovering reusable views/contracts.

        Returns:
            ClusteringConfig optimized for view discovery
        """
        return cls(
            similarity_threshold=0.2,
            relation_weight=2.0,
            join_weight=5.0,
            predicate_weight=2.0,
            column_weight=0.1,
            groupby_weight=1.0,
        )


class PatternCluster(BaseModel):
    """A cluster of similar patterns.

    Represents a group of RA expressions that share similar structural
    features and may be candidates for a common view.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), description="Cluster ID")
    pattern_ids: list[str] = Field(
        default_factory=list, description="IDs of patterns in this cluster"
    )
    total_weight: float = Field(default=0.0, description="Sum of pattern weights in the cluster")
    centroid_signature: PatternSignature | None = Field(
        default=None, description="Representative signature for the cluster"
    )
    common_relations: list[str] = Field(
        default_factory=list, description="Relations common to all/most patterns"
    )
    common_joins: list[str] = Field(
        default_factory=list, description="Joins common to most patterns"
    )
    common_predicates: list[str] = Field(
        default_factory=list, description="Predicates common to most patterns"
    )
    common_columns: list[str] = Field(
        default_factory=list, description="Columns common to most patterns"
    )
    feature_support: dict[str, float] = Field(
        default_factory=dict, description="Support score for each feature"
    )
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Additional metadata"
    )


class PatternInstance(BaseModel):
    """A pattern instance with associated metadata for clustering.

    Stores both the extracted signature (for fast feature-based clustering)
    and optionally the original RAExpression (for GCS-based view learning).
    """

    model_config = {"arbitrary_types_allowed": True}

    id: str = Field(default_factory=lambda: str(uuid4()), description="Pattern ID")
    signature: PatternSignature = Field(description="The pattern signature")
    expression: RAExpression | None = Field(
        default=None,
        description="Original RA expression (optional, for GCS-based learning)",
    )
    weight: float = Field(default=1.0, ge=0.0, description="Pattern weight/importance")
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Additional metadata"
    )


class PatternClusterer:
    """Clusters patterns by signature similarity.

    Uses agglomerative clustering with weighted Jaccard similarity
    to group structurally similar RA expressions.
    """

    def __init__(self, config: ClusteringConfig | None = None) -> None:
        self.config = config or ClusteringConfig()

    def cluster(self, patterns: list[PatternInstance]) -> list[PatternCluster]:
        """Cluster patterns based on signature similarity.

        Args:
            patterns: List of pattern instances to cluster

        Returns:
            List of pattern clusters
        """
        if not patterns:
            return []

        if len(patterns) == 1:
            return [self._single_pattern_cluster(patterns[0])]

        # Simple agglomerative clustering
        return self._agglomerative_cluster(patterns)

    def compute_similarity(self, sig1: PatternSignature, sig2: PatternSignature) -> float:
        """Compute weighted Jaccard similarity between two signatures.

        Args:
            sig1: First signature
            sig2: Second signature

        Returns:
            Similarity score between 0 and 1
        """
        weights = {
            "relation": self.config.relation_weight,
            "join": self.config.join_weight,
            "predicate": self.config.predicate_weight,
            "column": self.config.column_weight,
            "groupby": self.config.groupby_weight,
        }

        features1 = sig1.weighted_features(weights)
        features2 = sig2.weighted_features(weights)

        # Weighted Jaccard
        all_features = set(features1.keys()) | set(features2.keys())
        if not all_features:
            return 0.0

        intersection_weight = 0.0
        union_weight = 0.0

        for feature in all_features:
            w1 = features1.get(feature, 0.0)
            w2 = features2.get(feature, 0.0)
            intersection_weight += min(w1, w2)
            union_weight += max(w1, w2)

        if union_weight == 0:
            return 0.0

        return intersection_weight / union_weight

    def _agglomerative_cluster(self, patterns: list[PatternInstance]) -> list[PatternCluster]:
        """Simple agglomerative clustering."""
        n = len(patterns)

        # Initialize: each pattern is its own cluster
        cluster_assignments: dict[int, int] = {i: i for i in range(n)}
        active_clusters: set[int] = set(range(n))

        # Precompute similarities
        similarities: dict[tuple[int, int], float] = {}
        for i in range(n):
            for j in range(i + 1, n):
                sim = self.compute_similarity(patterns[i].signature, patterns[j].signature)
                similarities[(i, j)] = sim

        # Merge clusters until no pair exceeds threshold
        while True:
            # Find best pair to merge
            best_pair = None
            best_sim = self.config.similarity_threshold

            for (i, j), sim in similarities.items():
                ci = self._find_cluster(cluster_assignments, i)
                cj = self._find_cluster(cluster_assignments, j)
                if ci != cj and ci in active_clusters and cj in active_clusters:
                    if sim >= best_sim:
                        best_sim = sim
                        best_pair = (ci, cj)

            if best_pair is None:
                break

            # Merge clusters
            ci, cj = best_pair
            # Merge cj into ci
            for idx, _c in list(cluster_assignments.items()):
                if self._find_cluster(cluster_assignments, idx) == cj:
                    cluster_assignments[idx] = ci
            active_clusters.discard(cj)

        # Build PatternCluster objects
        cluster_members: dict[int, list[int]] = defaultdict(list)
        for idx, _cluster_id in cluster_assignments.items():
            final_cluster = self._find_cluster(cluster_assignments, idx)
            cluster_members[final_cluster].append(idx)

        result: list[PatternCluster] = []
        for _cluster_id, members in cluster_members.items():
            if len(members) < self.config.min_cluster_size:
                # Skip small clusters or add them as individual
                for idx in members:
                    result.append(self._single_pattern_cluster(patterns[idx]))
            else:
                cluster_patterns = [patterns[idx] for idx in members]
                cluster_info = self._build_cluster_info(cluster_patterns)
                result.append(cluster_info)

        return result

    def _find_cluster(self, assignments: dict[int, int], idx: int) -> int:
        """Find the root cluster for an index (union-find style)."""
        if assignments[idx] == idx:
            return idx
        # Path compression
        root = self._find_cluster(assignments, assignments[idx])
        assignments[idx] = root
        return root

    def _single_pattern_cluster(self, pattern: PatternInstance) -> PatternCluster:
        """Create a cluster for a single pattern."""
        sig = pattern.signature
        return PatternCluster(
            id=str(uuid4()),
            pattern_ids=[pattern.id],
            total_weight=pattern.weight,
            centroid_signature=sig,
            common_relations=sig.relations,
            common_joins=[j.fingerprint() for j in sig.join_edges],
            common_predicates=sig.predicates,
            common_columns=sig.projected_columns,
            feature_support=dict.fromkeys(sig.feature_set(), 1.0),
        )

    def _build_cluster_info(self, patterns: list[PatternInstance]) -> PatternCluster:
        """Build PatternCluster from a group of patterns."""
        pattern_ids = [p.id for p in patterns]
        total_weight = sum(p.weight for p in patterns)

        # Compute feature support
        feature_counts: dict[str, float] = defaultdict(float)
        for p in patterns:
            for feature in p.signature.feature_set():
                feature_counts[feature] += p.weight

        feature_support = {f: w / total_weight for f, w in feature_counts.items()}

        # Find common features (high support)
        high_support_threshold = 0.5

        common_relations = [
            f.split(":", 1)[1]
            for f, s in feature_support.items()
            if f.startswith("rel:") and s >= high_support_threshold
        ]
        common_joins = [
            f.split(":", 1)[1]
            for f, s in feature_support.items()
            if f.startswith("join:") and s >= high_support_threshold
        ]
        common_predicates = [
            f.split(":", 1)[1]
            for f, s in feature_support.items()
            if f.startswith("pred:") and s >= high_support_threshold
        ]
        common_columns = [
            f.split(":", 1)[1]
            for f, s in feature_support.items()
            if f.startswith("proj:") and s >= high_support_threshold
        ]

        # Use first pattern's signature as centroid (simplification)
        centroid = patterns[0].signature if patterns else None

        return PatternCluster(
            id=str(uuid4()),
            pattern_ids=pattern_ids,
            total_weight=total_weight,
            centroid_signature=centroid,
            common_relations=sorted(common_relations),
            common_joins=sorted(common_joins),
            common_predicates=sorted(common_predicates),
            common_columns=sorted(common_columns),
            feature_support=feature_support,
        )

    def compute_support(
        self,
        patterns: list[PatternInstance],
        feature: str,
    ) -> float:
        """Compute support for a feature across patterns.

        Support = Σ(w_i for patterns containing feature) / Σ(w_i for all patterns)
        """
        total_weight = sum(p.weight for p in patterns)
        if total_weight == 0:
            return 0.0

        supporting_weight = sum(p.weight for p in patterns if feature in p.signature.feature_set())

        return supporting_weight / total_weight
