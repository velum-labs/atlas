"""Hybrid view learning combining feature-based clustering with GCS verification.

This module provides a principled approach to view learning that:
1. Uses fast feature-based clustering to group similar queries
2. Computes the Greatest Common Subexpression (GCS) within each cluster
3. Verifies that the GCS is contained in all cluster members
4. Falls back to feature-based derivation for non-SPJ queries

The hybrid approach combines the efficiency of feature-based methods with
the correctness guarantees of algebraic GCS computation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from alma_algebrakit.learning.clustering import (
    ClusteringConfig,
    PatternCluster,
    PatternClusterer,
    PatternInstance,
)
from alma_algebrakit.learning.derivation import (
    DerivationConfig,
    ViewCandidate,
    ViewDeriver,
)
from alma_algebrakit.learning.gcs import (
    gcs_all,
    is_spj_query,
)
from alma_algebrakit.learning.patterns import PatternExtractor

if TYPE_CHECKING:
    pass

from alma_algebrakit.models.algebra import RAExpression

# =============================================================================
# Result Types
# =============================================================================


class LearnedView(BaseModel):
    """A view learned from a query workload.

    Contains the GCS expression (if computable) and metadata about
    how it was derived.
    """

    model_config = {"arbitrary_types_allowed": True}

    id: str = Field(description="View ID")
    name: str = Field(description="Suggested view name")
    expression: RAExpression | None = Field(default=None, description="The GCS expression (if SPJ)")
    cluster_id: str = Field(description="Source cluster ID")
    pattern_count: int = Field(default=0, description="Number of patterns in cluster")
    total_weight: float = Field(default=0.0, description="Total weight of patterns")

    # Derivation method
    method: str = Field(
        default="gcs",
        description="Method used: 'gcs' (algebraic) or 'feature' (feature-based)",
    )
    is_verified: bool = Field(
        default=False,
        description="Whether GCS was verified as contained in all patterns",
    )

    # GCS details
    common_relations: list[str] = Field(default_factory=list, description="Relations in the view")
    common_joins: list[str] = Field(default_factory=list, description="Join predicates")
    common_predicates: list[str] = Field(default_factory=list, description="Selection predicates")
    dropped_predicates: list[str] = Field(
        default_factory=list, description="Predicates that couldn't be generalized"
    )

    # Feature-based fallback
    feature_candidate: ViewCandidate | None = Field(
        default=None, description="Feature-based candidate (fallback)"
    )

    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Additional metadata"
    )


class HybridLearningResult(BaseModel):
    """Result of hybrid view learning."""

    views: list[LearnedView] = Field(default_factory=list, description="Learned views")
    clusters: list[PatternCluster] = Field(default_factory=list, description="Pattern clusters")
    total_patterns: int = Field(default=0)
    spj_patterns: int = Field(default=0, description="Number of SPJ patterns")
    gcs_success_count: int = Field(default=0, description="Number of clusters with successful GCS")
    fallback_count: int = Field(default=0, description="Number of clusters using feature fallback")


# =============================================================================
# Hybrid View Learner
# =============================================================================


class HybridViewLearner:
    """Learns views using hybrid feature + GCS approach.

    The learning process:
    1. Extract signatures from RA expressions
    2. Cluster patterns using feature-based similarity
    3. For each cluster:
       a. If all patterns are SPJ, compute GCS
       b. Verify GCS is contained in all patterns
       c. If verification fails or non-SPJ, fall back to feature derivation
    4. Return verified views

    Usage:
        learner = HybridViewLearner()
        result = learner.learn_views(expressions)

        for view in result.views:
            if view.is_verified:
                print(f"Verified view: {view.name}")
                print(f"  Relations: {view.common_relations}")
    """

    def __init__(
        self,
        clustering_config: ClusteringConfig | None = None,
        derivation_config: DerivationConfig | None = None,
        require_verification: bool = False,
    ) -> None:
        """Initialize the hybrid learner.

        Args:
            clustering_config: Configuration for pattern clustering
            derivation_config: Configuration for feature-based derivation
            require_verification: If True, only return verified GCS views
        """
        self.clustering_config = clustering_config or ClusteringConfig()
        self.derivation_config = derivation_config or DerivationConfig()
        self.require_verification = require_verification

        self._clusterer = PatternClusterer(self.clustering_config)
        self._deriver = ViewDeriver(self.derivation_config)
        self._extractor = PatternExtractor()

    def learn_views(
        self,
        expressions: list[RAExpression],
        weights: list[float] | None = None,
    ) -> HybridLearningResult:
        """Learn views from a list of RA expressions.

        Args:
            expressions: List of RA expressions (queries)
            weights: Optional weights for each expression

        Returns:
            HybridLearningResult with learned views
        """
        if not expressions:
            return HybridLearningResult()

        if weights is None:
            weights = [1.0] * len(expressions)

        # Step 1: Create pattern instances with both signature and expression
        patterns: list[PatternInstance] = []
        spj_count = 0

        for i, expr in enumerate(expressions):
            signature = self._extractor.extract(expr)
            is_spj = is_spj_query(expr)
            if is_spj:
                spj_count += 1

            pattern = PatternInstance(
                id=f"p_{i}",
                signature=signature,
                expression=expr,
                weight=weights[i],
                metadata={"is_spj": is_spj},
            )
            patterns.append(pattern)

        # Step 2: Cluster patterns
        clusters = self._clusterer.cluster(patterns)

        # Step 3: Learn views from each cluster
        views: list[LearnedView] = []
        gcs_success = 0
        fallback_count = 0

        for cluster in clusters:
            view = self._learn_view_from_cluster(cluster, patterns)
            if view is not None:
                views.append(view)
                if view.method == "gcs" and view.is_verified:
                    gcs_success += 1
                elif view.method == "feature":
                    fallback_count += 1

        return HybridLearningResult(
            views=views,
            clusters=clusters,
            total_patterns=len(patterns),
            spj_patterns=spj_count,
            gcs_success_count=gcs_success,
            fallback_count=fallback_count,
        )

    def _learn_view_from_cluster(
        self,
        cluster: PatternCluster,
        all_patterns: list[PatternInstance],
    ) -> LearnedView | None:
        """Learn a view from a single cluster.

        Attempts GCS first, falls back to feature-based if needed.
        """
        # Get patterns in this cluster
        cluster_patterns = [p for p in all_patterns if p.id in cluster.pattern_ids]

        if not cluster_patterns:
            return None

        # Check if all patterns are SPJ
        all_spj = all(p.metadata.get("is_spj", False) for p in cluster_patterns)

        # Get expressions (if available)
        expressions = [p.expression for p in cluster_patterns if p.expression is not None]

        # Try GCS if all are SPJ and we have expressions
        if all_spj and len(expressions) == len(cluster_patterns):
            gcs_result = gcs_all(expressions)

            if gcs_result.success and gcs_result.expression is not None:
                return LearnedView(
                    id=f"view_{cluster.id}",
                    name=self._suggest_name(cluster),
                    expression=gcs_result.expression,
                    cluster_id=cluster.id,
                    pattern_count=len(cluster_patterns),
                    total_weight=cluster.total_weight,
                    method="gcs",
                    is_verified=True,  # GCS is by definition contained in all
                    common_relations=gcs_result.common_relations,
                    common_joins=gcs_result.common_joins,
                    common_predicates=gcs_result.common_predicates,
                    dropped_predicates=gcs_result.dropped_predicates,
                    metadata={
                        "gcs_explanation": gcs_result.explanation,
                        "input_count": gcs_result.input_count,
                    },
                )

        # Fall back to feature-based derivation
        if self.require_verification:
            return None  # Skip non-verified views

        feature_candidates = self._deriver.derive_candidates([cluster], cluster_patterns)

        if feature_candidates:
            candidate = feature_candidates[0]
            return LearnedView(
                id=f"view_{cluster.id}",
                name=candidate.suggested_name,
                expression=None,  # No RA expression for feature-based
                cluster_id=cluster.id,
                pattern_count=candidate.pattern_count,
                total_weight=candidate.coverage_weight,
                method="feature",
                is_verified=False,
                common_relations=candidate.core_relations,
                common_joins=candidate.core_joins,
                common_predicates=candidate.invariant_predicates,
                dropped_predicates=candidate.optional_predicates,
                feature_candidate=candidate,
                metadata={
                    "support_score": candidate.support_score,
                    "fallback_reason": "non_spj" if not all_spj else "gcs_failed",
                },
            )

        return None

    def _suggest_name(self, cluster: PatternCluster) -> str:
        """Suggest a name for a view based on cluster contents."""
        if cluster.common_relations:
            primary = cluster.common_relations[0]
            if len(cluster.common_relations) > 1:
                return f"v_{primary}_joined"
            return f"v_{primary}"
        return f"v_cluster_{cluster.id[:8]}"


# =============================================================================
# Convenience Functions
# =============================================================================


def learn_views_hybrid(
    expressions: list[RAExpression],
    weights: list[float] | None = None,
    clustering_config: ClusteringConfig | None = None,
    derivation_config: DerivationConfig | None = None,
) -> HybridLearningResult:
    """Learn views from expressions using hybrid approach.

    This is a convenience function that creates a HybridViewLearner
    and runs the learning process.

    Args:
        expressions: List of RA expressions
        weights: Optional weights for each expression
        clustering_config: Optional clustering configuration
        derivation_config: Optional derivation configuration

    Returns:
        HybridLearningResult with learned views
    """
    learner = HybridViewLearner(
        clustering_config=clustering_config,
        derivation_config=derivation_config,
    )
    return learner.learn_views(expressions, weights)


def learn_verified_views(
    expressions: list[RAExpression],
    weights: list[float] | None = None,
) -> list[LearnedView]:
    """Learn only verified views (GCS-based, SPJ queries only).

    This function only returns views where the GCS was successfully
    computed and verified as contained in all cluster members.

    Args:
        expressions: List of RA expressions
        weights: Optional weights

    Returns:
        List of verified LearnedView objects
    """
    learner = HybridViewLearner(require_verification=True)
    result = learner.learn_views(expressions, weights)
    return result.views
