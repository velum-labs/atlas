"""Workload management for pattern learning.

A workload is a container for RA patterns that supports:
- Adding patterns from RA expressions
- Clustering patterns by structural similarity
- Deriving candidate views from clusters

This module provides SQL-agnostic workload management operating purely on
algebrakit's RA primitives.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, Field

from alma_algebrakit.learning.clustering import (
    ClusteringConfig,
    PatternCluster,
    PatternClusterer,
    PatternInstance,
)
from alma_algebrakit.learning.derivation import (
    DerivationConfig,
    HierarchicalConfig,
    HierarchicalViewDeriver,
    HierarchicalViewResult,
    ViewCandidate,
    ViewDeriver,
)
from alma_algebrakit.learning.patterns import PatternExtractor, PatternSignature
from alma_algebrakit.models.algebra import RAExpression


class Workload(BaseModel):
    """A workload of RA patterns.

    The workload is the central container for pattern learning. It:
    - Stores patterns extracted from RA expressions
    - Supports clustering patterns by similarity
    - Enables view derivation from clusters
    """

    id: str = Field(default_factory=lambda: str(uuid4()), description="Workload ID")
    name: str = Field(default="", description="Workload name")
    description: str | None = Field(default=None, description="Workload description")
    patterns: list[PatternInstance] = Field(
        default_factory=list, description="Patterns in workload"
    )
    clusters: list[PatternCluster] = Field(default_factory=list, description="Pattern clusters")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Additional metadata"
    )

    # Configuration
    clustering_config: ClusteringConfig = Field(default_factory=ClusteringConfig)
    derivation_config: DerivationConfig = Field(default_factory=DerivationConfig)
    hierarchical_config: HierarchicalConfig = Field(default_factory=HierarchicalConfig)

    model_config = {"arbitrary_types_allowed": True}

    def total_weight(self) -> float:
        """Return total weight of all patterns."""
        return sum(p.weight for p in self.patterns)

    def add_pattern(
        self,
        ra: RAExpression,
        weight: float = 1.0,
        pattern_id: str | None = None,
        store_expression: bool = True,
        **kwargs: str | int | float | bool,
    ) -> str:
        """Add a pattern from an RA expression.

        Args:
            ra: The RA expression to extract a pattern from
            weight: Pattern weight/importance
            pattern_id: Optional explicit pattern ID
            store_expression: Whether to store the original RA expression
                (enables GCS-based view learning, but uses more memory)
            **kwargs: Additional metadata

        Returns:
            The pattern ID
        """
        extractor = PatternExtractor()
        signature = extractor.extract(ra)

        pattern = PatternInstance(
            id=pattern_id or str(uuid4()),
            signature=signature,
            expression=ra if store_expression else None,
            weight=weight,
            metadata=kwargs,
        )
        self.patterns.append(pattern)
        self.updated_at = datetime.now(UTC)

        return pattern.id

    def add_signature(
        self,
        signature: PatternSignature,
        weight: float = 1.0,
        pattern_id: str | None = None,
        **kwargs: str | int | float | bool,
    ) -> str:
        """Add a pre-extracted pattern signature.

        Args:
            signature: The pattern signature
            weight: Pattern weight/importance
            pattern_id: Optional explicit pattern ID
            **kwargs: Additional metadata

        Returns:
            The pattern ID
        """
        pattern = PatternInstance(
            id=pattern_id or str(uuid4()),
            signature=signature,
            weight=weight,
            metadata=kwargs,
        )
        self.patterns.append(pattern)
        self.updated_at = datetime.now(UTC)

        return pattern.id

    def get_pattern(self, pattern_id: str) -> PatternInstance | None:
        """Get a pattern by ID."""
        for pattern in self.patterns:
            if pattern.id == pattern_id:
                return pattern
        return None

    def cluster(self, config: ClusteringConfig | None = None) -> list[PatternCluster]:
        """Cluster patterns by signature similarity.

        Args:
            config: Optional clustering configuration (uses default if not provided)

        Returns:
            List of pattern clusters
        """
        config = config or self.clustering_config
        clusterer = PatternClusterer(config)
        self.clusters = clusterer.cluster(self.patterns)
        self.updated_at = datetime.now(UTC)
        return self.clusters

    def derive_views(self, config: DerivationConfig | None = None) -> list[ViewCandidate]:
        """Derive view candidates from clusters.

        If clusters haven't been computed yet, this will cluster first.

        Args:
            config: Optional derivation configuration

        Returns:
            List of view candidates
        """
        if not self.clusters:
            self.cluster()

        config = config or self.derivation_config
        deriver = ViewDeriver(config)
        return deriver.derive_candidates(self.clusters, self.patterns)

    def derive_hierarchical_views(
        self,
        derivation_config: DerivationConfig | None = None,
        hierarchical_config: HierarchicalConfig | None = None,
    ) -> HierarchicalViewResult:
        """Derive hierarchical views (base + specialized).

        If clusters haven't been computed yet, this will cluster first.

        Args:
            derivation_config: Optional derivation configuration
            hierarchical_config: Optional hierarchical configuration

        Returns:
            HierarchicalViewResult with base and specialized views
        """
        if not self.clusters:
            self.cluster()

        derivation_config = derivation_config or self.derivation_config
        hierarchical_config = hierarchical_config or self.hierarchical_config
        deriver = HierarchicalViewDeriver(derivation_config, hierarchical_config)
        return deriver.derive_hierarchical(self.patterns, self.clusters)

    def get_cluster(self, cluster_id: str) -> PatternCluster | None:
        """Get a cluster by ID."""
        for cluster in self.clusters:
            if cluster.id == cluster_id:
                return cluster
        return None

    def patterns_in_cluster(self, cluster_id: str) -> list[PatternInstance]:
        """Get all patterns in a cluster."""
        cluster = self.get_cluster(cluster_id)
        if not cluster:
            return []
        return [p for p in self.patterns if p.id in cluster.pattern_ids]


class WorkloadStatistics(BaseModel):
    """Statistics about a workload."""

    total_patterns: int = Field(description="Total number of patterns")
    total_weight: float = Field(description="Total weight")
    unique_tables: int = Field(description="Number of unique tables")
    unique_columns: int = Field(description="Number of unique columns")
    avg_tables_per_pattern: float = Field(description="Average tables per pattern")
    avg_joins_per_pattern: float = Field(description="Average joins per pattern")
    avg_predicates_per_pattern: float = Field(description="Average predicates per pattern")
    cluster_count: int = Field(description="Number of clusters")
    coverage_by_top_clusters: dict[int, float] = Field(
        default_factory=dict, description="Coverage by top N clusters"
    )
    table_frequency: dict[str, int] = Field(
        default_factory=dict, description="Frequency of each table"
    )
    join_frequency: dict[str, int] = Field(
        default_factory=dict, description="Frequency of each join pattern"
    )


def compute_workload_statistics(workload: Workload) -> WorkloadStatistics:
    """Compute statistics for a workload.

    Args:
        workload: The workload to analyze

    Returns:
        WorkloadStatistics with computed metrics
    """
    patterns = workload.patterns
    clusters = workload.clusters

    # Basic counts
    total_patterns = len(patterns)
    total_weight = sum(p.weight for p in patterns)

    # Collect unique tables and columns
    all_tables: set[str] = set()
    all_columns: set[str] = set()
    table_frequency: dict[str, int] = {}
    join_frequency: dict[str, int] = {}

    total_tables = 0
    total_joins = 0
    total_predicates = 0

    for pattern in patterns:
        sig = pattern.signature

        # Tables
        for rel in sig.relations:
            all_tables.add(rel)
            table_frequency[rel] = table_frequency.get(rel, 0) + 1
        total_tables += len(sig.relations)

        # Columns
        for col in sig.projected_columns:
            all_columns.add(col)
        for col in sig.group_by_columns:
            all_columns.add(col)

        # Joins
        for edge in sig.join_edges:
            fp = edge.fingerprint()
            join_frequency[fp] = join_frequency.get(fp, 0) + 1
        total_joins += len(sig.join_edges)

        # Predicates
        total_predicates += len(sig.predicates)

    # Averages
    avg_tables = total_tables / total_patterns if total_patterns > 0 else 0.0
    avg_joins = total_joins / total_patterns if total_patterns > 0 else 0.0
    avg_predicates = total_predicates / total_patterns if total_patterns > 0 else 0.0

    # Coverage by top clusters
    coverage_by_top: dict[int, float] = {}
    sorted_clusters = sorted(clusters, key=lambda c: -c.total_weight)
    cumulative_weight = 0.0
    for i, cluster in enumerate(sorted_clusters):
        cumulative_weight += cluster.total_weight
        coverage_by_top[i + 1] = cumulative_weight / total_weight if total_weight > 0 else 0.0

    return WorkloadStatistics(
        total_patterns=total_patterns,
        total_weight=total_weight,
        unique_tables=len(all_tables),
        unique_columns=len(all_columns),
        avg_tables_per_pattern=avg_tables,
        avg_joins_per_pattern=avg_joins,
        avg_predicates_per_pattern=avg_predicates,
        cluster_count=len(clusters),
        coverage_by_top_clusters=coverage_by_top,
        table_frequency=table_frequency,
        join_frequency=join_frequency,
    )
