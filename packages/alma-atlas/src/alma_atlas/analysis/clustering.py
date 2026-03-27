"""Atlas-native workload clustering helpers."""

from __future__ import annotations

from alma_algebrakit.learning.clustering import ClusteringConfig, PatternCluster
from alma_algebrakit.learning.workload import Workload

from alma_atlas.analysis.models import AnalysisCluster


def build_clustering_config(
    *,
    similarity_threshold: float | None = None,
    min_cluster_size: int | None = None,
) -> ClusteringConfig:
    """Build the default Atlas clustering configuration."""

    config = ClusteringConfig.join_focused()
    if similarity_threshold is not None:
        config.similarity_threshold = similarity_threshold
    if min_cluster_size is not None:
        config.min_cluster_size = min_cluster_size
    return config


def cluster_workload(
    workload: Workload,
    *,
    similarity_threshold: float | None = None,
    min_cluster_size: int | None = None,
) -> list[PatternCluster]:
    """Cluster a workload using the Atlas default similarity profile."""

    config = build_clustering_config(
        similarity_threshold=similarity_threshold,
        min_cluster_size=min_cluster_size,
    )
    return workload.cluster(config)


def to_analysis_clusters(clusters: list[PatternCluster]) -> list[AnalysisCluster]:
    """Map algebrakit clusters into Atlas API DTOs."""

    return [
        AnalysisCluster(
            id=cluster.id,
            pattern_ids=list(cluster.pattern_ids),
            total_weight=cluster.total_weight,
            common_relations=list(cluster.common_relations),
            common_joins=list(cluster.common_joins),
            common_predicates=list(cluster.common_predicates),
            common_columns=list(cluster.common_columns),
        )
        for cluster in clusters
    ]
