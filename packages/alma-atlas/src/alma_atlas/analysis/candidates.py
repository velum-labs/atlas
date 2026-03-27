"""Atlas-native candidate derivation over stored traffic workloads."""

from __future__ import annotations

from dataclasses import dataclass

from alma_algebrakit.learning.clustering import PatternCluster, PatternInstance

from alma_atlas.analysis.clustering import cluster_workload, to_analysis_clusters
from alma_atlas.analysis.models import AnalysisCandidate, AnalysisResult
from alma_atlas.analysis.snapshot import AnalysisSnapshot
from alma_atlas.analysis.workload import build_workload


@dataclass(frozen=True)
class AnalysisDerivationConfig:
    relation_support_threshold: float = 0.5
    join_support_threshold: float = 0.5
    predicate_support_threshold: float = 0.8
    attribute_support_threshold: float = 0.3
    optional_predicate_support_threshold: float = 0.3


def derive_analysis_candidates(
    clusters: list[PatternCluster],
    patterns: list[PatternInstance],
) -> list[AnalysisCandidate]:
    """Derive Atlas-native candidates from clustered patterns.

    This keeps Atlas off the deprecated algebrakit derivation APIs while still
    using the stable workload + clustering primitives already in the repo.
    """

    config = AnalysisDerivationConfig()
    candidates = [
        candidate
        for cluster in clusters
        if (candidate := _derive_candidate(cluster, patterns, config)) is not None
    ]
    candidates.sort(key=lambda candidate: candidate.coverage_weight, reverse=True)
    return candidates


def run_analysis(
    snapshot: AnalysisSnapshot,
    *,
    similarity_threshold: float | None = None,
    min_cluster_size: int | None = None,
) -> AnalysisResult:
    """Run Atlas-native clustering and candidate derivation."""

    workload_result = build_workload(snapshot.graph.queries)

    if not workload_result.workload.patterns:
        return AnalysisResult(
            source_filter=snapshot.source_filter,
            total_input_queries=workload_result.total_input_queries,
            parsed_query_count=0,
            cluster_count=0,
            candidate_count=0,
            skipped_queries=workload_result.skipped_queries,
            metadata={
                "total_query_executions": snapshot.traffic_summary.total_query_executions,
            },
        )

    raw_clusters = cluster_workload(
        workload_result.workload,
        similarity_threshold=similarity_threshold,
        min_cluster_size=min_cluster_size,
    )
    clusters = to_analysis_clusters(raw_clusters)
    candidates = derive_analysis_candidates(raw_clusters, workload_result.workload.patterns)

    return AnalysisResult(
        source_filter=snapshot.source_filter,
        total_input_queries=workload_result.total_input_queries,
        parsed_query_count=workload_result.parsed_query_count,
        cluster_count=len(clusters),
        candidate_count=len(candidates),
        clusters=clusters,
        candidates=candidates,
        skipped_queries=workload_result.skipped_queries,
        metadata={
            "total_query_executions": snapshot.traffic_summary.total_query_executions,
        },
    )


def _derive_candidate(
    cluster: PatternCluster,
    patterns: list[PatternInstance],
    config: AnalysisDerivationConfig,
) -> AnalysisCandidate | None:
    cluster_patterns = [pattern for pattern in patterns if pattern.id in set(cluster.pattern_ids)]
    relation_aliases = _relation_aliases(cluster_patterns)

    core_relations = sorted(
        _feature_values(
            cluster,
            "rel:",
            min_support=config.relation_support_threshold,
        )
    )
    core_joins = sorted(
        _feature_values(
            cluster,
            "join:",
            min_support=config.join_support_threshold,
        )
    )
    invariant_predicates = sorted(
        _feature_values(
            cluster,
            "pred:",
            min_support=config.predicate_support_threshold,
        )
    )
    optional_predicates = sorted(
        value
        for value, support in _feature_values_with_support(cluster, "pred:")
        if config.optional_predicate_support_threshold <= support < config.predicate_support_threshold
    )
    core_attributes = sorted(
        set(
            _feature_values(
                cluster,
                "proj:",
                min_support=config.attribute_support_threshold,
            )
        )
        | set(
            _feature_values(
                cluster,
                "group:",
                min_support=config.attribute_support_threshold,
            )
        )
    )

    if not core_relations:
        return None

    candidate = AnalysisCandidate(
        id=f"analysis-candidate-{cluster.id[:8]}",
        suggested_name=_suggest_candidate_name(core_relations, relation_aliases),
        cluster_id=cluster.id,
        core_relations=core_relations,
        core_joins=core_joins,
        core_predicates=invariant_predicates,
        core_attributes=core_attributes,
        candidate_keys=_candidate_keys(core_attributes),
        proposed_grain=_proposed_grain(core_attributes),
        support_score=_support_score(
            cluster,
            core_relations=core_relations,
            core_joins=core_joins,
            core_predicates=invariant_predicates,
            core_attributes=core_attributes,
        ),
        coverage_weight=cluster.total_weight,
        pattern_count=len(cluster.pattern_ids),
        invariant_predicates=invariant_predicates,
        optional_predicates=optional_predicates,
        metadata={
            "derivation_strategy": "cluster_feature_support",
            "pattern_count": len(cluster.pattern_ids),
            "resolved_relation_count": len({relation_aliases.get(rel, rel) for rel in core_relations}),
        },
    )

    if not _is_meaningful_candidate(candidate):
        return None
    return candidate


def _feature_values(
    cluster: PatternCluster,
    prefix: str,
    *,
    min_support: float,
) -> list[str]:
    return [
        value
        for value, support in _feature_values_with_support(cluster, prefix)
        if support >= min_support
    ]


def _feature_values_with_support(
    cluster: PatternCluster,
    prefix: str,
) -> list[tuple[str, float]]:
    return sorted(
        (
            feature.split(":", 1)[1],
            support,
        )
        for feature, support in cluster.feature_support.items()
        if feature.startswith(prefix)
    )


def _candidate_keys(core_attributes: list[str]) -> list[list[str]]:
    key_columns = [attribute for attribute in core_attributes if attribute.endswith(".id")]
    if not key_columns:
        return []
    return [[column] for column in key_columns]


def _proposed_grain(core_attributes: list[str]) -> str | None:
    keys = _candidate_keys(core_attributes)
    if keys:
        return f"One row per {keys[0][0]}"
    return "One row per record"


def _support_score(
    cluster: PatternCluster,
    *,
    core_relations: list[str],
    core_joins: list[str],
    core_predicates: list[str],
    core_attributes: list[str],
) -> float:
    features = (
        [f"rel:{relation}" for relation in core_relations]
        + [f"join:{join}" for join in core_joins]
        + [f"pred:{predicate}" for predicate in core_predicates]
        + [f"proj:{attribute}" for attribute in core_attributes]
    )
    if not features:
        return 0.0
    return sum(cluster.feature_support.get(feature, 0.0) for feature in features) / len(features)


def _suggest_candidate_name(
    core_relations: list[str],
    relation_aliases: dict[str, str],
) -> str:
    short_names = [
        _short_relation_name(relation_aliases.get(relation, relation))
        for relation in core_relations
    ]
    if len(short_names) == 1:
        return f"v_{short_names[0]}"
    if len(short_names) == 2:
        return f"v_{short_names[0]}_{short_names[1]}"
    return f"v_{short_names[0]}_joined_{len(short_names) - 1}"


def _short_relation_name(relation: str) -> str:
    return relation.split(".")[-1]


def _relation_aliases(patterns: list[PatternInstance]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for pattern in patterns:
        aliases.update(pattern.signature.table_aliases)
    return aliases


def _is_meaningful_candidate(candidate: AnalysisCandidate) -> bool:
    has_joins = bool(candidate.core_joins) or len(candidate.core_relations) > 1
    has_predicates = bool(candidate.invariant_predicates) or bool(candidate.core_predicates)
    meaningful_attributes = [
        attribute
        for attribute in candidate.core_attributes
        if not attribute.endswith(".id")
        and not attribute.endswith(".*")
        and attribute != "id"
        and "*" not in attribute
    ]
    has_meaningful_attributes = len(meaningful_attributes) >= 2
    return has_joins or has_predicates or has_meaningful_attributes
