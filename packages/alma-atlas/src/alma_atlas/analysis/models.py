"""Atlas-native analysis result models."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SkippedQuery:
    fingerprint: str
    source: str
    reason: str
    sql_text: str

    def to_dict(self) -> dict[str, str]:
        return {
            "fingerprint": self.fingerprint,
            "source": self.source,
            "reason": self.reason,
            "sql_text": self.sql_text,
        }


@dataclass(frozen=True)
class AnalysisCluster:
    id: str
    pattern_ids: list[str]
    total_weight: float
    common_relations: list[str] = field(default_factory=list)
    common_joins: list[str] = field(default_factory=list)
    common_predicates: list[str] = field(default_factory=list)
    common_columns: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "pattern_ids": self.pattern_ids,
            "total_weight": self.total_weight,
            "common_relations": self.common_relations,
            "common_joins": self.common_joins,
            "common_predicates": self.common_predicates,
            "common_columns": self.common_columns,
        }


@dataclass(frozen=True)
class AnalysisCandidate:
    id: str
    suggested_name: str
    cluster_id: str
    core_relations: list[str]
    core_joins: list[str]
    core_predicates: list[str]
    core_attributes: list[str]
    candidate_keys: list[list[str]]
    proposed_grain: str | None
    support_score: float
    coverage_weight: float
    pattern_count: int
    invariant_predicates: list[str] = field(default_factory=list)
    optional_predicates: list[str] = field(default_factory=list)
    metadata: dict[str, str | int | float | bool] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "suggested_name": self.suggested_name,
            "cluster_id": self.cluster_id,
            "core_relations": self.core_relations,
            "core_joins": self.core_joins,
            "core_predicates": self.core_predicates,
            "core_attributes": self.core_attributes,
            "candidate_keys": self.candidate_keys,
            "proposed_grain": self.proposed_grain,
            "support_score": self.support_score,
            "coverage_weight": self.coverage_weight,
            "pattern_count": self.pattern_count,
            "invariant_predicates": self.invariant_predicates,
            "optional_predicates": self.optional_predicates,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class AnalysisResult:
    source_filter: str | None
    total_input_queries: int
    parsed_query_count: int
    cluster_count: int
    candidate_count: int
    clusters: list[AnalysisCluster] = field(default_factory=list)
    candidates: list[AnalysisCandidate] = field(default_factory=list)
    skipped_queries: list[SkippedQuery] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_filter": self.source_filter,
            "total_input_queries": self.total_input_queries,
            "parsed_query_count": self.parsed_query_count,
            "cluster_count": self.cluster_count,
            "candidate_count": self.candidate_count,
            "clusters": [cluster.to_dict() for cluster in self.clusters],
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "skipped_queries": [query.to_dict() for query in self.skipped_queries],
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)
