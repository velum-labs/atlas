"""Schema-based edge discovery for cross-system data movement."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from alma_connectors.edge_model import DataEdge, EdgeDiscoveryMethod, EdgeStatus, EdgeTransport
from alma_connectors.source_adapter import normalize_source_adapter_key
from alma_connectors.source_adapter_v2 import ColumnSchema, SchemaObject, SchemaSnapshotV2


def _normalize_name(value: str) -> str:
    return value.strip().lower()


def _normalize_scope(values: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _normalize_name(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return tuple(normalized)


def _object_identifier(table: SchemaObject) -> str:
    return f"{table.schema_name}.{table.object_name}"


def _object_sort_key(table: SchemaObject) -> tuple[str, str]:
    return (_normalize_name(table.schema_name), _normalize_name(table.object_name))


def _column_name_set(table: SchemaObject) -> set[str]:
    return {_normalize_name(column.name) for column in table.columns}


def _shared_columns(
    source_table: SchemaObject, dest_table: SchemaObject
) -> tuple[tuple[ColumnSchema, ColumnSchema], ...]:
    dest_lookup = {_normalize_name(column.name): column for column in dest_table.columns}
    pairs: list[tuple[ColumnSchema, ColumnSchema]] = []
    for source_column in source_table.columns:
        dest_column = dest_lookup.get(_normalize_name(source_column.name))
        if dest_column is None:
            continue
        pairs.append((source_column, dest_column))
    return tuple(pairs)


def _type_family(raw_type: str) -> str:
    normalized = _normalize_name(raw_type)
    if not normalized:
        return "unknown"
    if "uuid" in normalized or "char" in normalized or "text" in normalized:
        return "string"
    if normalized in {"string", "bytes"}:
        return normalized
    if "user-defined" in normalized or "enum" in normalized:
        return "string"
    if "json" in normalized:
        return "json"
    if "bool" in normalized:
        return "boolean"
    if "timestamp" in normalized:
        return "timestamp"
    if normalized == "datetime":
        return "datetime"
    if normalized == "date":
        return "date"
    if normalized == "time":
        return "time"
    if "numeric" in normalized or "decimal" in normalized or "number" in normalized:
        return "numeric"
    if (
        "float" in normalized
        or "double" in normalized
        or normalized == "real"
        or normalized == "float64"
    ):
        return "float"
    if "int" in normalized or normalized in {"serial", "bigserial", "smallserial"}:
        return "integer"
    if "byte" in normalized or "binary" in normalized:
        return "binary"
    if "record" in normalized or "struct" in normalized:
        return "record"
    if "array" in normalized or normalized.startswith("_"):
        return "array"
    return normalized


def _type_compatibility_score(source_type: str, dest_type: str) -> float:
    source_family = _type_family(source_type)
    dest_family = _type_family(dest_type)
    if source_family == dest_family:
        return 1.0
    if {source_family, dest_family}.issubset({"integer", "numeric", "float"}):
        return 0.5
    if {source_family, dest_family} == {"timestamp", "datetime"}:
        return 0.5
    return 0.0


@dataclass(frozen=True)
class EdgeDiscoveryConfig:
    """Configures score thresholds and destination scoping for one adapter pair."""

    match_threshold: float = 0.60
    dest_dataset_scope: tuple[str, ...] = ()
    table_name_weight: float = 0.50
    column_name_weight: float = 0.30
    type_compatibility_weight: float = 0.10
    row_count_weight: float = 0.10

    def __post_init__(self) -> None:
        if not 0.0 <= self.match_threshold <= 1.0:
            raise ValueError("match_threshold must be in [0.0, 1.0]")
        total = (
            self.table_name_weight
            + self.column_name_weight
            + self.type_compatibility_weight
            + self.row_count_weight
        )
        if total <= 0:
            raise ValueError("edge discovery weights must sum to a positive number")
        object.__setattr__(
            self,
            "dest_dataset_scope",
            _normalize_scope(self.dest_dataset_scope),
        )


@dataclass(frozen=True)
class MatchScoreBreakdown:
    """Stores one composite edge-match score and its component parts."""

    table_name_match: float
    column_name_jaccard: float
    type_compatibility_ratio: float
    row_count_similarity: float | None
    total_score: float

    def to_metadata(self) -> dict[str, object]:
        return {
            "table_name_match": self.table_name_match,
            "column_name_jaccard": self.column_name_jaccard,
            "type_compatibility_ratio": self.type_compatibility_ratio,
            "row_count_similarity": self.row_count_similarity,
            "total_score": self.total_score,
        }


@dataclass(frozen=True)
class _ScoredMatch:
    source_table: SchemaObject
    dest_table: SchemaObject
    breakdown: MatchScoreBreakdown


class EdgeDiscoveryEngine:
    """Discovers likely source-to-destination edges by comparing schema snapshots."""

    def __init__(
        self,
        *,
        source_adapter_key: str,
        dest_adapter_key: str,
        config: EdgeDiscoveryConfig | None = None,
    ) -> None:
        self._source_adapter_key = normalize_source_adapter_key(source_adapter_key)
        self._dest_adapter_key = normalize_source_adapter_key(dest_adapter_key)
        self._config = config or EdgeDiscoveryConfig()

    def discover_edges(
        self,
        source: SchemaSnapshotV2,
        dest: SchemaSnapshotV2,
    ) -> tuple[DataEdge, ...]:
        """Return discovered edges for one source/destination schema-snapshot pair."""

        source_objects = self._filter_queryable_objects(source.objects)
        scoped_dest_objects = self._filter_dest_objects(dest.objects)
        if not source_objects or not scoped_dest_objects:
            return ()

        edges: list[DataEdge] = []
        for source_table in sorted(source_objects, key=_object_sort_key):
            ranked_matches = self._rank_matches(source_table, scoped_dest_objects)
            if not ranked_matches:
                continue

            for rank, match in enumerate(ranked_matches, start=1):
                meets_threshold = match.breakdown.total_score >= self._config.match_threshold
                edges.append(
                    self._build_edge(
                        match,
                        rank=rank,
                        is_primary=rank == 1 and meets_threshold,
                        is_candidate=not meets_threshold,
                    )
                )

        return tuple(edges)

    def _filter_dest_objects(
        self, dest_objects: Sequence[SchemaObject]
    ) -> tuple[SchemaObject, ...]:
        queryable_objects = self._filter_queryable_objects(dest_objects)
        if not self._config.dest_dataset_scope:
            return tuple(sorted(queryable_objects, key=_object_sort_key))

        scope = set(self._config.dest_dataset_scope)
        return tuple(
            table
            for table in sorted(queryable_objects, key=_object_sort_key)
            if _normalize_name(table.schema_name) in scope
        )

    def _filter_queryable_objects(
        self, objects: Sequence[SchemaObject]
    ) -> tuple[SchemaObject, ...]:
        return tuple(
            obj
            for obj in objects
            if obj.columns
        )

    def _rank_matches(
        self,
        source_table: SchemaObject,
        dest_objects: Sequence[SchemaObject],
    ) -> tuple[_ScoredMatch, ...]:
        matches = [
            _ScoredMatch(
                source_table=source_table,
                dest_table=dest_table,
                breakdown=self._score_match(source_table, dest_table),
            )
            for dest_table in dest_objects
        ]
        positive_matches = [match for match in matches if match.breakdown.total_score > 0.0]
        positive_matches.sort(
            key=lambda match: (
                -match.breakdown.total_score,
                _normalize_name(match.dest_table.schema_name),
                _normalize_name(match.dest_table.object_name),
            )
        )
        return tuple(positive_matches)

    def _score_match(
        self,
        source_table: SchemaObject,
        dest_table: SchemaObject,
    ) -> MatchScoreBreakdown:
        table_name_match = float(
            _normalize_name(source_table.object_name) == _normalize_name(dest_table.object_name)
        )

        source_columns = _column_name_set(source_table)
        dest_columns = _column_name_set(dest_table)
        union_columns = source_columns | dest_columns
        if union_columns:
            column_name_jaccard = len(source_columns & dest_columns) / len(union_columns)
        else:
            column_name_jaccard = 0.0

        shared_columns = _shared_columns(source_table, dest_table)
        if shared_columns:
            type_compatibility_ratio = sum(
                _type_compatibility_score(source_column.data_type, dest_column.data_type)
                for source_column, dest_column in shared_columns
            ) / len(shared_columns)
        else:
            type_compatibility_ratio = 0.0

        row_count_similarity = self._row_count_similarity(
            source_table.row_count, dest_table.row_count
        )
        row_count_weight = (
            self._config.row_count_weight if row_count_similarity is not None else 0.0
        )
        total_weight = (
            self._config.table_name_weight
            + self._config.column_name_weight
            + self._config.type_compatibility_weight
            + row_count_weight
        )
        weighted_total = (
            (table_name_match * self._config.table_name_weight)
            + (column_name_jaccard * self._config.column_name_weight)
            + (type_compatibility_ratio * self._config.type_compatibility_weight)
            + ((row_count_similarity or 0.0) * row_count_weight)
        ) / total_weight

        return MatchScoreBreakdown(
            table_name_match=table_name_match,
            column_name_jaccard=column_name_jaccard,
            type_compatibility_ratio=type_compatibility_ratio,
            row_count_similarity=row_count_similarity,
            total_score=weighted_total,
        )

    def _row_count_similarity(
        self, source_row_count: int | None, dest_row_count: int | None
    ) -> float | None:
        if source_row_count is None or dest_row_count is None:
            return None
        if source_row_count == dest_row_count:
            return 1.0
        max_count = max(source_row_count, dest_row_count, 1)
        delta = abs(source_row_count - dest_row_count)
        similarity = 1.0 - (delta / max_count)
        return max(0.0, similarity)

    def _build_edge(
        self,
        match: _ScoredMatch,
        *,
        rank: int,
        is_primary: bool,
        is_candidate: bool,
    ) -> DataEdge:
        metadata = {
            "edge_discovery": {
                "match_threshold": self._config.match_threshold,
                "meets_threshold": not is_candidate,
                "is_primary": is_primary,
                "is_candidate": is_candidate,
                "match_rank": rank,
                "score_breakdown": match.breakdown.to_metadata(),
            }
        }
        return DataEdge(
            id=self._edge_id(match.source_table, match.dest_table),
            source_adapter_key=self._source_adapter_key,
            source_object=_object_identifier(match.source_table),
            dest_adapter_key=self._dest_adapter_key,
            dest_object=_object_identifier(match.dest_table),
            discovery_method=EdgeDiscoveryMethod.SCHEMA_MATCH,
            confidence=match.breakdown.total_score,
            transport=EdgeTransport(metadata=metadata),
            status=EdgeStatus.DISCOVERED,
        )

    def _edge_id(self, source_table: SchemaObject, dest_table: SchemaObject) -> str:
        seed = ":".join(
            (
                "edge-discovery",
                self._source_adapter_key,
                _normalize_name(_object_identifier(source_table)),
                self._dest_adapter_key,
                _normalize_name(_object_identifier(dest_table)),
            )
        )
        return str(uuid5(NAMESPACE_URL, seed))


__all__ = ["EdgeDiscoveryConfig", "EdgeDiscoveryEngine", "MatchScoreBreakdown"]
