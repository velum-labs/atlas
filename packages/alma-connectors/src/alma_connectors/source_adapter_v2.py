"""Adapter protocol v2 domain types and SourceAdapterV2 protocol.

Purely additive — v1 types in source_adapter.py are untouched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from alma_connectors.source_adapter import (
    ConnectionTestResult,
    ObservedQueryEvent,
    PersistedSourceAdapter,
    QueryResult,
    SetupInstructions,
    SourceAdapterKind as SourceAdapterKindV2,
)

__all__ = [
    # Enums
    "AdapterCapability",
    "ExtractionScope",
    "LineageEdgeKind",
    "SchemaObjectKind",
    "SourceAdapterKindV2",
    # Core / provenance
    "CapabilityProbeResult",
    "ExtractionMeta",
    "ScopeContext",
    # DISCOVER
    "DiscoveredContainer",
    "DiscoverySnapshot",
    # SCHEMA
    "ColumnSchema",
    "ObjectDependency",
    "SchemaObject",
    "SchemaSnapshotV2",
    # DEFINITIONS
    "DefinitionSnapshot",
    "ObjectDefinition",
    # TRAFFIC
    "TrafficExtractionResult",
    # LINEAGE
    "LineageEdge",
    "LineageSnapshot",
    # ORCHESTRATION
    "OrchestrationSnapshot",
    "OrchestrationTask",
    "OrchestrationUnit",
    # Protocol
    "SourceAdapterV2",
]

# ---------------------------------------------------------------------------
# Internal helpers (same style as v1)
# ---------------------------------------------------------------------------


def _norm_req(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def _norm_opt(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string when provided")
    return normalized


# ---------------------------------------------------------------------------
# Capability / scope enums
# ---------------------------------------------------------------------------


class AdapterCapability(StrEnum):
    """The six extraction capabilities of the v2 protocol."""

    DISCOVER = "discover"
    SCHEMA = "schema"
    DEFINITIONS = "definitions"
    TRAFFIC = "traffic"
    LINEAGE = "lineage"
    ORCHESTRATION = "orchestration"


class ExtractionScope(StrEnum):
    """Granularity of an extraction run."""

    GLOBAL = "global"
    REGION = "region"
    DATABASE = "database"
    SCHEMA = "schema"
    OBJECT = "object"


# ---------------------------------------------------------------------------
# Scope context
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScopeContext:
    """Where an extraction ran."""

    scope: ExtractionScope
    identifiers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "identifiers", dict(self.identifiers))


# ---------------------------------------------------------------------------
# Capability probing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CapabilityProbeResult:
    """Runtime confirmation of one capability's availability."""

    capability: AdapterCapability
    available: bool
    scope: ExtractionScope
    scope_context: ScopeContext | None = None
    fallback_used: bool = False
    message: str | None = None
    permissions_missing: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "message",
            _norm_opt(self.message, field_name="message"),
        )
        object.__setattr__(
            self,
            "permissions_missing",
            tuple(
                _norm_req(p, field_name="permissions_missing")
                for p in self.permissions_missing
            ),
        )


# ---------------------------------------------------------------------------
# Extraction provenance
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionMeta:
    """Provenance carried by every extraction result."""

    adapter_key: str
    adapter_kind: SourceAdapterKindV2
    capability: AdapterCapability
    scope_context: ScopeContext
    captured_at: datetime
    duration_ms: float
    row_count: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "adapter_key",
            _norm_req(self.adapter_key, field_name="adapter_key"),
        )
        if self.duration_ms < 0:
            raise ValueError("duration_ms must be >= 0")
        if self.row_count < 0:
            raise ValueError("row_count must be >= 0")


# ---------------------------------------------------------------------------
# DISCOVER
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DiscoveredContainer:
    """A namespace: BQ dataset, Snowflake database, PG schema, dbt project, Airflow instance."""

    container_id: str
    container_type: str
    display_name: str
    location: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)
    raw_payload: dict[str, object] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "container_id",
            _norm_req(self.container_id, field_name="container_id"),
        )
        object.__setattr__(
            self,
            "container_type",
            _norm_req(self.container_type, field_name="container_type"),
        )
        object.__setattr__(
            self,
            "display_name",
            _norm_req(self.display_name, field_name="display_name"),
        )
        object.__setattr__(
            self,
            "location",
            _norm_opt(self.location, field_name="location"),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))
        if self.raw_payload is not None:
            object.__setattr__(self, "raw_payload", dict(self.raw_payload))


@dataclass(frozen=True)
class DiscoverySnapshot:
    """Result of a DISCOVER extraction."""

    meta: ExtractionMeta
    containers: tuple[DiscoveredContainer, ...]


# ---------------------------------------------------------------------------
# SCHEMA
# ---------------------------------------------------------------------------


class SchemaObjectKind(StrEnum):
    """Extended object kinds (superset of v1 SchemaObjectKind)."""

    # v1 kinds
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    EXTERNAL_TABLE = "external_table"
    # v2 additions
    UDF = "udf"
    PROCEDURE = "procedure"
    TABLE_FUNCTION = "table_function"
    MACRO = "macro"
    ML_MODEL = "ml_model"
    SEMANTIC_MODEL = "semantic_model"
    METRIC = "metric"


@dataclass(frozen=True)
class ColumnSchema:
    """One column or routine parameter."""

    name: str
    data_type: str
    is_nullable: bool = True
    description: str | None = None
    is_partition: bool = False
    clustering_ordinal: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _norm_req(self.name, field_name="name"))
        object.__setattr__(
            self,
            "data_type",
            _norm_req(self.data_type, field_name="data_type"),
        )
        object.__setattr__(
            self,
            "description",
            _norm_opt(self.description, field_name="description"),
        )
        if self.clustering_ordinal is not None and self.clustering_ordinal < 0:
            raise ValueError("clustering_ordinal must be >= 0")


@dataclass(frozen=True)
class SchemaObject:
    """Universal metadata object — tables, views, routines, models."""

    schema_name: str
    object_name: str
    kind: SchemaObjectKind
    columns: tuple[ColumnSchema, ...] = ()

    # Freshness (populated when available, None when not)
    last_modified: datetime | None = None
    last_queried: datetime | None = None
    row_count: int | None = None
    size_bytes: int | None = None

    # Routine-specific (None for non-routines)
    language: str | None = None
    return_type: str | None = None
    definition_body: str | None = None

    # Model-specific (None for non-models)
    model_type: str | None = None
    feature_columns: tuple[str, ...] = ()
    label_column: str | None = None

    # Storage partitioning/clustering
    partition_column: str | None = None
    clustering_columns: tuple[str, ...] = ()

    # General
    description: str | None = None
    owner: str | None = None
    tags: tuple[str, ...] = ()
    metadata: dict[str, object] = field(default_factory=dict)
    raw_payload: dict[str, object] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "schema_name",
            _norm_req(self.schema_name, field_name="schema_name"),
        )
        object.__setattr__(
            self,
            "object_name",
            _norm_req(self.object_name, field_name="object_name"),
        )
        if self.row_count is not None and self.row_count < 0:
            raise ValueError("row_count must be >= 0")
        if self.size_bytes is not None and self.size_bytes < 0:
            raise ValueError("size_bytes must be >= 0")
        for opt_field in (
            "language",
            "return_type",
            "definition_body",
            "model_type",
            "label_column",
            "partition_column",
            "description",
            "owner",
        ):
            object.__setattr__(
                self,
                opt_field,
                _norm_opt(getattr(self, opt_field), field_name=opt_field),
            )
        object.__setattr__(self, "metadata", dict(self.metadata))
        if self.raw_payload is not None:
            object.__setattr__(self, "raw_payload", dict(self.raw_payload))


@dataclass(frozen=True)
class ObjectDependency:
    """Directed dependency between objects within the same adapter."""

    source_schema: str
    source_object: str
    target_schema: str
    target_object: str

    def __post_init__(self) -> None:
        for fld in ("source_schema", "source_object", "target_schema", "target_object"):
            object.__setattr__(self, fld, _norm_req(getattr(self, fld), field_name=fld))


@dataclass(frozen=True)
class SchemaSnapshotV2:
    """Result of a SCHEMA extraction."""

    meta: ExtractionMeta
    objects: tuple[SchemaObject, ...]
    dependencies: tuple[ObjectDependency, ...] = ()


# ---------------------------------------------------------------------------
# DEFINITIONS
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ObjectDefinition:
    """DDL or source text for one schema object."""

    schema_name: str
    object_name: str
    object_kind: SchemaObjectKind
    definition_text: str
    definition_language: str
    metadata: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for fld in ("schema_name", "object_name", "definition_text", "definition_language"):
            object.__setattr__(self, fld, _norm_req(getattr(self, fld), field_name=fld))
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class DefinitionSnapshot:
    """Result of a DEFINITIONS extraction."""

    meta: ExtractionMeta
    definitions: tuple[ObjectDefinition, ...]


# ---------------------------------------------------------------------------
# TRAFFIC
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TrafficExtractionResult:
    """Result of a TRAFFIC extraction, wrapping v1 ObservedQueryEvent."""

    meta: ExtractionMeta
    events: tuple[ObservedQueryEvent, ...]
    observation_cursor: dict[str, object] | None = None

    def __post_init__(self) -> None:
        if self.observation_cursor is not None:
            object.__setattr__(self, "observation_cursor", dict(self.observation_cursor))


# ---------------------------------------------------------------------------
# LINEAGE
# ---------------------------------------------------------------------------


class LineageEdgeKind(StrEnum):
    """How a lineage edge was derived."""

    DECLARED = "declared"
    INFERRED_SQL = "inferred_sql"
    INFERRED_TRAFFIC = "inferred_traffic"
    CONNECTOR_API = "connector_api"
    HEURISTIC = "heuristic"


@dataclass(frozen=True)
class LineageEdge:
    """One directed data-flow edge between two objects."""

    source_object: str
    target_object: str
    edge_kind: LineageEdgeKind
    confidence: float
    column_mappings: tuple[tuple[str, str], ...] = ()
    transformation_sql: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_object",
            _norm_req(self.source_object, field_name="source_object"),
        )
        object.__setattr__(
            self,
            "target_object",
            _norm_req(self.target_object, field_name="target_object"),
        )
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError("confidence must be between 0.0 and 1.0")
        object.__setattr__(
            self,
            "transformation_sql",
            _norm_opt(self.transformation_sql, field_name="transformation_sql"),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class LineageSnapshot:
    """Result of a LINEAGE extraction."""

    meta: ExtractionMeta
    edges: tuple[LineageEdge, ...]


# ---------------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrchestrationTask:
    """One task within an orchestration unit."""

    task_id: str
    task_type: str
    upstream_task_ids: tuple[str, ...] = ()
    metadata: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "task_id",
            _norm_req(self.task_id, field_name="task_id"),
        )
        object.__setattr__(
            self,
            "task_type",
            _norm_req(self.task_type, field_name="task_type"),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class OrchestrationUnit:
    """A DAG, pipeline, stream, or scheduled sync."""

    unit_id: str
    unit_type: str
    display_name: str
    schedule: str | None = None
    tasks: tuple[OrchestrationTask, ...] = ()
    last_run_at: datetime | None = None
    last_run_status: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "unit_id",
            _norm_req(self.unit_id, field_name="unit_id"),
        )
        object.__setattr__(
            self,
            "unit_type",
            _norm_req(self.unit_type, field_name="unit_type"),
        )
        object.__setattr__(
            self,
            "display_name",
            _norm_req(self.display_name, field_name="display_name"),
        )
        object.__setattr__(
            self,
            "schedule",
            _norm_opt(self.schedule, field_name="schedule"),
        )
        object.__setattr__(
            self,
            "last_run_status",
            _norm_opt(self.last_run_status, field_name="last_run_status"),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class OrchestrationSnapshot:
    """Result of an ORCHESTRATION extraction."""

    meta: ExtractionMeta
    units: tuple[OrchestrationUnit, ...]


# ---------------------------------------------------------------------------
# SourceAdapterV2 Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class SourceAdapterV2(Protocol):
    """Universal metadata extraction protocol (v2).

    Adapters declare capabilities statically via ``declared_capabilities``
    and confirm them at runtime via ``probe()``. The scanner calls extract
    methods only for capabilities that passed probing.
    """

    @property
    def declared_capabilities(self) -> frozenset[AdapterCapability]:
        """Capabilities this adapter implementation claims to support."""
        ...

    # --- lifecycle ---

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate credentials and connectivity."""
        ...

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe which capabilities are actually available.

        If *capabilities* is ``None``, probes all ``declared_capabilities``.
        """
        ...

    # --- extraction ---

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: enumerate containers and top-level namespaces."""
        ...

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """SCHEMA: structure of all objects (tables, views, routines, models, freshness)."""
        ...

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """DEFINITIONS: DDLs, compiled SQL, view definitions."""
        ...

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        """TRAFFIC: raw query/execution events (incremental via cursor)."""
        ...

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """LINEAGE: declared or inferred data flow edges."""
        ...

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """ORCHESTRATION: DAGs, task dependencies, schedules."""
        ...

    # --- utility (not capabilities) ---

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
        dry_run: bool = False,
    ) -> QueryResult:
        """Execute SQL for validation or shadow workflows."""
        ...

    def get_setup_instructions(self) -> SetupInstructions:
        """Operator guidance for enabling this adapter kind."""
        ...
