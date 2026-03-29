"""Canonical edge domain models for cross-system data movement."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

from alma_connectors.source_adapter import normalize_source_adapter_key


def _normalize_required_string(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def _normalize_optional_string(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string when provided")
    return normalized


def _normalize_optional_datetime(value: datetime | str | None, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{field_name} must be a non-empty ISO-8601 string when provided")
        normalized_iso = f"{normalized[:-1]}+00:00" if normalized.endswith(("Z", "z")) else normalized
        try:
            return datetime.fromisoformat(normalized_iso)
        except ValueError as error:
            raise ValueError(f"{field_name} must be an ISO-8601 datetime string") from error
    raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")


def _normalize_optional_timedelta(value: timedelta | float | int | None, *, field_name: str) -> timedelta | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    if isinstance(value, int | float):
        return timedelta(seconds=float(value))
    raise ValueError(f"{field_name} must be a timedelta or numeric seconds value")


def _normalize_string_tuple(values: Sequence[str], *, field_name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise ValueError(f"{field_name} must be a sequence of strings")
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        normalized = _normalize_required_string(raw_value, field_name=field_name)
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(normalized)
    return tuple(cleaned)


def _normalize_optional_sequence(value: object | None, *, field_name: str) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be a sequence")
    return tuple(value)


def _normalize_optional_string_tuple(value: object | None, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be a sequence of strings")
    return _normalize_string_tuple(value, field_name=field_name)


def _normalize_uuid_tuple(values: Sequence[str], *, field_name: str) -> tuple[str, ...]:
    normalized_values = _normalize_string_tuple(values, field_name=field_name)
    for value in normalized_values:
        UUID(value)
    return normalized_values


def _normalize_mapping(
    value: Mapping[str, object] | None,
    *,
    field_name: str,
) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping")
    return {str(key): item for key, item in value.items()}


def _normalize_non_negative_int(value: int | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return value


def _datetime_to_json(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _timedelta_to_json(value: timedelta | None) -> float | None:
    return value.total_seconds() if value is not None else None


def _require_mapping(value: object, *, field_name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


class EdgeDiscoveryMethod(StrEnum):
    """Supported edge-discovery sources."""

    SCHEMA_MATCH = "schema_match"
    DECLARED = "declared"
    PIPELINE_ANALYSIS = "pipeline_analysis"
    OPENLINEAGE = "openlineage"
    CONNECTOR_API = "connector_api"
    TRAFFIC_CORRELATION = "traffic_correlation"
    DBT_SOURCE_REF = "dbt_source_ref"


class EdgeStatus(StrEnum):
    """Lifecycle states for one edge."""

    DISCOVERED = "discovered"
    VERIFIED = "verified"
    MONITORED = "monitored"
    BROKEN = "broken"
    STALE = "stale"


class TransportKind(StrEnum):
    """How data is transported between systems."""

    CUSTOM_SCRIPT = "custom_script"
    AIRBYTE = "airbyte"
    FIVETRAN = "fivetran"
    CDC = "cdc"
    CLOUD_TRANSFER = "cloud_transfer"
    DBT_SEED = "dbt_seed"
    UNKNOWN = "unknown"


class CopyStrategy(StrEnum):
    """The movement strategy applied by the transport."""

    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"
    APPEND_ONLY = "append_only"
    UNKNOWN = "unknown"


class WriteDisposition(StrEnum):
    """How the destination is written during a transfer."""

    TRUNCATE = "truncate"
    APPEND = "append"
    MERGE = "merge"
    UNKNOWN = "unknown"


class SerializationFormat(StrEnum):
    """The transport serialization format."""

    CSV = "csv"
    PARQUET = "parquet"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    JSON = "json"
    UNKNOWN = "unknown"


class ColumnParity(StrEnum):
    """Column-set compatibility across one edge boundary."""

    STRICT = "strict"
    SUPERSET = "superset"
    SUBSET = "subset"
    DIVERGED = "diverged"


class TypeCompatibility(StrEnum):
    """Semantic compatibility for one type mapping."""

    EXACT = "exact"
    SAFE = "safe"
    LOSSY = "lossy"
    RISKY = "risky"
    UNKNOWN = "unknown"


class WaypointKind(StrEnum):
    """Kinds of intermediate transport waypoints."""

    SOURCE_READ = "source_read"
    OBJECT_STORE = "object_store"
    STREAM_TOPIC = "stream_topic"
    STAGING_TABLE = "staging_table"
    TRANSFORM = "transform"
    DEST_WRITE = "dest_write"


class WaypointStatus(StrEnum):
    """Observed health for one waypoint."""

    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    STALE = "stale"
    FAILED = "failed"


class ProbeMethod(StrEnum):
    """How a waypoint or edge probe is executed."""

    GCS_STAT = "gcs_stat"
    S3_HEAD = "s3_head"
    KAFKA_LAG = "kafka_lag"
    SQL_QUERY = "sql_query"
    BQ_TABLE_META = "bq_table_meta"
    API_CALL = "api_call"
    NONE = "none"


class ProbeTarget(StrEnum):
    """Where a probe should be routed when adapters support multiple targets."""

    PRIMARY = "primary"
    READ_REPLICA = "read_replica"
    METADATA_ONLY = "metadata_only"


class TransportGroupKind(StrEnum):
    """Kinds of shared-failure transport groups."""

    AIRFLOW_DAG = "airflow_dag"
    CONNECTOR = "connector"
    SHARED_CREDENTIAL = "shared_credential"
    MANUAL = "manual"


class ProbeErrorType(StrEnum):
    """Failure reasons for one probe attempt."""

    TIMEOUT = "timeout"
    PERMISSION_DENIED = "permission_denied"
    CONNECTION_FAILED = "connection_failed"
    QUERY_ERROR = "query_error"
    BUDGET_EXHAUSTED = "budget_exhausted"
    ADAPTER_UNAVAILABLE = "adapter_unavailable"


@dataclass(frozen=True)
class TypeMapping:
    """Type mapping for one column across an edge boundary."""

    column_name: str
    source_type: str
    dest_type: str
    compatibility: TypeCompatibility

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "column_name",
            _normalize_required_string(self.column_name, field_name="column_name"),
        )
        object.__setattr__(
            self,
            "source_type",
            _normalize_required_string(self.source_type, field_name="source_type"),
        )
        object.__setattr__(
            self,
            "dest_type",
            _normalize_required_string(self.dest_type, field_name="dest_type"),
        )


@dataclass(frozen=True)
class TypeWarning:
    """A non-trivial semantic risk on one type mapping."""

    column_name: str
    source_type: str
    dest_type: str
    risk: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "column_name",
            _normalize_required_string(self.column_name, field_name="column_name"),
        )
        object.__setattr__(
            self,
            "source_type",
            _normalize_required_string(self.source_type, field_name="source_type"),
        )
        object.__setattr__(
            self,
            "dest_type",
            _normalize_required_string(self.dest_type, field_name="dest_type"),
        )
        object.__setattr__(self, "risk", _normalize_required_string(self.risk, field_name="risk"))


@dataclass(frozen=True)
class NullWarning:
    """A null-handling discrepancy detected across one edge."""

    column_name: str
    issue: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "column_name",
            _normalize_required_string(self.column_name, field_name="column_name"),
        )
        object.__setattr__(self, "issue", _normalize_required_string(self.issue, field_name="issue"))


@dataclass(frozen=True)
class EdgeTransport:
    """How data physically moves across a data edge."""

    kind: TransportKind = TransportKind.UNKNOWN
    schedule: str | None = None
    strategy: CopyStrategy = CopyStrategy.UNKNOWN
    write_disposition: WriteDisposition = WriteDisposition.UNKNOWN
    serialization_format: SerializationFormat = SerializationFormat.UNKNOWN
    watermark_column: str | None = None
    select_mode: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "schedule",
            _normalize_optional_string(self.schedule, field_name="schedule"),
        )
        object.__setattr__(
            self,
            "watermark_column",
            _normalize_optional_string(self.watermark_column, field_name="watermark_column"),
        )
        object.__setattr__(
            self,
            "select_mode",
            _normalize_optional_string(self.select_mode, field_name="select_mode"),
        )
        object.__setattr__(self, "metadata", _normalize_mapping(self.metadata, field_name="metadata"))


@dataclass(frozen=True)
class EdgeContract:
    """Semantic contract observed or inferred at an edge boundary."""

    column_parity: ColumnParity
    source_only_columns: tuple[str, ...] = ()
    dest_only_columns: tuple[str, ...] = ()
    type_mappings: tuple[TypeMapping, ...] = ()
    type_warnings: tuple[TypeWarning, ...] = ()
    null_warnings: tuple[NullWarning, ...] = ()
    freshness_sla: timedelta | None = None
    observed_lag: timedelta | None = None
    analyzed_at: datetime | None = None
    schema_hash_source: str | None = None
    schema_hash_dest: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_only_columns",
            _normalize_string_tuple(self.source_only_columns, field_name="source_only_columns"),
        )
        object.__setattr__(
            self,
            "dest_only_columns",
            _normalize_string_tuple(self.dest_only_columns, field_name="dest_only_columns"),
        )
        object.__setattr__(self, "type_mappings", tuple(self.type_mappings))
        object.__setattr__(self, "type_warnings", tuple(self.type_warnings))
        object.__setattr__(self, "null_warnings", tuple(self.null_warnings))
        object.__setattr__(
            self,
            "freshness_sla",
            _normalize_optional_timedelta(self.freshness_sla, field_name="freshness_sla"),
        )
        object.__setattr__(
            self,
            "observed_lag",
            _normalize_optional_timedelta(self.observed_lag, field_name="observed_lag"),
        )
        object.__setattr__(
            self,
            "analyzed_at",
            _normalize_optional_datetime(self.analyzed_at, field_name="analyzed_at"),
        )
        object.__setattr__(
            self,
            "schema_hash_source",
            _normalize_optional_string(self.schema_hash_source, field_name="schema_hash_source"),
        )
        object.__setattr__(
            self,
            "schema_hash_dest",
            _normalize_optional_string(self.schema_hash_dest, field_name="schema_hash_dest"),
        )
        if not all(isinstance(item, TypeMapping) for item in self.type_mappings):
            raise ValueError("type_mappings must contain TypeMapping values")
        if not all(isinstance(item, TypeWarning) for item in self.type_warnings):
            raise ValueError("type_warnings must contain TypeWarning values")
        if not all(isinstance(item, NullWarning) for item in self.null_warnings):
            raise ValueError("null_warnings must contain NullWarning values")


@dataclass(frozen=True)
class DataEdge:
    """A directed data movement connection between two adapter-observed objects."""

    id: str
    source_adapter_key: str
    source_object: str
    dest_adapter_key: str
    dest_object: str
    discovery_method: EdgeDiscoveryMethod
    confidence: float
    transport: EdgeTransport = field(default_factory=EdgeTransport)
    contract: EdgeContract | None = None
    status: EdgeStatus = EdgeStatus.DISCOVERED
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        UUID(self.id)
        object.__setattr__(
            self,
            "source_adapter_key",
            normalize_source_adapter_key(self.source_adapter_key),
        )
        object.__setattr__(
            self,
            "source_object",
            _normalize_required_string(self.source_object, field_name="source_object"),
        )
        object.__setattr__(
            self,
            "dest_adapter_key",
            normalize_source_adapter_key(self.dest_adapter_key),
        )
        object.__setattr__(
            self,
            "dest_object",
            _normalize_required_string(self.dest_object, field_name="dest_object"),
        )
        object.__setattr__(
            self,
            "created_at",
            _normalize_optional_datetime(self.created_at, field_name="created_at"),
        )
        object.__setattr__(
            self,
            "updated_at",
            _normalize_optional_datetime(self.updated_at, field_name="updated_at"),
        )
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError("confidence must be in [0.0, 1.0]")
        if not isinstance(self.transport, EdgeTransport):
            raise ValueError("transport must be an EdgeTransport")
        if self.contract is not None and not isinstance(self.contract, EdgeContract):
            raise ValueError("contract must be an EdgeContract when provided")


@dataclass(frozen=True)
class EdgeWaypoint:
    """An observable intermediate step in a data edge path."""

    id: str
    edge_id: str
    ordinal: int
    kind: WaypointKind
    location: str
    observable: bool
    probe_method: ProbeMethod = ProbeMethod.NONE
    last_seen_at: datetime | None = None
    last_size_bytes: int | None = None
    last_row_count: int | None = None
    status: WaypointStatus = WaypointStatus.UNKNOWN

    def __post_init__(self) -> None:
        UUID(self.id)
        UUID(self.edge_id)
        object.__setattr__(
            self,
            "location",
            _normalize_required_string(self.location, field_name="location"),
        )
        object.__setattr__(
            self,
            "last_seen_at",
            _normalize_optional_datetime(self.last_seen_at, field_name="last_seen_at"),
        )
        object.__setattr__(
            self,
            "last_size_bytes",
            _normalize_non_negative_int(self.last_size_bytes, field_name="last_size_bytes"),
        )
        object.__setattr__(
            self,
            "last_row_count",
            _normalize_non_negative_int(self.last_row_count, field_name="last_row_count"),
        )
        if isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int):
            raise ValueError("ordinal must be an integer")
        if self.ordinal < 0:
            raise ValueError("ordinal must be >= 0")


@dataclass(frozen=True)
class TransportGroup:
    """A group of edges sharing a transport or failure domain."""

    id: str
    kind: TransportGroupKind
    group_key: str
    display_name: str
    edge_ids: tuple[str, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        UUID(self.id)
        object.__setattr__(
            self,
            "group_key",
            _normalize_required_string(self.group_key, field_name="group_key"),
        )
        object.__setattr__(
            self,
            "display_name",
            _normalize_required_string(self.display_name, field_name="display_name"),
        )
        object.__setattr__(
            self,
            "edge_ids",
            _normalize_uuid_tuple(self.edge_ids, field_name="edge_ids"),
        )
        object.__setattr__(self, "metadata", _normalize_mapping(self.metadata, field_name="metadata"))


@dataclass(frozen=True)
class ProbeSuccess:
    """One successful probe execution."""

    value: object
    duration_ms: float
    probed_at: datetime

    def __post_init__(self) -> None:
        if self.duration_ms < 0:
            raise ValueError("duration_ms must be >= 0")
        object.__setattr__(
            self,
            "probed_at",
            _normalize_optional_datetime(self.probed_at, field_name="probed_at"),
        )
        if self.probed_at is None:
            raise ValueError("probed_at is required")


@dataclass(frozen=True)
class ProbeError:
    """One failed probe execution."""

    error_type: ProbeErrorType
    message: str
    probed_at: datetime
    retryable: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "message",
            _normalize_required_string(self.message, field_name="message"),
        )
        object.__setattr__(
            self,
            "probed_at",
            _normalize_optional_datetime(self.probed_at, field_name="probed_at"),
        )
        if self.probed_at is None:
            raise ValueError("probed_at is required")


type ProbeResult = ProbeSuccess | ProbeError


@dataclass(frozen=True)
class ProbeState:
    """Persisted health state for one probe type on one edge."""

    edge_id: str
    probe_type: str
    consecutive_failures: int = 0
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_error: ProbeError | None = None

    def __post_init__(self) -> None:
        UUID(self.edge_id)
        object.__setattr__(
            self,
            "probe_type",
            _normalize_required_string(self.probe_type, field_name="probe_type"),
        )
        object.__setattr__(
            self,
            "last_success_at",
            _normalize_optional_datetime(self.last_success_at, field_name="last_success_at"),
        )
        object.__setattr__(
            self,
            "last_failure_at",
            _normalize_optional_datetime(self.last_failure_at, field_name="last_failure_at"),
        )
        if isinstance(self.consecutive_failures, bool) or not isinstance(self.consecutive_failures, int):
            raise ValueError("consecutive_failures must be an integer")
        if self.consecutive_failures < 0:
            raise ValueError("consecutive_failures must be >= 0")
        if self.last_error is not None and not isinstance(self.last_error, ProbeError):
            raise ValueError("last_error must be a ProbeError when provided")


def data_edge_to_row_payload(edge: DataEdge) -> dict[str, object]:
    return {
        "id": edge.id,
        "source_adapter_key": edge.source_adapter_key,
        "source_object": edge.source_object,
        "dest_adapter_key": edge.dest_adapter_key,
        "dest_object": edge.dest_object,
        "discovery_method": edge.discovery_method.value,
        "confidence": edge.confidence,
        "status": edge.status.value,
        "transport_json": edge_transport_to_json(edge.transport),
        "contract_json": edge_contract_to_json(edge.contract) if edge.contract is not None else None,
        "created_at": edge.created_at,
        "updated_at": edge.updated_at,
    }


def data_edge_from_row(row: Mapping[str, Any]) -> DataEdge:
    return DataEdge(
        id=str(row.get("id", "")),
        source_adapter_key=str(row.get("source_adapter_key", "")),
        source_object=str(row.get("source_object", "")),
        dest_adapter_key=str(row.get("dest_adapter_key", "")),
        dest_object=str(row.get("dest_object", "")),
        discovery_method=EdgeDiscoveryMethod(str(row.get("discovery_method", ""))),
        confidence=float(row.get("confidence", 0.0)),
        transport=edge_transport_from_json(row.get("transport_json")),
        contract=edge_contract_from_json(row.get("contract_json")),
        status=EdgeStatus(str(row.get("status", EdgeStatus.DISCOVERED.value))),
        created_at=_normalize_optional_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_normalize_optional_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def edge_transport_to_json(transport: EdgeTransport) -> dict[str, object]:
    return {
        "kind": transport.kind.value,
        "schedule": transport.schedule,
        "strategy": transport.strategy.value,
        "write_disposition": transport.write_disposition.value,
        "serialization_format": transport.serialization_format.value,
        "watermark_column": transport.watermark_column,
        "select_mode": transport.select_mode,
        "metadata": dict(transport.metadata),
    }


def edge_transport_from_json(value: object | None) -> EdgeTransport:
    if value is None:
        return EdgeTransport()
    payload = _require_mapping(value, field_name="edge_transport")
    return EdgeTransport(
        kind=TransportKind(str(payload.get("kind", TransportKind.UNKNOWN.value))),
        schedule=payload.get("schedule") if isinstance(payload.get("schedule"), str) else None,
        strategy=CopyStrategy(str(payload.get("strategy", CopyStrategy.UNKNOWN.value))),
        write_disposition=WriteDisposition(str(payload.get("write_disposition", WriteDisposition.UNKNOWN.value))),
        serialization_format=SerializationFormat(
            str(payload.get("serialization_format", SerializationFormat.UNKNOWN.value))
        ),
        watermark_column=(
            payload.get("watermark_column") if isinstance(payload.get("watermark_column"), str) else None
        ),
        select_mode=payload.get("select_mode") if isinstance(payload.get("select_mode"), str) else None,
        metadata=_normalize_mapping(payload.get("metadata"), field_name="metadata"),
    )


def edge_contract_to_json(contract: EdgeContract) -> dict[str, object]:
    return {
        "column_parity": contract.column_parity.value,
        "source_only_columns": list(contract.source_only_columns),
        "dest_only_columns": list(contract.dest_only_columns),
        "type_mappings": [type_mapping_to_json(item) for item in contract.type_mappings],
        "type_warnings": [type_warning_to_json(item) for item in contract.type_warnings],
        "null_warnings": [null_warning_to_json(item) for item in contract.null_warnings],
        "freshness_sla_seconds": _timedelta_to_json(contract.freshness_sla),
        "observed_lag_seconds": _timedelta_to_json(contract.observed_lag),
        "analyzed_at": _datetime_to_json(contract.analyzed_at),
        "schema_hash_source": contract.schema_hash_source,
        "schema_hash_dest": contract.schema_hash_dest,
    }


def edge_contract_from_json(value: object | None) -> EdgeContract | None:
    if value is None:
        return None
    payload = _require_mapping(value, field_name="edge_contract")
    return EdgeContract(
        column_parity=ColumnParity(str(payload.get("column_parity", ColumnParity.STRICT.value))),
        source_only_columns=_normalize_optional_string_tuple(
            payload.get("source_only_columns"),
            field_name="source_only_columns",
        ),
        dest_only_columns=_normalize_optional_string_tuple(
            payload.get("dest_only_columns"),
            field_name="dest_only_columns",
        ),
        type_mappings=tuple(
            type_mapping_from_json(item)
            for item in _normalize_optional_sequence(
                payload.get("type_mappings"),
                field_name="type_mappings",
            )
        ),
        type_warnings=tuple(
            type_warning_from_json(item)
            for item in _normalize_optional_sequence(
                payload.get("type_warnings"),
                field_name="type_warnings",
            )
        ),
        null_warnings=tuple(
            null_warning_from_json(item)
            for item in _normalize_optional_sequence(
                payload.get("null_warnings"),
                field_name="null_warnings",
            )
        ),
        freshness_sla=_normalize_optional_timedelta(
            payload.get("freshness_sla_seconds"),
            field_name="freshness_sla",
        ),
        observed_lag=_normalize_optional_timedelta(
            payload.get("observed_lag_seconds"),
            field_name="observed_lag",
        ),
        analyzed_at=_normalize_optional_datetime(payload.get("analyzed_at"), field_name="analyzed_at"),
        schema_hash_source=(
            payload.get("schema_hash_source") if isinstance(payload.get("schema_hash_source"), str) else None
        ),
        schema_hash_dest=(
            payload.get("schema_hash_dest") if isinstance(payload.get("schema_hash_dest"), str) else None
        ),
    )


def type_mapping_to_json(mapping: TypeMapping) -> dict[str, object]:
    return {
        "column_name": mapping.column_name,
        "source_type": mapping.source_type,
        "dest_type": mapping.dest_type,
        "compatibility": mapping.compatibility.value,
    }


def type_mapping_from_json(value: object) -> TypeMapping:
    payload = _require_mapping(value, field_name="type_mapping")
    return TypeMapping(
        column_name=str(payload.get("column_name", "")),
        source_type=str(payload.get("source_type", "")),
        dest_type=str(payload.get("dest_type", "")),
        compatibility=TypeCompatibility(str(payload.get("compatibility", "unknown"))),
    )


def type_warning_to_json(warning: TypeWarning) -> dict[str, object]:
    return {
        "column_name": warning.column_name,
        "source_type": warning.source_type,
        "dest_type": warning.dest_type,
        "risk": warning.risk,
    }


def type_warning_from_json(value: object) -> TypeWarning:
    payload = _require_mapping(value, field_name="type_warning")
    return TypeWarning(
        column_name=str(payload.get("column_name", "")),
        source_type=str(payload.get("source_type", "")),
        dest_type=str(payload.get("dest_type", "")),
        risk=str(payload.get("risk", "")),
    )


def null_warning_to_json(warning: NullWarning) -> dict[str, object]:
    return {
        "column_name": warning.column_name,
        "issue": warning.issue,
    }


def null_warning_from_json(value: object) -> NullWarning:
    payload = _require_mapping(value, field_name="null_warning")
    return NullWarning(
        column_name=str(payload.get("column_name", "")),
        issue=str(payload.get("issue", "")),
    )


__all__ = [
    "ColumnParity",
    "CopyStrategy",
    "DataEdge",
    "EdgeContract",
    "EdgeDiscoveryMethod",
    "EdgeStatus",
    "EdgeTransport",
    "EdgeWaypoint",
    "NullWarning",
    "ProbeError",
    "ProbeErrorType",
    "ProbeMethod",
    "ProbeResult",
    "ProbeState",
    "ProbeSuccess",
    "ProbeTarget",
    "SerializationFormat",
    "TransportGroup",
    "TransportGroupKind",
    "TransportKind",
    "TypeCompatibility",
    "TypeMapping",
    "TypeWarning",
    "WaypointKind",
    "WaypointStatus",
    "WriteDisposition",
    "data_edge_from_row",
    "data_edge_to_row_payload",
    "edge_contract_from_json",
    "edge_contract_to_json",
    "edge_transport_from_json",
    "edge_transport_to_json",
    "null_warning_from_json",
    "null_warning_to_json",
    "type_mapping_from_json",
    "type_mapping_to_json",
    "type_warning_from_json",
    "type_warning_to_json",
]


def edge_waypoint_to_row_payload(waypoint: EdgeWaypoint) -> dict[str, object]:
    return {
        "id": waypoint.id,
        "edge_id": waypoint.edge_id,
        "ordinal": waypoint.ordinal,
        "kind": waypoint.kind.value,
        "location": waypoint.location,
        "observable": waypoint.observable,
        "probe_method": waypoint.probe_method.value,
        "last_seen_at": waypoint.last_seen_at,
        "last_size_bytes": waypoint.last_size_bytes,
        "last_row_count": waypoint.last_row_count,
        "status": waypoint.status.value,
    }


def edge_waypoint_from_row(row: Mapping[str, Any]) -> EdgeWaypoint:
    return EdgeWaypoint(
        id=str(row.get("id", "")),
        edge_id=str(row.get("edge_id", "")),
        ordinal=int(row.get("ordinal", 0)),
        kind=WaypointKind(str(row.get("kind", ""))),
        location=str(row.get("location", "")),
        observable=bool(row.get("observable", False)),
        probe_method=ProbeMethod(str(row.get("probe_method", ProbeMethod.NONE.value))),
        last_seen_at=_normalize_optional_datetime(row.get("last_seen_at"), field_name="last_seen_at"),
        last_size_bytes=(int(row["last_size_bytes"]) if row.get("last_size_bytes") is not None else None),
        last_row_count=(int(row["last_row_count"]) if row.get("last_row_count") is not None else None),
        status=WaypointStatus(str(row.get("status", WaypointStatus.UNKNOWN.value))),
    )


def transport_group_to_row_payload(group: TransportGroup) -> dict[str, object]:
    return {
        "id": group.id,
        "kind": group.kind.value,
        "group_key": group.group_key,
        "display_name": group.display_name,
        "metadata_json": dict(group.metadata),
    }


def transport_group_from_row(
    row: Mapping[str, Any],
    *,
    edge_ids: Sequence[str] | None = None,
) -> TransportGroup:
    return TransportGroup(
        id=str(row.get("id", "")),
        kind=TransportGroupKind(str(row.get("kind", ""))),
        group_key=str(row.get("group_key", "")),
        display_name=str(row.get("display_name", "")),
        edge_ids=tuple(edge_ids or ()),
        metadata=_normalize_mapping(row.get("metadata_json"), field_name="metadata_json"),
    )


def probe_error_to_json(error: ProbeError) -> dict[str, object]:
    return {
        "error_type": error.error_type.value,
        "message": error.message,
        "probed_at": _datetime_to_json(error.probed_at),
        "retryable": error.retryable,
    }


def probe_error_from_json(value: object | None) -> ProbeError | None:
    if value is None:
        return None
    payload = _require_mapping(value, field_name="probe_error")
    return ProbeError(
        error_type=ProbeErrorType(str(payload.get("error_type", ""))),
        message=str(payload.get("message", "")),
        probed_at=_normalize_optional_datetime(payload.get("probed_at"), field_name="probed_at"),
        retryable=bool(payload.get("retryable", False)),
    )


def probe_state_to_row_payload(state: ProbeState) -> dict[str, object]:
    return {
        "edge_id": state.edge_id,
        "probe_type": state.probe_type,
        "consecutive_failures": state.consecutive_failures,
        "last_success_at": state.last_success_at,
        "last_failure_at": state.last_failure_at,
        "last_error_json": probe_error_to_json(state.last_error) if state.last_error is not None else None,
    }


def probe_state_from_row(row: Mapping[str, Any]) -> ProbeState:
    return ProbeState(
        edge_id=str(row.get("edge_id", "")),
        probe_type=str(row.get("probe_type", "")),
        consecutive_failures=int(row.get("consecutive_failures", 0)),
        last_success_at=_normalize_optional_datetime(row.get("last_success_at"), field_name="last_success_at"),
        last_failure_at=_normalize_optional_datetime(row.get("last_failure_at"), field_name="last_failure_at"),
        last_error=probe_error_from_json(row.get("last_error_json")),
    )


__all__ = [
    "ColumnParity",
    "CopyStrategy",
    "DataEdge",
    "EdgeContract",
    "EdgeDiscoveryMethod",
    "EdgeStatus",
    "EdgeTransport",
    "EdgeWaypoint",
    "NullWarning",
    "ProbeError",
    "ProbeErrorType",
    "ProbeMethod",
    "ProbeResult",
    "ProbeState",
    "ProbeSuccess",
    "ProbeTarget",
    "SerializationFormat",
    "TransportGroup",
    "TransportGroupKind",
    "TransportKind",
    "TypeCompatibility",
    "TypeMapping",
    "TypeWarning",
    "WaypointKind",
    "WaypointStatus",
    "WriteDisposition",
    "data_edge_from_row",
    "data_edge_to_row_payload",
    "edge_contract_from_json",
    "edge_contract_to_json",
    "edge_transport_from_json",
    "edge_transport_to_json",
    "edge_waypoint_from_row",
    "edge_waypoint_to_row_payload",
    "null_warning_from_json",
    "null_warning_to_json",
    "probe_error_from_json",
    "probe_error_to_json",
    "probe_state_from_row",
    "probe_state_to_row_payload",
    "transport_group_from_row",
    "transport_group_to_row_payload",
    "type_mapping_from_json",
    "type_mapping_to_json",
    "type_warning_from_json",
    "type_warning_to_json",
]
