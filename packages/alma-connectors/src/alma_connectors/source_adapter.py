"""Canonical source-adapter domain models and protocols."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID

_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_DEFAULT_INCLUDE_SCHEMAS = ("public",)
_DEFAULT_EXCLUDE_SCHEMAS = ("pg_catalog", "information_schema")
_PROBE_TARGET_VALUES = frozenset(("primary", "read_replica", "metadata_only"))


def _normalize_required_string(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def _normalize_optional_string(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string when provided")
    return normalized


def _normalize_optional_probe_target(
    value: str | None,
    *,
    field_name: str,
) -> str | None:
    normalized = _normalize_optional_string(value, field_name=field_name)
    if normalized is None:
        return None
    lowered = normalized.lower()
    if lowered not in _PROBE_TARGET_VALUES:
        allowed = ", ".join(sorted(_PROBE_TARGET_VALUES))
        raise ValueError(f"{field_name} must be one of: {allowed}")
    return lowered


def _normalize_schema_names(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_required_string(value, field_name=field_name)
        normalized_key = normalized.lower()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        cleaned.append(normalized)
    return tuple(cleaned)


def normalize_source_adapter_key(value: str) -> str:
    """Normalize a source-adapter key to the canonical persisted form."""
    normalized = value.strip().lower().replace(" ", "-")
    if not _KEY_PATTERN.fullmatch(normalized):
        raise ValueError("source adapter key must match '^[a-z0-9][a-z0-9_-]*$' after normalization")
    return normalized


class SourceAdapterKind(StrEnum):
    """Supported adapter kinds."""

    POSTGRES = "postgres"
    SQLITE = "sqlite"
    BIGQUERY = "bigquery"
    DBT = "dbt"
    SNOWFLAKE = "snowflake"
    AIRFLOW = "airflow"
    LOOKER = "looker"
    FIVETRAN = "fivetran"
    METABASE = "metabase"
    GITHUB = "github"


class SourceAdapterStatus(StrEnum):
    """Persisted adapter lifecycle states."""

    PENDING = "pending"
    READY = "ready"
    ERROR = "error"
    DISABLED = "disabled"


class SecretStorageKind(StrEnum):
    """Credential ownership strategies for adapters."""

    MANAGED_SECRET = "managed_secret"
    EXTERNAL_SECRET_REF = "external_secret_ref"


class SchemaObjectKind(StrEnum):
    """Supported schema object kinds."""

    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"


@dataclass(frozen=True)
class ManagedSecret:
    """Credential encrypted and persisted by Atlas."""

    ciphertext: bytes
    storage_kind: SecretStorageKind = SecretStorageKind.MANAGED_SECRET

    def __post_init__(self) -> None:
        if not self.ciphertext:
            raise ValueError("ciphertext must be non-empty")


@dataclass(frozen=True)
class ExternalSecretRef:
    """Reference to an externally managed secret."""

    provider: str
    reference: str
    storage_kind: SecretStorageKind = SecretStorageKind.EXTERNAL_SECRET_REF

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider",
            _normalize_required_string(self.provider, field_name="provider"),
        )
        object.__setattr__(
            self,
            "reference",
            _normalize_required_string(self.reference, field_name="reference"),
        )


type SourceAdapterSecret = ManagedSecret | ExternalSecretRef


@dataclass(frozen=True)
class PostgresLogCaptureConfig:
    """Traffic-capture settings for PostgreSQL log ingestion."""

    log_path: str
    default_source: str | None = None
    default_database_name: str | None = None
    default_database_user: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "log_path",
            _normalize_required_string(self.log_path, field_name="log_path"),
        )
        object.__setattr__(
            self,
            "default_source",
            _normalize_optional_string(self.default_source, field_name="default_source"),
        )
        object.__setattr__(
            self,
            "default_database_name",
            _normalize_optional_string(
                self.default_database_name,
                field_name="default_database_name",
            ),
        )
        object.__setattr__(
            self,
            "default_database_user",
            _normalize_optional_string(
                self.default_database_user,
                field_name="default_database_user",
            ),
        )


@dataclass(frozen=True)
class PostgresReadReplicaConfig:
    """Optional read-replica routing settings for PostgreSQL probe execution."""

    database_secret: SourceAdapterSecret | None = None
    host: str | None = None
    port: int | None = None
    expected_lag_seconds: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "host",
            _normalize_optional_string(self.host, field_name="host"),
        )
        if self.port is not None and (self.port < 1 or self.port > 65_535):
            raise ValueError("port must be between 1 and 65535")
        if self.expected_lag_seconds < 0:
            raise ValueError("expected_lag_seconds must be >= 0")
        if self.database_secret is None and self.host is None and self.port is None:
            raise ValueError("read_replica must provide at least one of database_secret, host, or port")


@dataclass(frozen=True)
class PostgresAdapterConfig:
    """Canonical persisted config for PostgreSQL adapters."""

    database_secret: SourceAdapterSecret
    include_schemas: tuple[str, ...] = _DEFAULT_INCLUDE_SCHEMAS
    exclude_schemas: tuple[str, ...] = _DEFAULT_EXCLUDE_SCHEMAS
    log_capture: PostgresLogCaptureConfig | None = None
    probe_target: str | None = None
    read_replica: PostgresReadReplicaConfig | None = None

    def __post_init__(self) -> None:
        normalized_include = _normalize_schema_names(
            self.include_schemas,
            field_name="include_schemas",
        )
        normalized_exclude = _normalize_schema_names(
            self.exclude_schemas,
            field_name="exclude_schemas",
        )
        overlap = set(schema.lower() for schema in normalized_include).intersection(
            schema.lower() for schema in normalized_exclude
        )
        if overlap:
            overlap_list = ", ".join(sorted(overlap))
            raise ValueError("include_schemas and exclude_schemas overlap after normalization: " + overlap_list)
        object.__setattr__(self, "include_schemas", normalized_include)
        object.__setattr__(self, "exclude_schemas", normalized_exclude)
        object.__setattr__(
            self,
            "probe_target",
            _normalize_optional_probe_target(self.probe_target, field_name="probe_target"),
        )


@dataclass(frozen=True)
class SQLiteAdapterConfig:
    """Canonical persisted config for SQLite adapters."""

    path: str

    def __post_init__(self) -> None:
        normalized_path = _normalize_required_string(self.path, field_name="path")
        object.__setattr__(
            self,
            "path",
            str(Path(normalized_path).expanduser().resolve()),
        )


@dataclass(frozen=True)
class BigQueryAdapterConfig:
    """Canonical persisted config for BigQuery adapters."""

    project_id: str
    service_account_secret: SourceAdapterSecret | None = None
    location: str = "us"
    lookback_hours: int = 24
    max_job_rows: int = 10_000
    max_column_rows: int = 20_000
    maximum_bytes_billed: int | None = None
    default_job_timeout_ms: int = 300_000
    include_job_cost_stats: bool = True
    probe_target: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "project_id",
            _normalize_required_string(self.project_id, field_name="project_id"),
        )
        object.__setattr__(
            self,
            "location",
            _normalize_required_string(self.location, field_name="location").lower(),
        )
        for field_name in ("lookback_hours", "max_job_rows", "max_column_rows", "default_job_timeout_ms"):
            raw_value = getattr(self, field_name)
            if raw_value < 1:
                raise ValueError(f"{field_name} must be >= 1")
        if self.maximum_bytes_billed is not None and self.maximum_bytes_billed < 1:
            raise ValueError("maximum_bytes_billed must be >= 1 when provided")
        object.__setattr__(
            self,
            "probe_target",
            _normalize_optional_probe_target(self.probe_target, field_name="probe_target"),
        )


@dataclass(frozen=True)
class DbtAdapterConfig:
    """Canonical persisted config for dbt adapters."""

    manifest_path: str
    catalog_path: str | None = None
    run_results_path: str | None = None
    project_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "manifest_path",
            _normalize_required_string(self.manifest_path, field_name="manifest_path"),
        )
        object.__setattr__(
            self,
            "catalog_path",
            _normalize_optional_string(self.catalog_path, field_name="catalog_path"),
        )
        object.__setattr__(
            self,
            "run_results_path",
            _normalize_optional_string(self.run_results_path, field_name="run_results_path"),
        )
        object.__setattr__(
            self,
            "project_name",
            _normalize_optional_string(self.project_name, field_name="project_name"),
        )


@dataclass(frozen=True)
class SnowflakeAdapterConfig:
    """Canonical persisted config for Snowflake adapters."""

    account_secret: SourceAdapterSecret
    account: str
    warehouse: str = "COMPUTE_WH"
    database: str = ""
    role: str = ""
    include_schemas: tuple[str, ...] = ()
    exclude_schemas: tuple[str, ...] = ("INFORMATION_SCHEMA",)
    lookback_hours: int = 168
    max_query_rows: int = 10_000
    probe_target: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account",
            _normalize_required_string(self.account, field_name="account"),
        )
        object.__setattr__(
            self,
            "warehouse",
            _normalize_required_string(self.warehouse, field_name="warehouse"),
        )
        normalized_exclude = _normalize_schema_names(
            self.exclude_schemas,
            field_name="exclude_schemas",
        )
        object.__setattr__(self, "exclude_schemas", normalized_exclude)
        if self.include_schemas:
            normalized_include = _normalize_schema_names(
                self.include_schemas,
                field_name="include_schemas",
            )
            object.__setattr__(self, "include_schemas", normalized_include)
        for field_name in ("lookback_hours", "max_query_rows"):
            raw_value = getattr(self, field_name)
            if raw_value < 1:
                raise ValueError(f"{field_name} must be >= 1")
        object.__setattr__(
            self,
            "probe_target",
            _normalize_optional_probe_target(self.probe_target, field_name="probe_target"),
        )


@dataclass(frozen=True)
class AirflowAdapterConfig:
    """Canonical persisted config for Airflow adapters."""

    base_url: str
    auth_token_secret: SourceAdapterSecret | None = None
    username: str | None = None
    password_secret: SourceAdapterSecret | None = None
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        normalized_base_url = _normalize_required_string(self.base_url, field_name="base_url").rstrip("/")
        if not normalized_base_url:
            raise ValueError("base_url must be a non-empty string")
        object.__setattr__(self, "base_url", normalized_base_url)
        object.__setattr__(
            self,
            "username",
            _normalize_optional_string(self.username, field_name="username"),
        )
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be >= 1")
        if self.auth_token_secret is None and (self.username is None or self.password_secret is None):
            raise ValueError("airflow adapters require either auth_token_secret or both username and password_secret")


@dataclass(frozen=True)
class LookerAdapterConfig:
    """Canonical persisted config for Looker adapters."""

    instance_url: str
    client_id: SourceAdapterSecret
    client_secret: SourceAdapterSecret
    port: int = 19999
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        normalized_instance_url = _normalize_required_string(
            self.instance_url,
            field_name="instance_url",
        ).rstrip("/")
        if not normalized_instance_url:
            raise ValueError("instance_url must be a non-empty string")
        object.__setattr__(self, "instance_url", normalized_instance_url)
        if self.port < 1 or self.port > 65_535:
            raise ValueError("port must be between 1 and 65535")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be >= 1")


@dataclass(frozen=True)
class FivetranAdapterConfig:
    """Canonical persisted config for Fivetran adapters."""

    api_key: SourceAdapterSecret
    api_secret: SourceAdapterSecret
    api_base: str = "https://api.fivetran.com"
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        normalized_api_base = _normalize_required_string(self.api_base, field_name="api_base").rstrip("/")
        if not normalized_api_base:
            raise ValueError("api_base must be a non-empty string")
        object.__setattr__(self, "api_base", normalized_api_base)
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be >= 1")


@dataclass(frozen=True)
class MetabaseAdapterConfig:
    """Canonical persisted config for Metabase adapters."""

    instance_url: str
    api_key: SourceAdapterSecret | None = None
    username: str | None = None
    password: SourceAdapterSecret | None = None
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        normalized_instance_url = _normalize_required_string(
            self.instance_url,
            field_name="instance_url",
        ).rstrip("/")
        if not normalized_instance_url:
            raise ValueError("instance_url must be a non-empty string")
        object.__setattr__(self, "instance_url", normalized_instance_url)
        object.__setattr__(
            self,
            "username",
            _normalize_optional_string(self.username, field_name="username"),
        )
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be >= 1")
        if self.api_key is None and (self.username is None or self.password is None):
            raise ValueError("metabase adapters require api_key or both username and password")


@dataclass(frozen=True)
class GitHubAdapterConfig:
    """Canonical persisted config for GitHub App adapters."""

    base_url: str = "https://api.github.com"
    app_id: str = ""
    private_key_secret: SourceAdapterSecret | None = None
    installation_id: str = ""
    token_secret: SourceAdapterSecret | None = None
    repos: tuple[str, ...] = ()
    include_patterns: tuple[str, ...] = ("*.sql", "*.py", "dbt_project.yml", "schema.yml", "*.yml")
    exclude_patterns: tuple[str, ...] = ("**/node_modules/**", "**/.git/**", "**/venv/**")
    max_file_size_bytes: int = 1_000_000
    branch: str = ""
    scan_mode: str = "clone"

    def __post_init__(self) -> None:
        normalized_base_url = _normalize_required_string(self.base_url, field_name="base_url").rstrip("/")
        object.__setattr__(self, "base_url", normalized_base_url)
        if not self.token_secret and not self.app_id:
            raise ValueError("github adapters require either app_id + installation_id or a token")
        if self.app_id and not self.installation_id:
            raise ValueError("github adapters with app_id also require installation_id")
        if self.max_file_size_bytes < 1:
            raise ValueError("max_file_size_bytes must be >= 1")
        if self.scan_mode not in ("clone", "archive", "git"):
            raise ValueError("scan_mode must be 'clone', 'archive', or 'git'")


type SourceAdapterConfig = (
    PostgresAdapterConfig
    | SQLiteAdapterConfig
    | BigQueryAdapterConfig
    | DbtAdapterConfig
    | SnowflakeAdapterConfig
    | AirflowAdapterConfig
    | LookerAdapterConfig
    | FivetranAdapterConfig
    | MetabaseAdapterConfig
    | GitHubAdapterConfig
)


@dataclass(frozen=True)
class SourceAdapterProbeRoutingOverride:
    """Per-adapter probe routing overlay resolved from customer configuration."""

    kind: SourceAdapterKind
    probe_target: str | None = None
    read_replica: PostgresReadReplicaConfig | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "probe_target",
            _normalize_optional_probe_target(self.probe_target, field_name="probe_target"),
        )
        if self.kind != SourceAdapterKind.POSTGRES and self.read_replica is not None:
            raise ValueError("read_replica is only supported for postgres adapters")


@dataclass(frozen=True)
class SourceAdapterCapabilities:
    """Capability flags advertised by an adapter implementation."""

    can_test_connection: bool = True
    can_introspect_schema: bool = True
    can_observe_traffic: bool = True
    can_execute_query: bool = True


def resolve_probe_target(adapter: PersistedSourceAdapter) -> str:
    """Return the effective probe target for one adapter."""

    if isinstance(adapter.config, PostgresAdapterConfig):
        return adapter.config.probe_target or "primary"
    if isinstance(adapter.config, BigQueryAdapterConfig):
        return adapter.config.probe_target or "metadata_only"
    if isinstance(adapter.config, SnowflakeAdapterConfig):
        return adapter.config.probe_target or "primary"
    return "primary"


def apply_probe_routing_override(
    adapter: PersistedSourceAdapter,
    override: SourceAdapterProbeRoutingOverride,
) -> PersistedSourceAdapter:
    """Overlay probe-routing settings from customer config onto a runtime adapter."""

    if override.kind != adapter.kind:
        raise ValueError(
            f"probe routing override kind '{override.kind.value}' does not match "
            f"adapter '{adapter.key}' kind '{adapter.kind.value}'"
        )

    if isinstance(adapter.config, PostgresAdapterConfig):
        updated_config = replace(
            adapter.config,
            probe_target=(override.probe_target if override.probe_target is not None else adapter.config.probe_target),
            read_replica=(override.read_replica if override.read_replica is not None else adapter.config.read_replica),
        )
        return replace(adapter, config=updated_config)

    if override.read_replica is not None:
        raise ValueError("read_replica is only supported for postgres adapters")
    if isinstance(adapter.config, DbtAdapterConfig):
        raise ValueError("dbt adapters do not support probe routing overrides")
    if not hasattr(adapter.config, "probe_target"):
        if override.probe_target is not None:
            raise ValueError(f"{adapter.kind.value} adapters do not support probe routing overrides")
        return adapter
    updated_config = replace(
        adapter.config,
        probe_target=(override.probe_target if override.probe_target is not None else adapter.config.probe_target),
    )
    return replace(adapter, config=updated_config)


@dataclass(frozen=True)
class SourceAdapterDefinition:
    """Create/update shape for persisted adapters."""

    key: str
    display_name: str
    kind: SourceAdapterKind
    target_id: str
    config: SourceAdapterConfig
    description: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", normalize_source_adapter_key(self.key))
        object.__setattr__(
            self,
            "display_name",
            _normalize_required_string(self.display_name, field_name="display_name"),
        )
        object.__setattr__(
            self,
            "target_id",
            _normalize_required_string(self.target_id, field_name="target_id"),
        )
        object.__setattr__(
            self,
            "description",
            _normalize_optional_string(self.description, field_name="description"),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True)
class PersistedSourceAdapter(SourceAdapterDefinition):
    """Fully persisted adapter record."""

    id: str = field(kw_only=True)
    status: SourceAdapterStatus = SourceAdapterStatus.PENDING
    status_message: str | None = None
    last_tested_at: datetime | None = None
    last_observed_at: datetime | None = None
    observation_cursor: dict[str, object] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        UUID(self.id)
        object.__setattr__(
            self,
            "status_message",
            _normalize_optional_string(self.status_message, field_name="status_message"),
        )


@dataclass(frozen=True)
class ConnectionTestResult:
    """Typed result from connection and credential validation."""

    success: bool
    message: str
    resource_count: int = 0
    resource_label: str = "objects"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "message",
            _normalize_required_string(self.message, field_name="message"),
        )
        object.__setattr__(
            self,
            "resource_label",
            _normalize_required_string(self.resource_label, field_name="resource_label"),
        )
        if self.resource_count < 0:
            raise ValueError("resource_count must be >= 0")


@dataclass(frozen=True)
class SourceColumnSchema:
    """One observed column in a source object."""

    name: str
    data_type: str
    is_nullable: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _normalize_required_string(self.name, field_name="name"))
        object.__setattr__(
            self,
            "data_type",
            _normalize_required_string(self.data_type, field_name="data_type"),
        )


@dataclass(frozen=True)
class SourceTableSchema:
    """One observed source object with its columns.

    The optional fields (row_count, size_bytes, partition_column, clustering_columns)
    are populated by adapters that have access to storage-level metadata (e.g. BigQuery
    TABLE_STORAGE and INFORMATION_SCHEMA.COLUMNS partition/clustering extensions).
    Adapters that lack this information leave them at their defaults.
    """

    schema_name: str
    object_name: str
    object_kind: SchemaObjectKind
    columns: tuple[SourceColumnSchema, ...] = ()
    row_count: int | None = None
    size_bytes: int | None = None
    partition_column: str | None = None
    clustering_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "schema_name",
            _normalize_required_string(self.schema_name, field_name="schema_name"),
        )
        object.__setattr__(
            self,
            "object_name",
            _normalize_required_string(self.object_name, field_name="object_name"),
        )
        if self.row_count is not None and self.row_count < 0:
            raise ValueError("row_count must be >= 0")
        if self.size_bytes is not None and self.size_bytes < 0:
            raise ValueError("size_bytes must be >= 0")


@dataclass(frozen=True)
class SourceObjectDependency:
    """Directed dependency between observed source objects."""

    source_schema: str
    source_object: str
    target_schema: str
    target_object: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_schema",
            _normalize_required_string(self.source_schema, field_name="source_schema"),
        )
        object.__setattr__(
            self,
            "source_object",
            _normalize_required_string(self.source_object, field_name="source_object"),
        )
        object.__setattr__(
            self,
            "target_schema",
            _normalize_required_string(self.target_schema, field_name="target_schema"),
        )
        object.__setattr__(
            self,
            "target_object",
            _normalize_required_string(self.target_object, field_name="target_object"),
        )


@dataclass(frozen=True)
class SchemaSnapshot:
    """Typed schema-introspection payload returned by adapters."""

    captured_at: datetime
    objects: tuple[SourceTableSchema, ...]
    dependencies: tuple[SourceObjectDependency, ...] = ()


@dataclass(frozen=True)
class ObservedQueryEvent:
    """Canonical query event emitted by traffic observation."""

    captured_at: datetime
    sql: str
    source_name: str
    query_type: str
    event_id: str | None = None
    database_name: str | None = None
    database_user: str | None = None
    client_addr: str | None = None
    statement_id: str | None = None
    error_message: str | None = None
    duration_ms: float | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)
    raw_payload: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sql", _normalize_required_string(self.sql, field_name="sql"))
        object.__setattr__(
            self,
            "source_name",
            _normalize_required_string(self.source_name, field_name="source_name"),
        )
        object.__setattr__(
            self,
            "query_type",
            _normalize_required_string(self.query_type, field_name="query_type"),
        )
        object.__setattr__(
            self,
            "event_id",
            _normalize_optional_string(self.event_id, field_name="event_id"),
        )
        object.__setattr__(
            self,
            "client_addr",
            _normalize_optional_string(self.client_addr, field_name="client_addr"),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))
        if self.raw_payload is not None:
            object.__setattr__(self, "raw_payload", dict(self.raw_payload))


@dataclass(frozen=True)
class TrafficObservationResult:
    """Typed traffic-observation payload returned by adapters."""

    scanned_records: int
    events: tuple[ObservedQueryEvent, ...]
    errors: tuple[str, ...] = ()
    observation_cursor: dict[str, object] | None = None

    def __post_init__(self) -> None:
        if self.scanned_records < 0:
            raise ValueError("scanned_records must be >= 0")


@dataclass(frozen=True)
class QueryResult:
    """Typed query execution result returned by adapters."""

    success: bool
    row_count: int
    duration_ms: float
    rows: tuple[Mapping[str, object], ...] = ()
    error_message: str | None = None
    content_hash: str | None = None
    truncated: bool = False
    bytes_processed: int | None = None
    bytes_billed: int | None = None

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise ValueError("row_count must be >= 0")
        if self.duration_ms < 0:
            raise ValueError("duration_ms must be >= 0")
        if self.bytes_processed is not None and self.bytes_processed < 0:
            raise ValueError("bytes_processed must be >= 0 when provided")
        if self.bytes_billed is not None and self.bytes_billed < 0:
            raise ValueError("bytes_billed must be >= 0 when provided")
        object.__setattr__(
            self,
            "error_message",
            _normalize_optional_string(self.error_message, field_name="error_message"),
        )
        object.__setattr__(
            self,
            "content_hash",
            _normalize_optional_string(self.content_hash, field_name="content_hash"),
        )


@dataclass(frozen=True)
class SetupInstructions:
    """Human-readable setup instructions for enabling an adapter."""

    title: str
    summary: str
    steps: tuple[str, ...]
    docs_url: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "title",
            _normalize_required_string(self.title, field_name="title"),
        )
        object.__setattr__(
            self,
            "summary",
            _normalize_required_string(self.summary, field_name="summary"),
        )
        object.__setattr__(
            self,
            "steps",
            tuple(_normalize_required_string(step, field_name="steps") for step in self.steps),
        )
        object.__setattr__(
            self,
            "docs_url",
            _normalize_optional_string(self.docs_url, field_name="docs_url"),
        )


