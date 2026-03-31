"""Application helpers for persisted source adapters."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from alma_connectors.adapters.airflow import AirflowAdapter
from alma_connectors.adapters.bigquery import BigQueryAdapter
from alma_connectors.adapters.dbt import DbtAdapter
from alma_connectors.adapters.fivetran import FivetranAdapter
from alma_connectors.adapters.looker import LookerAdapter
from alma_connectors.adapters.metabase import MetabaseAdapter
from alma_connectors.adapters.postgres import (
    _DEFAULT_POSTGRES_EXCLUDE_SCHEMAS,
    _DEFAULT_POSTGRES_INCLUDE_SCHEMAS,
    PostgresAdapter,
)
from alma_connectors.adapters.snowflake import SnowflakeAdapter
from alma_connectors.credentials import decrypt_credential, encrypt_credential
from alma_connectors.source_adapter import (
    AirflowAdapterConfig,
    BigQueryAdapterConfig,
    ConnectionTestResult,
    DbtAdapterConfig,
    ExternalSecretRef,
    FivetranAdapterConfig,
    LookerAdapterConfig,
    ManagedSecret,
    MetabaseAdapterConfig,
    PersistedSourceAdapter,
    PostgresAdapterConfig,
    PostgresLogCaptureConfig,
    PostgresReadReplicaConfig,
    QueryResult,
    SchemaSnapshot,
    SetupInstructions,
    SnowflakeAdapterConfig,
    SourceAdapter,
    SourceAdapterCapabilities,
    SourceAdapterDefinition,
    SourceAdapterKind,
    SourceAdapterStatus,
    TrafficObservationResult,
)
from alma_connectors.source_adapter_runtime import RuntimeSourceAdapter, instantiate_runtime_adapter
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    DefinitionSnapshot,
    DiscoverySnapshot,
    LineageSnapshot,
    OrchestrationSnapshot,
    SourceAdapterV2,
    TrafficExtractionResult,
)


def _require_dict(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


def _normalize_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return None


def _normalize_cursor(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    return None


def _iterable_or_empty(value: object) -> tuple[object, ...]:
    if isinstance(value, list | tuple):
        return tuple(value)
    return ()


class SourceAdapterService:
    """Maps persisted adapter rows onto typed runtime behavior."""

    def __init__(self, *, encryption_key: str) -> None:
        self._encryption_key = encryption_key

    def _get_adapter(self, adapter: PersistedSourceAdapter) -> RuntimeSourceAdapter:
        """Return the runtime adapter for a persisted adapter."""
        return instantiate_runtime_adapter(adapter, resolve_secret=self.resolve_secret)

    def _get_v2_adapter(self, adapter: PersistedSourceAdapter) -> SourceAdapterV2:
        runtime = self._get_adapter(adapter)
        if not isinstance(runtime, SourceAdapterV2):
            raise ValueError(f"{type(runtime).__name__} does not implement SourceAdapterV2")
        return runtime

    def _get_v1_adapter(self, adapter: PersistedSourceAdapter) -> SourceAdapter:
        runtime = self._get_adapter(adapter)
        if not isinstance(runtime, SourceAdapter):
            raise ValueError(f"{type(runtime).__name__} does not implement the legacy SourceAdapter protocol")
        return runtime

    def encrypt_secret(self, secret: str) -> bytes:
        return encrypt_credential(secret, key=self._encryption_key)

    def decrypt_secret(self, ciphertext: bytes) -> str:
        return decrypt_credential(ciphertext, key=self._encryption_key)

    def encrypt_dsn(self, dsn: str) -> bytes:
        return self.encrypt_secret(dsn)

    def decrypt_dsn(self, ciphertext: bytes) -> str:
        return self.decrypt_secret(ciphertext)

    def resolve_secret(self, secret: ManagedSecret | ExternalSecretRef) -> str:
        if isinstance(secret, ManagedSecret):
            return self.decrypt_secret(secret.ciphertext)
        provider = secret.provider.lower()
        if provider == "literal":
            return secret.reference
        if provider in {"env", "environment"}:
            resolved = os.getenv(secret.reference)
            if resolved is None:
                raise ValueError(f"environment variable '{secret.reference}' is not configured for adapter secret")
            return resolved
        raise ValueError(f"external secret provider '{secret.provider}' is not supported for live resolution")

    def get_capabilities(self, adapter: PersistedSourceAdapter) -> SourceAdapterCapabilities:
        runtime_adapter = self._get_adapter(adapter)
        capabilities = getattr(runtime_adapter, "capabilities", None)
        if capabilities is not None:
            return capabilities

        declared_capabilities = getattr(runtime_adapter, "declared_capabilities", frozenset())
        return SourceAdapterCapabilities(
            can_test_connection=True,
            can_introspect_schema=AdapterCapability.SCHEMA in declared_capabilities,
            can_observe_traffic=AdapterCapability.TRAFFIC in declared_capabilities,
            can_execute_query=False,
        )

    def get_setup_instructions(self, kind: SourceAdapterKind) -> SetupInstructions:
        if kind == SourceAdapterKind.POSTGRES:
            return PostgresAdapter(resolve_secret=self.resolve_secret).get_setup_instructions()
        if kind == SourceAdapterKind.BIGQUERY:
            return BigQueryAdapter(resolve_secret=self.resolve_secret).get_setup_instructions()
        if kind == SourceAdapterKind.SNOWFLAKE:
            return SnowflakeAdapter(resolve_secret=self.resolve_secret).get_setup_instructions()
        if kind == SourceAdapterKind.DBT:
            return DbtAdapter(manifest_path="placeholder/manifest.json").get_setup_instructions()
        if kind == SourceAdapterKind.AIRFLOW:
            return AirflowAdapter(base_url="https://airflow.example.com", auth_token="token").get_setup_instructions()
        if kind == SourceAdapterKind.LOOKER:
            return LookerAdapter(
                instance_url="https://looker.example.com",
                client_id="client-id",
                client_secret="client-secret",
            ).get_setup_instructions()
        if kind == SourceAdapterKind.FIVETRAN:
            return FivetranAdapter(api_key="api-key", api_secret="api-secret").get_setup_instructions()
        if kind == SourceAdapterKind.METABASE:
            return MetabaseAdapter(instance_url="https://metabase.example.com", api_key="api-key").get_setup_instructions()
        raise ValueError(f"unsupported adapter kind for setup instructions: {kind}")

    def serialize_definition(
        self,
        definition: SourceAdapterDefinition,
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        if definition.kind == SourceAdapterKind.POSTGRES:
            config = definition.config
            if not isinstance(config, PostgresAdapterConfig):
                raise ValueError("postgres adapter definition requires postgres config")
            payload = {
                "include_schemas": list(config.include_schemas),
                "exclude_schemas": list(config.exclude_schemas),
                "probe_target": config.probe_target,
                "log_capture": (
                    {
                        "log_path": config.log_capture.log_path,
                        "default_source": config.log_capture.default_source,
                        "default_database_name": config.log_capture.default_database_name,
                        "default_database_user": config.log_capture.default_database_user,
                    }
                    if config.log_capture is not None
                    else None
                ),
                "read_replica": (
                    {
                        "host": config.read_replica.host,
                        "port": config.read_replica.port,
                        "expected_lag_seconds": config.read_replica.expected_lag_seconds,
                    }
                    if config.read_replica is not None
                    else None
                ),
            }
            secrets = {
                "database_secret": self._serialize_secret(config.database_secret),
            }
            if config.read_replica is not None and config.read_replica.database_secret is not None:
                secrets["read_replica_database_secret"] = self._serialize_secret(config.read_replica.database_secret)
            return payload, secrets

        if definition.kind == SourceAdapterKind.BIGQUERY:
            config = definition.config
            if not isinstance(config, BigQueryAdapterConfig):
                raise ValueError("bigquery adapter definition requires bigquery config")
            payload = {
                "project_id": config.project_id,
                "location": config.location,
                "lookback_hours": config.lookback_hours,
                "max_job_rows": config.max_job_rows,
                "max_column_rows": config.max_column_rows,
                "probe_target": config.probe_target,
            }
            secrets = (
                {
                    "service_account_secret": self._serialize_secret(config.service_account_secret),
                }
                if config.service_account_secret is not None
                else {}
            )
            return payload, secrets

        if definition.kind == SourceAdapterKind.SNOWFLAKE:
            config = definition.config
            if not isinstance(config, SnowflakeAdapterConfig):
                raise ValueError("snowflake adapter definition requires snowflake config")
            payload = {
                "account": config.account,
                "warehouse": config.warehouse,
                "database": config.database,
                "role": config.role,
                "include_schemas": list(config.include_schemas),
                "exclude_schemas": list(config.exclude_schemas),
                "lookback_hours": config.lookback_hours,
                "max_query_rows": config.max_query_rows,
                "probe_target": config.probe_target,
            }
            secrets = {
                "account_secret": self._serialize_secret(config.account_secret),
            }
            return payload, secrets

        if definition.kind == SourceAdapterKind.DBT:
            config = definition.config
            if not isinstance(config, DbtAdapterConfig):
                raise ValueError("dbt adapter definition requires dbt config")
            payload: dict[str, Any] = {
                "manifest_path": config.manifest_path,
                "catalog_path": config.catalog_path,
                "run_results_path": config.run_results_path,
                "project_name": config.project_name,
            }
            secrets: dict[str, dict[str, Any]] = {}
            return payload, secrets

        if definition.kind == SourceAdapterKind.AIRFLOW:
            config = definition.config
            if not isinstance(config, AirflowAdapterConfig):
                raise ValueError("airflow adapter definition requires airflow config")
            payload = {
                "base_url": config.base_url,
                "username": config.username,
                "timeout_seconds": config.timeout_seconds,
            }
            secrets: dict[str, dict[str, Any]] = {}
            if config.auth_token_secret is not None:
                secrets["auth_token_secret"] = self._serialize_secret(config.auth_token_secret)
            if config.password_secret is not None:
                secrets["password_secret"] = self._serialize_secret(config.password_secret)
            return payload, secrets

        if definition.kind == SourceAdapterKind.LOOKER:
            config = definition.config
            if not isinstance(config, LookerAdapterConfig):
                raise ValueError("looker adapter definition requires looker config")
            payload = {
                "instance_url": config.instance_url,
                "port": config.port,
                "timeout_seconds": config.timeout_seconds,
            }
            secrets = {
                "client_id": self._serialize_secret(config.client_id),
                "client_secret": self._serialize_secret(config.client_secret),
            }
            return payload, secrets

        if definition.kind == SourceAdapterKind.FIVETRAN:
            config = definition.config
            if not isinstance(config, FivetranAdapterConfig):
                raise ValueError("fivetran adapter definition requires fivetran config")
            payload = {
                "api_base": config.api_base,
                "timeout_seconds": config.timeout_seconds,
            }
            secrets = {
                "api_key": self._serialize_secret(config.api_key),
                "api_secret": self._serialize_secret(config.api_secret),
            }
            return payload, secrets

        if definition.kind == SourceAdapterKind.METABASE:
            config = definition.config
            if not isinstance(config, MetabaseAdapterConfig):
                raise ValueError("metabase adapter definition requires metabase config")
            payload = {
                "instance_url": config.instance_url,
                "username": config.username,
                "timeout_seconds": config.timeout_seconds,
            }
            secrets: dict[str, dict[str, Any]] = {}
            if config.api_key is not None:
                secrets["api_key"] = self._serialize_secret(config.api_key)
            if config.password is not None:
                secrets["password"] = self._serialize_secret(config.password)
            return payload, secrets

        raise ValueError(f"unsupported adapter kind for serialization: {definition.kind}")

    def row_to_adapter(self, row: dict[str, Any]) -> PersistedSourceAdapter:
        adapter_id = str(row.get("id", "")).strip()
        UUID(adapter_id)
        kind = SourceAdapterKind(str(row.get("kind", "")).strip())
        config = _require_dict(row.get("config") or {}, field_name="config")
        secrets = _require_dict(row.get("secrets") or {}, field_name="secrets")

        if kind == SourceAdapterKind.POSTGRES:
            log_capture_payload = config.get("log_capture")
            log_capture = None
            if log_capture_payload:
                log_capture_data = _require_dict(log_capture_payload, field_name="log_capture")
                log_capture = PostgresLogCaptureConfig(
                    log_path=str(log_capture_data.get("log_path", "")),
                    default_source=log_capture_data.get("default_source"),
                    default_database_name=log_capture_data.get("default_database_name"),
                    default_database_user=log_capture_data.get("default_database_user"),
                )
            read_replica_payload = config.get("read_replica")
            read_replica = None
            if read_replica_payload or secrets.get("read_replica_database_secret"):
                read_replica_data = (
                    _require_dict(read_replica_payload, field_name="read_replica")
                    if read_replica_payload is not None
                    else {}
                )
                read_replica_secret_payload = secrets.get("read_replica_database_secret")
                read_replica = PostgresReadReplicaConfig(
                    database_secret=(
                        self._secret_from_storage_payload(
                            _require_dict(
                                read_replica_secret_payload or {},
                                field_name="read_replica_database_secret",
                            )
                        )
                        if read_replica_secret_payload is not None
                        else None
                    ),
                    host=read_replica_data.get("host"),
                    port=(int(read_replica_data["port"]) if read_replica_data.get("port") is not None else None),
                    expected_lag_seconds=int(read_replica_data.get("expected_lag_seconds") or 0),
                )
            adapter_config = PostgresAdapterConfig(
                database_secret=self._secret_from_storage_payload(
                    _require_dict(
                        secrets.get("database_secret") or {},
                        field_name="database_secret",
                    )
                ),
                include_schemas=tuple(
                    str(item)
                    for item in (
                        _DEFAULT_POSTGRES_INCLUDE_SCHEMAS
                        if config.get("include_schemas") is None
                        else _iterable_or_empty(config.get("include_schemas"))
                    )
                ),
                exclude_schemas=tuple(
                    str(item)
                    for item in (
                        _DEFAULT_POSTGRES_EXCLUDE_SCHEMAS
                        if config.get("exclude_schemas") is None
                        else _iterable_or_empty(config.get("exclude_schemas"))
                    )
                ),
                log_capture=log_capture,
                probe_target=(str(config["probe_target"]) if config.get("probe_target") is not None else None),
                read_replica=read_replica,
            )
        elif kind == SourceAdapterKind.BIGQUERY:
            service_account_secret = secrets.get("service_account_secret")
            adapter_config = BigQueryAdapterConfig(
                project_id=str(config.get("project_id", "")),
                service_account_secret=(
                    self._secret_from_storage_payload(
                        _require_dict(service_account_secret or {}, field_name="service_account_secret")
                    )
                    if service_account_secret is not None
                    else None
                ),
                location=str(config.get("location") or "us"),
                lookback_hours=int(config.get("lookback_hours") or 24),
                max_job_rows=int(config.get("max_job_rows") or 10_000),
                max_column_rows=int(config.get("max_column_rows") or 20_000),
                probe_target=(str(config["probe_target"]) if config.get("probe_target") is not None else None),
            )
        elif kind == SourceAdapterKind.SNOWFLAKE:
            adapter_config = SnowflakeAdapterConfig(
                account_secret=self._secret_from_storage_payload(
                    _require_dict(
                        secrets.get("account_secret") or {},
                        field_name="account_secret",
                    )
                ),
                account=str(config.get("account", "")),
                warehouse=str(config.get("warehouse") or "COMPUTE_WH"),
                database=str(config.get("database") or ""),
                role=str(config.get("role") or ""),
                include_schemas=tuple(
                    str(item) for item in (config.get("include_schemas") or [])
                ),
                exclude_schemas=tuple(
                    str(item) for item in (config.get("exclude_schemas") or ["INFORMATION_SCHEMA"])
                ),
                lookback_hours=int(config.get("lookback_hours") or 168),
                max_query_rows=int(config.get("max_query_rows") or 10_000),
                probe_target=(str(config["probe_target"]) if config.get("probe_target") is not None else None),
            )
        elif kind == SourceAdapterKind.DBT:
            adapter_config = DbtAdapterConfig(
                manifest_path=str(config.get("manifest_path", "")),
                catalog_path=(str(config["catalog_path"]) if config.get("catalog_path") is not None else None),
                run_results_path=(str(config["run_results_path"]) if config.get("run_results_path") is not None else None),
                project_name=(str(config["project_name"]) if config.get("project_name") is not None else None),
            )
        elif kind == SourceAdapterKind.AIRFLOW:
            auth_token_secret = secrets.get("auth_token_secret")
            password_secret = secrets.get("password_secret")
            adapter_config = AirflowAdapterConfig(
                base_url=str(config.get("base_url", "")),
                auth_token_secret=(
                    self._secret_from_storage_payload(
                        _require_dict(auth_token_secret or {}, field_name="auth_token_secret")
                    )
                    if auth_token_secret is not None
                    else None
                ),
                username=(str(config["username"]) if config.get("username") is not None else None),
                password_secret=(
                    self._secret_from_storage_payload(
                        _require_dict(password_secret or {}, field_name="password_secret")
                    )
                    if password_secret is not None
                    else None
                ),
                timeout_seconds=int(config.get("timeout_seconds") or 30),
            )
        elif kind == SourceAdapterKind.LOOKER:
            adapter_config = LookerAdapterConfig(
                instance_url=str(config.get("instance_url", "")),
                client_id=self._secret_from_storage_payload(
                    _require_dict(secrets.get("client_id") or {}, field_name="client_id")
                ),
                client_secret=self._secret_from_storage_payload(
                    _require_dict(secrets.get("client_secret") or {}, field_name="client_secret")
                ),
                port=int(config.get("port") or 19999),
                timeout_seconds=int(config.get("timeout_seconds") or 30),
            )
        elif kind == SourceAdapterKind.FIVETRAN:
            adapter_config = FivetranAdapterConfig(
                api_key=self._secret_from_storage_payload(
                    _require_dict(secrets.get("api_key") or {}, field_name="api_key")
                ),
                api_secret=self._secret_from_storage_payload(
                    _require_dict(secrets.get("api_secret") or {}, field_name="api_secret")
                ),
                api_base=str(config.get("api_base") or "https://api.fivetran.com"),
                timeout_seconds=int(config.get("timeout_seconds") or 30),
            )
        elif kind == SourceAdapterKind.METABASE:
            api_key_secret = secrets.get("api_key")
            password_secret = secrets.get("password")
            adapter_config = MetabaseAdapterConfig(
                instance_url=str(config.get("instance_url", "")),
                api_key=(
                    self._secret_from_storage_payload(
                        _require_dict(api_key_secret or {}, field_name="api_key")
                    )
                    if api_key_secret is not None
                    else None
                ),
                username=(str(config["username"]) if config.get("username") is not None else None),
                password=(
                    self._secret_from_storage_payload(
                        _require_dict(password_secret or {}, field_name="password")
                    )
                    if password_secret is not None
                    else None
                ),
                timeout_seconds=int(config.get("timeout_seconds") or 30),
            )
        else:
            raise ValueError(f"unsupported adapter kind for deserialization: {kind}")

        adapter = PersistedSourceAdapter(
            id=adapter_id,
            key=str(row.get("key", "")),
            display_name=str(row.get("display_name", "")),
            kind=kind,
            target_id=str(row.get("target_id", "")),
            description=row.get("description"),
            metadata={
                str(key): str(value)
                for key, value in _require_dict(
                    row.get("metadata") or {},
                    field_name="metadata",
                ).items()
            },
            config=adapter_config,
            status=SourceAdapterStatus(str(row.get("status", "pending"))),
            status_message=row.get("status_message"),
            last_tested_at=_normalize_datetime(row.get("last_tested_at")),
            last_observed_at=_normalize_datetime(row.get("last_observed_at")),
            observation_cursor=_normalize_cursor(row.get("observation_cursor")),
            created_at=_normalize_datetime(row.get("created_at")),
            updated_at=_normalize_datetime(row.get("updated_at")),
        )
        return adapter

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        return await self._get_adapter(adapter).test_connection(adapter)

    async def introspect_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshot:
        return await self._get_v1_adapter(adapter).introspect_schema(adapter)

    async def observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        return await self._get_v1_adapter(adapter).observe_traffic(adapter, since=since)

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
    ) -> QueryResult:
        return await self._get_adapter(adapter).execute_query(
            adapter,
            sql,
            max_rows=max_rows,
            probe_target=probe_target,
        )

    async def discover(self, adapter: PersistedSourceAdapter) -> DiscoverySnapshot:
        return await self._get_v2_adapter(adapter).discover(adapter)

    async def extract_schema(self, adapter: PersistedSourceAdapter):
        return await self._get_v2_adapter(adapter).extract_schema(adapter)

    async def extract_definitions(self, adapter: PersistedSourceAdapter) -> DefinitionSnapshot:
        return await self._get_v2_adapter(adapter).extract_definitions(adapter)

    async def extract_lineage(self, adapter: PersistedSourceAdapter) -> LineageSnapshot:
        return await self._get_v2_adapter(adapter).extract_lineage(adapter)

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        return await self._get_v2_adapter(adapter).extract_traffic(adapter, since=since)

    async def extract_orchestration(self, adapter: PersistedSourceAdapter) -> OrchestrationSnapshot:
        return await self._get_v2_adapter(adapter).extract_orchestration(adapter)

    def _serialize_secret(self, secret: ManagedSecret | ExternalSecretRef) -> dict[str, Any]:
        if isinstance(secret, ManagedSecret):
            return {
                "storage_kind": "managed_secret",
                "ciphertext": secret.ciphertext,
                "external_secret_provider": None,
                "external_secret_reference": None,
            }
        return {
            "storage_kind": "external_secret_ref",
            "ciphertext": None,
            "external_secret_provider": secret.provider,
            "external_secret_reference": secret.reference,
        }

    def _secret_from_storage_payload(
        self,
        payload: dict[str, Any],
    ) -> ManagedSecret | ExternalSecretRef:
        storage_kind = str(payload.get("storage_kind", "")).strip()
        if storage_kind == "managed_secret":
            ciphertext = payload.get("ciphertext")
            if not isinstance(ciphertext, bytes):
                raise ValueError("managed secrets require ciphertext bytes")
            return ManagedSecret(ciphertext=ciphertext)
        if storage_kind == "external_secret_ref":
            return ExternalSecretRef(
                provider=str(payload.get("external_secret_provider", "")),
                reference=str(payload.get("external_secret_reference", "")),
            )
        raise ValueError(f"unsupported source adapter secret kind: {storage_kind}")
