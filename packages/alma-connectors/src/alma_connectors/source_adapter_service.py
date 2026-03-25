"""Application helpers for persisted source adapters."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from alma_connectors.adapters.bigquery import BigQueryAdapter
from alma_connectors.adapters.dbt import DbtAdapter
from alma_connectors.adapters.postgres import (
    _DEFAULT_POSTGRES_EXCLUDE_SCHEMAS,
    _DEFAULT_POSTGRES_INCLUDE_SCHEMAS,
    PostgresAdapter,
)
from alma_connectors.adapters.snowflake import SnowflakeAdapter
from alma_connectors.credentials import decrypt_credential, encrypt_credential
from alma_connectors.source_adapter import (
    BigQueryAdapterConfig,
    ConnectionTestResult,
    DbtAdapterConfig,
    ExternalSecretRef,
    ManagedSecret,
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


def _require_dict(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return dict(value)


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
        return dict(value)
    return None


class SourceAdapterService:
    """Maps persisted adapter rows onto typed runtime behavior."""

    def __init__(self, *, encryption_key: str) -> None:
        self._encryption_key = encryption_key
        self._registry: dict[SourceAdapterKind, SourceAdapter] = {
            SourceAdapterKind.POSTGRES: PostgresAdapter(
                resolve_secret=self.resolve_secret,
            ),
            SourceAdapterKind.BIGQUERY: BigQueryAdapter(
                resolve_secret=self.resolve_secret,
            ),
            SourceAdapterKind.SNOWFLAKE: SnowflakeAdapter(
                resolve_secret=self.resolve_secret,
            ),
        }

    def _get_adapter(self, adapter: PersistedSourceAdapter) -> SourceAdapter:
        """Return the runtime adapter for a persisted adapter.

        DbtAdapter is instantiated per-call from the persisted config because
        its paths are stored in instance state (unlike stateless v1 adapters).
        """
        if adapter.kind == SourceAdapterKind.DBT:
            if not isinstance(adapter.config, DbtAdapterConfig):
                raise ValueError("dbt adapter requires DbtAdapterConfig")
            return DbtAdapter(
                manifest_path=adapter.config.manifest_path,
                catalog_path=adapter.config.catalog_path,
                run_results_path=adapter.config.run_results_path,
                project_name=adapter.config.project_name,
            )
        return self._registry[adapter.kind]

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
        if provider in {"env", "environment"}:
            resolved = os.getenv(secret.reference)
            if resolved is None:
                raise ValueError(f"environment variable '{secret.reference}' is not configured for adapter secret")
            return resolved
        raise ValueError(f"external secret provider '{secret.provider}' is not supported for live resolution")

    def get_capabilities(self, adapter: PersistedSourceAdapter) -> SourceAdapterCapabilities:
        return self._get_adapter(adapter).capabilities

    def get_setup_instructions(self, kind: SourceAdapterKind) -> SetupInstructions:
        if kind == SourceAdapterKind.DBT:
            return DbtAdapter(manifest_path="").get_setup_instructions()
        return self._registry[kind].get_setup_instructions()

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
            secrets = {
                "service_account_secret": self._serialize_secret(config.service_account_secret),
            }
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
                        else config.get("include_schemas")
                    )
                ),
                exclude_schemas=tuple(
                    str(item)
                    for item in (
                        _DEFAULT_POSTGRES_EXCLUDE_SCHEMAS
                        if config.get("exclude_schemas") is None
                        else config.get("exclude_schemas")
                    )
                ),
                log_capture=log_capture,
                probe_target=(str(config["probe_target"]) if config.get("probe_target") is not None else None),
                read_replica=read_replica,
            )
        elif kind == SourceAdapterKind.BIGQUERY:
            adapter_config = BigQueryAdapterConfig(
                service_account_secret=self._secret_from_storage_payload(
                    _require_dict(
                        secrets.get("service_account_secret") or {},
                        field_name="service_account_secret",
                    )
                ),
                project_id=str(config.get("project_id", "")),
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
        return await self._get_adapter(adapter).introspect_schema(adapter)

    async def observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        return await self._get_adapter(adapter).observe_traffic(adapter, since=since)

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
