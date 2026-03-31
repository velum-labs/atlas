"""SourceConfig -> persisted adapter + runtime adapter construction."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

from alma_atlas.config import SourceConfig
from alma_atlas.source_specs import (
    DEFAULT_BIGQUERY_LOCATION,
    DEFAULT_BIGQUERY_LOOKBACK_HOURS,
    DEFAULT_BIGQUERY_MAX_COLUMN_ROWS,
    DEFAULT_BIGQUERY_MAX_JOB_ROWS,
    DEFAULT_BIGQUERY_SERVICE_ACCOUNT_ENV,
    DEFAULT_LOOKER_PORT,
    DEFAULT_POSTGRES_EXCLUDE_SCHEMAS,
    DEFAULT_POSTGRES_SCHEMA,
    DEFAULT_SNOWFLAKE_EXCLUDE_SCHEMAS,
    DEFAULT_SNOWFLAKE_LOOKBACK_HOURS,
    DEFAULT_SNOWFLAKE_MAX_QUERY_ROWS,
    DEFAULT_SNOWFLAKE_WAREHOUSE,
    SUPPORTED_SOURCE_KINDS,
    ensure_source_params_allowed,
)
from alma_connectors.source_adapter import (
    AirflowAdapterConfig,
    BigQueryAdapterConfig,
    DbtAdapterConfig,
    ExternalSecretRef,
    FivetranAdapterConfig,
    LookerAdapterConfig,
    MetabaseAdapterConfig,
    PersistedSourceAdapter,
    PostgresAdapterConfig,
    PostgresLogCaptureConfig,
    PostgresReadReplicaConfig,
    SnowflakeAdapterConfig,
    SourceAdapterKind,
    SourceAdapterSecret,
    SourceAdapterStatus,
)
from alma_connectors.source_adapter_runtime import (
    RuntimeSourceAdapter,
    instantiate_runtime_adapter,
)


def _resolve_env(secret: object) -> str:
    provider = getattr(secret, "provider", "env")
    ref = getattr(secret, "reference", None)
    if provider == "literal":
        return ref or ""
    return os.environ.get(ref, "") if ref else ""


def _observation_cursor(source: SourceConfig) -> dict[str, object] | None:
    cursor = source.params.get("observation_cursor")
    return dict(cursor) if isinstance(cursor, dict) else None


def build_runtime_adapter(source: SourceConfig) -> tuple[RuntimeSourceAdapter, PersistedSourceAdapter]:
    """Construct both the runtime adapter and canonical persisted adapter record."""

    adapter_id = str(uuid.uuid5(uuid.NAMESPACE_URL, source.id))
    adapter_key = source.id.replace(":", "-")
    kind = source.kind

    if kind == "bigquery":
        ensure_source_params_allowed(source)
        if "credentials" in source.params and "service_account_env" in source.params:
            raise ValueError("bigquery sources may set only one of 'credentials' or 'service_account_env'")

        if "credentials" in source.params:
            credentials_path = Path(str(source.params["credentials"])).expanduser()
            if not credentials_path.exists():
                raise ValueError(f"BigQuery credentials file not found: {credentials_path}")
            service_account_secret = ExternalSecretRef(
                provider="literal",
                reference=credentials_path.read_text(encoding="utf-8"),
            )
        else:
            service_account_secret = ExternalSecretRef(
                provider="env",
                reference=source.params.get("service_account_env", DEFAULT_BIGQUERY_SERVICE_ACCOUNT_ENV),
            )

        config = BigQueryAdapterConfig(
            service_account_secret=service_account_secret,
            project_id=source.params.get("project_id") or source.params["project"],
            location=source.params.get("location", DEFAULT_BIGQUERY_LOCATION),
            lookback_hours=int(source.params.get("lookback_hours", DEFAULT_BIGQUERY_LOOKBACK_HOURS)),
            max_job_rows=int(source.params.get("max_job_rows", DEFAULT_BIGQUERY_MAX_JOB_ROWS)),
            max_column_rows=int(source.params.get("max_column_rows", DEFAULT_BIGQUERY_MAX_COLUMN_ROWS)),
            probe_target=source.params.get("probe_target"),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.BIGQUERY,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    if kind == "postgres":
        ensure_source_params_allowed(source)
        if "dsn" in source.params:
            db_secret: SourceAdapterSecret = ExternalSecretRef(
                provider="literal",
                reference=source.params["dsn"],
            )
        else:
            dsn_env = source.params.get("dsn_env")
            if not isinstance(dsn_env, str) or not dsn_env:
                raise ValueError("postgres sources require either 'dsn' or 'dsn_env'")
            db_secret = ExternalSecretRef(
                provider="env",
                reference=dsn_env,
            )

        if "include_schemas" in source.params:
            raw_schemas = source.params["include_schemas"]
            include_schemas = tuple(raw_schemas) if isinstance(raw_schemas, (list, tuple)) else (raw_schemas,)
        elif "schema" in source.params:
            include_schemas = (source.params["schema"],)
        else:
            include_schemas = (DEFAULT_POSTGRES_SCHEMA,)

        exclude_schemas = tuple(source.params.get("exclude_schemas", DEFAULT_POSTGRES_EXCLUDE_SCHEMAS))
        log_capture_raw = source.params.get("log_capture")
        log_capture = None
        if isinstance(log_capture_raw, dict):
            log_capture = PostgresLogCaptureConfig(
                log_path=log_capture_raw["log_path"],
                default_source=log_capture_raw.get("default_source"),
                default_database_name=log_capture_raw.get("default_database_name"),
                default_database_user=log_capture_raw.get("default_database_user"),
            )

        read_replica_raw = source.params.get("read_replica")
        read_replica = None
        if isinstance(read_replica_raw, dict):
            replica_secret: SourceAdapterSecret | None = None
            if "dsn" in read_replica_raw:
                replica_secret = ExternalSecretRef(
                    provider="literal",
                    reference=read_replica_raw["dsn"],
                )
            elif "dsn_env" in read_replica_raw:
                replica_secret = ExternalSecretRef(
                    provider="env",
                    reference=read_replica_raw["dsn_env"],
                )
            read_replica = PostgresReadReplicaConfig(
                database_secret=replica_secret,
                host=read_replica_raw.get("host"),
                port=read_replica_raw.get("port"),
                expected_lag_seconds=int(read_replica_raw.get("expected_lag_seconds", 0)),
            )

        config = PostgresAdapterConfig(
            database_secret=db_secret,
            include_schemas=include_schemas,
            exclude_schemas=exclude_schemas,
            log_capture=log_capture,
            probe_target=source.params.get("probe_target"),
            read_replica=read_replica,
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.POSTGRES,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    if kind == "dbt":
        ensure_source_params_allowed(source)
        config = DbtAdapterConfig(
            manifest_path=source.params.get("manifest_path", ""),
            catalog_path=source.params.get("catalog_path"),
            run_results_path=source.params.get("run_results_path"),
            project_name=source.params.get("project_name"),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.DBT,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    if kind == "snowflake":
        ensure_source_params_allowed(source)
        account_secret_env = source.params.get("account_secret_env")
        if not isinstance(account_secret_env, str) or not account_secret_env:
            raise ValueError("snowflake sources require 'account_secret_env'")
        config = SnowflakeAdapterConfig(
            account_secret=ExternalSecretRef(
                provider="env",
                reference=account_secret_env,
            ),
            account=source.params.get("account", ""),
            warehouse=source.params.get("warehouse", DEFAULT_SNOWFLAKE_WAREHOUSE),
            database=source.params.get("database", ""),
            role=source.params.get("role", ""),
            include_schemas=tuple(source.params.get("include_schemas", ())),
            exclude_schemas=tuple(source.params.get("exclude_schemas", DEFAULT_SNOWFLAKE_EXCLUDE_SCHEMAS)),
            lookback_hours=int(source.params.get("lookback_hours", DEFAULT_SNOWFLAKE_LOOKBACK_HOURS)),
            max_query_rows=int(source.params.get("max_query_rows", DEFAULT_SNOWFLAKE_MAX_QUERY_ROWS)),
            probe_target=source.params.get("probe_target"),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.SNOWFLAKE,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    if kind == "airflow":
        ensure_source_params_allowed(source)
        auth_token_secret: SourceAdapterSecret | None = None
        if source.params.get("auth_token") is not None:
            auth_token_secret = ExternalSecretRef(
                provider="literal",
                reference=source.params["auth_token"],
            )
        elif source.params.get("auth_token_env") is not None:
            auth_token_secret = ExternalSecretRef(
                provider="env",
                reference=source.params["auth_token_env"],
            )
        if auth_token_secret is None and not (source.params.get("username") and source.params.get("password")):
            raise ValueError("airflow sources require 'auth_token', 'auth_token_env', or username/password")
        config = AirflowAdapterConfig(
            base_url=source.params.get("base_url", ""),
            auth_token_secret=auth_token_secret,
            username=source.params.get("username"),
            password_secret=(
                ExternalSecretRef(provider="literal", reference=source.params["password"])
                if source.params.get("password") is not None
                else None
            ),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.AIRFLOW,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    if kind == "looker":
        ensure_source_params_allowed(source)
        client_id_secret: SourceAdapterSecret | None = None
        client_secret_secret: SourceAdapterSecret | None = None
        if source.params.get("client_id") is not None:
            client_id_secret = ExternalSecretRef(provider="literal", reference=source.params["client_id"])
        elif source.params.get("client_id_env") is not None:
            client_id_secret = ExternalSecretRef(provider="env", reference=source.params["client_id_env"])
        if source.params.get("client_secret") is not None:
            client_secret_secret = ExternalSecretRef(
                provider="literal",
                reference=source.params["client_secret"],
            )
        elif source.params.get("client_secret_env") is not None:
            client_secret_secret = ExternalSecretRef(
                provider="env",
                reference=source.params["client_secret_env"],
            )
        if client_id_secret is None or client_secret_secret is None:
            raise ValueError(
                "looker sources require client credentials via 'client_id'/'client_secret' "
                "or explicit 'client_id_env'/'client_secret_env'"
            )
        config = LookerAdapterConfig(
            instance_url=source.params.get("instance_url", ""),
            client_id=client_id_secret,
            client_secret=client_secret_secret,
            port=int(source.params.get("port", DEFAULT_LOOKER_PORT)),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.LOOKER,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    if kind == "fivetran":
        ensure_source_params_allowed(source)
        api_key_secret: SourceAdapterSecret | None = None
        api_secret_secret: SourceAdapterSecret | None = None
        if source.params.get("api_key") is not None:
            api_key_secret = ExternalSecretRef(provider="literal", reference=source.params["api_key"])
        elif source.params.get("api_key_env") is not None:
            api_key_secret = ExternalSecretRef(provider="env", reference=source.params["api_key_env"])
        if source.params.get("api_secret") is not None:
            api_secret_secret = ExternalSecretRef(provider="literal", reference=source.params["api_secret"])
        elif source.params.get("api_secret_env") is not None:
            api_secret_secret = ExternalSecretRef(provider="env", reference=source.params["api_secret_env"])
        if api_key_secret is None or api_secret_secret is None:
            raise ValueError(
                "fivetran sources require API credentials via 'api_key'/'api_secret' "
                "or explicit 'api_key_env'/'api_secret_env'"
            )
        config = FivetranAdapterConfig(
            api_key=api_key_secret,
            api_secret=api_secret_secret,
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.FIVETRAN,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    if kind == "metabase":
        ensure_source_params_allowed(source)
        api_key_secret: SourceAdapterSecret | None = None
        if source.params.get("api_key") is not None:
            api_key_secret = ExternalSecretRef(provider="literal", reference=source.params["api_key"])
        elif source.params.get("api_key_env") is not None:
            api_key_secret = ExternalSecretRef(provider="env", reference=source.params["api_key_env"])
        if api_key_secret is None and not (source.params.get("username") and source.params.get("password")):
            raise ValueError("metabase sources require 'api_key', 'api_key_env', or username/password")
        config = MetabaseAdapterConfig(
            instance_url=source.params.get("instance_url", ""),
            api_key=api_key_secret,
            username=source.params.get("username"),
            password=(
                ExternalSecretRef(provider="literal", reference=source.params["password"])
                if source.params.get("password") is not None
                else None
            ),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.METABASE,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
            observation_cursor=_observation_cursor(source),
        )
        return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted

    raise ValueError(f"Unknown source kind: {kind!r}. Supported: {', '.join(sorted(SUPPORTED_SOURCE_KINDS))}")
