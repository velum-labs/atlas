"""SourceConfig -> persisted adapter + runtime adapter construction."""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from alma_atlas.config import SourceConfig
from alma_atlas.source_registry import SUPPORTED_SOURCE_KINDS
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

type AdapterConfig = (
    AirflowAdapterConfig
    | BigQueryAdapterConfig
    | DbtAdapterConfig
    | FivetranAdapterConfig
    | LookerAdapterConfig
    | MetabaseAdapterConfig
    | PostgresAdapterConfig
    | SnowflakeAdapterConfig
)
type ConfigBuilder = Callable[[SourceConfig], AdapterConfig]


def _resolve_env(secret: object) -> str:
    provider = getattr(secret, "provider", "env")
    ref = getattr(secret, "reference", None)
    if provider == "literal":
        return ref or ""
    return os.environ.get(ref, "") if ref else ""


def _observation_cursor(source: SourceConfig) -> dict[str, object] | None:
    cursor = source.params.get("observation_cursor")
    return dict(cursor) if isinstance(cursor, dict) else None


def _literal_secret(value: object) -> SourceAdapterSecret:
    return ExternalSecretRef(provider="literal", reference=str(value))


def _env_secret(reference: object) -> SourceAdapterSecret:
    return ExternalSecretRef(provider="env", reference=str(reference))


def _optional_secret(
    params: dict[str, Any],
    *,
    literal_key: str,
    env_key: str,
) -> SourceAdapterSecret | None:
    if params.get(literal_key) is not None:
        return _literal_secret(params[literal_key])
    if params.get(env_key) is not None:
        return _env_secret(params[env_key])
    return None


def _required_secret(
    params: dict[str, Any],
    *,
    literal_key: str,
    env_key: str,
    error_message: str,
) -> SourceAdapterSecret:
    secret = _optional_secret(params, literal_key=literal_key, env_key=env_key)
    if secret is None:
        raise ValueError(error_message)
    return secret


def _build_bigquery_config(source: SourceConfig) -> BigQueryAdapterConfig:
    params = source.params
    if "credentials" in params and "service_account_env" in params:
        raise ValueError("bigquery sources may set only one of 'credentials' or 'service_account_env'")
    if "credentials" in params:
        credentials_path = Path(str(params["credentials"])).expanduser()
        if not credentials_path.exists():
            raise ValueError(f"BigQuery credentials file not found: {credentials_path}")
        service_account_secret = _literal_secret(credentials_path.read_text(encoding="utf-8"))
    else:
        service_account_secret = _env_secret(params.get("service_account_env", DEFAULT_BIGQUERY_SERVICE_ACCOUNT_ENV))
    return BigQueryAdapterConfig(
        service_account_secret=service_account_secret,
        project_id=params.get("project_id") or params["project"],
        location=params.get("location", DEFAULT_BIGQUERY_LOCATION),
        lookback_hours=int(params.get("lookback_hours", DEFAULT_BIGQUERY_LOOKBACK_HOURS)),
        max_job_rows=int(params.get("max_job_rows", DEFAULT_BIGQUERY_MAX_JOB_ROWS)),
        max_column_rows=int(params.get("max_column_rows", DEFAULT_BIGQUERY_MAX_COLUMN_ROWS)),
        probe_target=params.get("probe_target"),
    )


def _build_postgres_config(source: SourceConfig) -> PostgresAdapterConfig:
    params = source.params
    database_secret = _required_secret(
        params,
        literal_key="dsn",
        env_key="dsn_env",
        error_message="postgres sources require either 'dsn' or 'dsn_env'",
    )
    if "include_schemas" in params:
        raw_schemas = params["include_schemas"]
        include_schemas = tuple(raw_schemas) if isinstance(raw_schemas, (list, tuple)) else (raw_schemas,)
    elif "schema" in params:
        include_schemas = (params["schema"],)
    else:
        include_schemas = (DEFAULT_POSTGRES_SCHEMA,)
    log_capture_raw = params.get("log_capture")
    log_capture = None
    if isinstance(log_capture_raw, dict):
        log_capture = PostgresLogCaptureConfig(
            log_path=log_capture_raw["log_path"],
            default_source=log_capture_raw.get("default_source"),
            default_database_name=log_capture_raw.get("default_database_name"),
            default_database_user=log_capture_raw.get("default_database_user"),
        )
    read_replica_raw = params.get("read_replica")
    read_replica = None
    if isinstance(read_replica_raw, dict):
        read_replica = PostgresReadReplicaConfig(
            database_secret=_optional_secret(read_replica_raw, literal_key="dsn", env_key="dsn_env"),
            host=read_replica_raw.get("host"),
            port=read_replica_raw.get("port"),
            expected_lag_seconds=int(read_replica_raw.get("expected_lag_seconds", 0)),
        )
    return PostgresAdapterConfig(
        database_secret=database_secret,
        include_schemas=include_schemas,
        exclude_schemas=tuple(params.get("exclude_schemas", DEFAULT_POSTGRES_EXCLUDE_SCHEMAS)),
        log_capture=log_capture,
        probe_target=params.get("probe_target"),
        read_replica=read_replica,
    )


def _build_dbt_config(source: SourceConfig) -> DbtAdapterConfig:
    params = source.params
    return DbtAdapterConfig(
        manifest_path=params.get("manifest_path", ""),
        catalog_path=params.get("catalog_path"),
        run_results_path=params.get("run_results_path"),
        project_name=params.get("project_name"),
    )


def _build_snowflake_config(source: SourceConfig) -> SnowflakeAdapterConfig:
    params = source.params
    account_secret_env = params.get("account_secret_env")
    if not isinstance(account_secret_env, str) or not account_secret_env:
        raise ValueError("snowflake sources require 'account_secret_env'")
    return SnowflakeAdapterConfig(
        account_secret=_env_secret(account_secret_env),
        account=params.get("account", ""),
        warehouse=params.get("warehouse", DEFAULT_SNOWFLAKE_WAREHOUSE),
        database=params.get("database", ""),
        role=params.get("role", ""),
        include_schemas=tuple(params.get("include_schemas", ())),
        exclude_schemas=tuple(params.get("exclude_schemas", DEFAULT_SNOWFLAKE_EXCLUDE_SCHEMAS)),
        lookback_hours=int(params.get("lookback_hours", DEFAULT_SNOWFLAKE_LOOKBACK_HOURS)),
        max_query_rows=int(params.get("max_query_rows", DEFAULT_SNOWFLAKE_MAX_QUERY_ROWS)),
        probe_target=params.get("probe_target"),
    )


def _build_airflow_config(source: SourceConfig) -> AirflowAdapterConfig:
    params = source.params
    auth_token_secret = _optional_secret(params, literal_key="auth_token", env_key="auth_token_env")
    if auth_token_secret is None and not (params.get("username") and params.get("password")):
        raise ValueError("airflow sources require 'auth_token', 'auth_token_env', or username/password")
    return AirflowAdapterConfig(
        base_url=params.get("base_url", ""),
        auth_token_secret=auth_token_secret,
        username=params.get("username"),
        password_secret=_optional_secret(params, literal_key="password", env_key="password_env"),
    )


def _build_looker_config(source: SourceConfig) -> LookerAdapterConfig:
    params = source.params
    client_id = _required_secret(
        params,
        literal_key="client_id",
        env_key="client_id_env",
        error_message=(
            "looker sources require client credentials via 'client_id'/'client_secret' "
            "or explicit 'client_id_env'/'client_secret_env'"
        ),
    )
    client_secret = _required_secret(
        params,
        literal_key="client_secret",
        env_key="client_secret_env",
        error_message=(
            "looker sources require client credentials via 'client_id'/'client_secret' "
            "or explicit 'client_id_env'/'client_secret_env'"
        ),
    )
    return LookerAdapterConfig(
        instance_url=params.get("instance_url", ""),
        client_id=client_id,
        client_secret=client_secret,
        port=int(params.get("port", DEFAULT_LOOKER_PORT)),
    )


def _build_fivetran_config(source: SourceConfig) -> FivetranAdapterConfig:
    params = source.params
    api_key = _required_secret(
        params,
        literal_key="api_key",
        env_key="api_key_env",
        error_message=(
            "fivetran sources require API credentials via 'api_key'/'api_secret' "
            "or explicit 'api_key_env'/'api_secret_env'"
        ),
    )
    api_secret = _required_secret(
        params,
        literal_key="api_secret",
        env_key="api_secret_env",
        error_message=(
            "fivetran sources require API credentials via 'api_key'/'api_secret' "
            "or explicit 'api_key_env'/'api_secret_env'"
        ),
    )
    return FivetranAdapterConfig(api_key=api_key, api_secret=api_secret)


def _build_metabase_config(source: SourceConfig) -> MetabaseAdapterConfig:
    params = source.params
    api_key = _optional_secret(params, literal_key="api_key", env_key="api_key_env")
    if api_key is None and not (params.get("username") and params.get("password")):
        raise ValueError("metabase sources require 'api_key', 'api_key_env', or username/password")
    return MetabaseAdapterConfig(
        instance_url=params.get("instance_url", ""),
        api_key=api_key,
        username=params.get("username"),
        password=_optional_secret(params, literal_key="password", env_key="password_env"),
    )


_CONFIG_BUILDERS: dict[str, tuple[SourceAdapterKind, ConfigBuilder]] = {
    "bigquery": (SourceAdapterKind.BIGQUERY, _build_bigquery_config),
    "postgres": (SourceAdapterKind.POSTGRES, _build_postgres_config),
    "dbt": (SourceAdapterKind.DBT, _build_dbt_config),
    "snowflake": (SourceAdapterKind.SNOWFLAKE, _build_snowflake_config),
    "airflow": (SourceAdapterKind.AIRFLOW, _build_airflow_config),
    "looker": (SourceAdapterKind.LOOKER, _build_looker_config),
    "fivetran": (SourceAdapterKind.FIVETRAN, _build_fivetran_config),
    "metabase": (SourceAdapterKind.METABASE, _build_metabase_config),
}


def _make_persisted_adapter(
    source: SourceConfig,
    *,
    adapter_kind: SourceAdapterKind,
    config: AdapterConfig,
) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, source.id)),
        key=source.id.replace(":", "-"),
        display_name=source.id,
        kind=adapter_kind,
        target_id=source.id,
        status=SourceAdapterStatus.READY,
        config=config,
        observation_cursor=_observation_cursor(source),
    )


def build_runtime_adapter(source: SourceConfig) -> tuple[RuntimeSourceAdapter, PersistedSourceAdapter]:
    """Construct both the runtime adapter and canonical persisted adapter record."""
    ensure_source_params_allowed(source)
    try:
        adapter_kind, config_builder = _CONFIG_BUILDERS[source.kind]
    except KeyError as exc:
        supported = ", ".join(sorted(SUPPORTED_SOURCE_KINDS))
        raise ValueError(f"Unknown source kind: {source.kind!r}. Supported: {supported}") from exc
    persisted = _make_persisted_adapter(source, adapter_kind=adapter_kind, config=config_builder(source))
    return instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env), persisted
