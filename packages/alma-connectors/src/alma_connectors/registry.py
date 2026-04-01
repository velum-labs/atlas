"""Canonical connector registry for source-kind semantics and runtime wiring."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from alma_connectors.adapters.airflow import AirflowAdapter
from alma_connectors.adapters.bigquery import BigQueryAdapter
from alma_connectors.adapters.dbt import DbtAdapter
from alma_connectors.adapters.fivetran import FivetranAdapter
from alma_connectors.adapters.looker import LookerAdapter
from alma_connectors.adapters.metabase import MetabaseAdapter
from alma_connectors.adapters.postgres import PostgresAdapter
from alma_connectors.adapters.snowflake import SnowflakeAdapter
from alma_connectors.source_adapter import (
    AirflowAdapterConfig,
    BigQueryAdapterConfig,
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
    SetupInstructions,
    SnowflakeAdapterConfig,
    SourceAdapterConfig,
    SourceAdapterDefinition,
    SourceAdapterKind,
    SourceAdapterStatus,
)

DEFAULT_BIGQUERY_LOCATION = "us"
DEFAULT_BIGQUERY_SERVICE_ACCOUNT_ENV = "BQ_SERVICE_ACCOUNT_JSON"
DEFAULT_BIGQUERY_LOOKBACK_HOURS = 24
DEFAULT_BIGQUERY_MAX_JOB_ROWS = 10_000
DEFAULT_BIGQUERY_MAX_COLUMN_ROWS = 20_000
DEFAULT_BIGQUERY_DEFAULT_JOB_TIMEOUT_MS = 300_000

DEFAULT_POSTGRES_SCHEMA = "public"
DEFAULT_POSTGRES_INCLUDE_SCHEMAS = (DEFAULT_POSTGRES_SCHEMA,)
DEFAULT_POSTGRES_EXCLUDE_SCHEMAS = ("pg_catalog", "information_schema")

DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV = "SNOWFLAKE_CONNECTION_JSON"
DEFAULT_SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
DEFAULT_SNOWFLAKE_EXCLUDE_SCHEMAS = ("INFORMATION_SCHEMA",)
DEFAULT_SNOWFLAKE_LOOKBACK_HOURS = 168
DEFAULT_SNOWFLAKE_MAX_QUERY_ROWS = 10_000

DEFAULT_AIRFLOW_AUTH_TOKEN_ENV = "AIRFLOW_AUTH_TOKEN"
DEFAULT_LOOKER_CLIENT_ID_ENV = "LOOKER_CLIENT_ID"
DEFAULT_LOOKER_CLIENT_SECRET_ENV = "LOOKER_CLIENT_SECRET"
DEFAULT_LOOKER_PORT = 19999
DEFAULT_FIVETRAN_API_KEY_ENV = "FIVETRAN_API_KEY"
DEFAULT_FIVETRAN_API_SECRET_ENV = "FIVETRAN_API_SECRET"

type SecretResolver = Callable[[ManagedSecret | ExternalSecretRef], str]
type SerializeSecret = Callable[[ManagedSecret | ExternalSecretRef], dict[str, Any]]
type DeserializeSecret = Callable[[dict[str, Any]], ManagedSecret | ExternalSecretRef]
type RuntimeSourceAdapter = object
type RuntimeFactory = Callable[[SourceAdapterConfig, SecretResolver], RuntimeSourceAdapter]
type RawConfigBuilder = Callable[[dict[str, Any]], SourceAdapterConfig]
type EncodeDefinition = Callable[
    [SourceAdapterDefinition, SerializeSecret],
    tuple[dict[str, Any], dict[str, dict[str, Any]]],
]
type DecodeConfig = Callable[
    [dict[str, Any], dict[str, Any], DeserializeSecret],
    SourceAdapterConfig,
]
type SetupInstructionsFactory = Callable[[], SetupInstructions]


@dataclass(frozen=True)
class ConnectorSpec:
    """Canonical definition for one supported connector kind."""

    kind: str
    adapter_kind: SourceAdapterKind
    allowed_params: frozenset[str]
    secret_paths: tuple[tuple[str, ...], ...]
    build_config: RawConfigBuilder
    runtime_factory: RuntimeFactory
    encode_definition: EncodeDefinition
    decode_config: DecodeConfig
    setup_instructions_factory: SetupInstructionsFactory


def _literal_secret(value: object) -> ExternalSecretRef:
    return ExternalSecretRef(provider="literal", reference=str(value))


def _env_secret(reference: object) -> ExternalSecretRef:
    return ExternalSecretRef(provider="env", reference=str(reference))


def _optional_secret(
    params: dict[str, Any],
    *,
    literal_key: str,
    env_key: str,
) -> ExternalSecretRef | None:
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
) -> ExternalSecretRef:
    secret = _optional_secret(params, literal_key=literal_key, env_key=env_key)
    if secret is None:
        raise ValueError(error_message)
    return secret


def _normalize_schema_tuple(value: object, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        return default
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    return (str(value),)


def _coerce_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"expected a boolean-compatible value, got {value!r}")


def _require_config_type[T](definition: SourceAdapterDefinition, config_type: type[T], *, kind: str) -> T:
    config = definition.config
    if not isinstance(config, config_type):
        raise ValueError(f"{kind} adapter definition requires {kind} config")
    return config


def _resolve_optional_secret(
    secret: ManagedSecret | ExternalSecretRef | None,
    *,
    resolve_secret: SecretResolver,
) -> str | None:
    if secret is None:
        return None
    return resolve_secret(secret)


def _build_bigquery_config(params: dict[str, Any]) -> BigQueryAdapterConfig:
    if "credentials" in params and "service_account_env" in params:
        raise ValueError("bigquery sources may set only one of 'credentials' or 'service_account_env'")
    if "credentials" in params:
        credentials_path = Path(str(params["credentials"])).expanduser()
        if not credentials_path.exists():
            raise ValueError(f"BigQuery credentials file not found: {credentials_path}")
        service_account_secret = _literal_secret(credentials_path.read_text(encoding="utf-8"))
    elif "service_account_env" in params:
        service_account_secret = _env_secret(params["service_account_env"])
    else:
        service_account_secret = None
    return BigQueryAdapterConfig(
        project_id=params.get("project_id") or params["project"],
        service_account_secret=service_account_secret,
        location=params.get("location", DEFAULT_BIGQUERY_LOCATION),
        lookback_hours=int(params.get("lookback_hours", DEFAULT_BIGQUERY_LOOKBACK_HOURS)),
        max_job_rows=int(params.get("max_job_rows", DEFAULT_BIGQUERY_MAX_JOB_ROWS)),
        max_column_rows=int(params.get("max_column_rows", DEFAULT_BIGQUERY_MAX_COLUMN_ROWS)),
        maximum_bytes_billed=(
            int(params["maximum_bytes_billed"])
            if params.get("maximum_bytes_billed") is not None
            else None
        ),
        default_job_timeout_ms=int(
            params.get("default_job_timeout_ms", DEFAULT_BIGQUERY_DEFAULT_JOB_TIMEOUT_MS)
        ),
        include_job_cost_stats=_coerce_bool(
            params.get("include_job_cost_stats"),
            default=True,
        ),
        probe_target=params.get("probe_target"),
    )


def _build_postgres_config(params: dict[str, Any]) -> PostgresAdapterConfig:
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
        include_schemas = DEFAULT_POSTGRES_INCLUDE_SCHEMAS

    log_capture = None
    log_capture_raw = params.get("log_capture")
    if isinstance(log_capture_raw, dict):
        log_capture = PostgresLogCaptureConfig(
            log_path=log_capture_raw["log_path"],
            default_source=log_capture_raw.get("default_source"),
            default_database_name=log_capture_raw.get("default_database_name"),
            default_database_user=log_capture_raw.get("default_database_user"),
        )

    read_replica = None
    read_replica_raw = params.get("read_replica")
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
        exclude_schemas=_normalize_schema_tuple(
            params.get("exclude_schemas"),
            default=DEFAULT_POSTGRES_EXCLUDE_SCHEMAS,
        ),
        log_capture=log_capture,
        probe_target=params.get("probe_target"),
        read_replica=read_replica,
    )


def _build_dbt_config(params: dict[str, Any]) -> DbtAdapterConfig:
    return DbtAdapterConfig(
        manifest_path=str(params.get("manifest_path", "")),
        catalog_path=params.get("catalog_path"),
        run_results_path=params.get("run_results_path"),
        project_name=params.get("project_name"),
    )


def _build_snowflake_config(params: dict[str, Any]) -> SnowflakeAdapterConfig:
    account_secret_env = params.get("account_secret_env", DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV)
    if not isinstance(account_secret_env, str) or not account_secret_env:
        raise ValueError("snowflake sources require a non-empty 'account_secret_env'")
    return SnowflakeAdapterConfig(
        account_secret=_env_secret(account_secret_env),
        account=params.get("account", ""),
        warehouse=params.get("warehouse", DEFAULT_SNOWFLAKE_WAREHOUSE),
        database=params.get("database", ""),
        role=params.get("role", ""),
        include_schemas=_normalize_schema_tuple(params.get("include_schemas"), default=()),
        exclude_schemas=_normalize_schema_tuple(
            params.get("exclude_schemas"),
            default=DEFAULT_SNOWFLAKE_EXCLUDE_SCHEMAS,
        ),
        lookback_hours=int(params.get("lookback_hours", DEFAULT_SNOWFLAKE_LOOKBACK_HOURS)),
        max_query_rows=int(params.get("max_query_rows", DEFAULT_SNOWFLAKE_MAX_QUERY_ROWS)),
        probe_target=params.get("probe_target"),
    )


def _build_airflow_config(params: dict[str, Any]) -> AirflowAdapterConfig:
    has_basic_auth = bool(params.get("username")) and bool(params.get("password") or params.get("password_env"))
    auth_token_secret = _optional_secret(params, literal_key="auth_token", env_key="auth_token_env")
    if auth_token_secret is None and not has_basic_auth:
        auth_token_secret = _env_secret(DEFAULT_AIRFLOW_AUTH_TOKEN_ENV)
    if auth_token_secret is None and not has_basic_auth:
        raise ValueError("airflow sources require 'auth_token', 'auth_token_env', or username/password")
    return AirflowAdapterConfig(
        base_url=params.get("base_url", ""),
        auth_token_secret=auth_token_secret,
        username=params.get("username"),
        password_secret=_optional_secret(params, literal_key="password", env_key="password_env"),
    )


def _build_looker_config(params: dict[str, Any]) -> LookerAdapterConfig:
    client_id = _optional_secret(params, literal_key="client_id", env_key="client_id_env") or _env_secret(
        DEFAULT_LOOKER_CLIENT_ID_ENV
    )
    client_secret = _optional_secret(params, literal_key="client_secret", env_key="client_secret_env") or _env_secret(
        DEFAULT_LOOKER_CLIENT_SECRET_ENV
    )
    return LookerAdapterConfig(
        instance_url=params.get("instance_url", ""),
        client_id=client_id,
        client_secret=client_secret,
        port=int(params.get("port", DEFAULT_LOOKER_PORT)),
    )


def _build_fivetran_config(params: dict[str, Any]) -> FivetranAdapterConfig:
    api_key = _optional_secret(params, literal_key="api_key", env_key="api_key_env") or _env_secret(
        DEFAULT_FIVETRAN_API_KEY_ENV
    )
    api_secret = _optional_secret(params, literal_key="api_secret", env_key="api_secret_env") or _env_secret(
        DEFAULT_FIVETRAN_API_SECRET_ENV
    )
    return FivetranAdapterConfig(api_key=api_key, api_secret=api_secret)


def _build_metabase_config(params: dict[str, Any]) -> MetabaseAdapterConfig:
    api_key = _optional_secret(params, literal_key="api_key", env_key="api_key_env")
    has_basic_auth = bool(params.get("username")) and bool(params.get("password") or params.get("password_env"))
    if api_key is None and not has_basic_auth:
        raise ValueError("metabase sources require 'api_key', 'api_key_env', or username/password")
    return MetabaseAdapterConfig(
        instance_url=params.get("instance_url", ""),
        api_key=api_key,
        username=params.get("username"),
        password=_optional_secret(params, literal_key="password", env_key="password_env"),
    )


def _build_postgres_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    assert isinstance(config, PostgresAdapterConfig)
    return PostgresAdapter(resolve_secret=resolve_secret)


def _build_bigquery_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    assert isinstance(config, BigQueryAdapterConfig)
    return BigQueryAdapter(resolve_secret=resolve_secret)


def _build_dbt_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    del resolve_secret
    assert isinstance(config, DbtAdapterConfig)
    return DbtAdapter(
        manifest_path=config.manifest_path,
        catalog_path=config.catalog_path,
        run_results_path=config.run_results_path,
        project_name=config.project_name,
    )


def _build_snowflake_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    assert isinstance(config, SnowflakeAdapterConfig)
    return SnowflakeAdapter(resolve_secret=resolve_secret)


def _build_airflow_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    assert isinstance(config, AirflowAdapterConfig)
    return AirflowAdapter(
        base_url=config.base_url,
        auth_token=_resolve_optional_secret(config.auth_token_secret, resolve_secret=resolve_secret),
        username=config.username,
        password=_resolve_optional_secret(config.password_secret, resolve_secret=resolve_secret),
        timeout_seconds=config.timeout_seconds,
    )


def _build_looker_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    assert isinstance(config, LookerAdapterConfig)
    return LookerAdapter(
        instance_url=config.instance_url,
        client_id=resolve_secret(config.client_id),
        client_secret=resolve_secret(config.client_secret),
        port=config.port,
        timeout_seconds=config.timeout_seconds,
    )


def _build_fivetran_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    assert isinstance(config, FivetranAdapterConfig)
    return FivetranAdapter(
        api_key=resolve_secret(config.api_key),
        api_secret=resolve_secret(config.api_secret),
        api_base=config.api_base,
        timeout_seconds=config.timeout_seconds,
    )


def _build_metabase_runtime(config: SourceAdapterConfig, resolve_secret: SecretResolver) -> RuntimeSourceAdapter:
    assert isinstance(config, MetabaseAdapterConfig)
    return MetabaseAdapter(
        instance_url=config.instance_url,
        api_key=_resolve_optional_secret(config.api_key, resolve_secret=resolve_secret),
        username=config.username,
        password=_resolve_optional_secret(config.password, resolve_secret=resolve_secret),
        timeout_seconds=config.timeout_seconds,
    )


def _encode_postgres_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = _require_config_type(definition, PostgresAdapterConfig, kind="postgres")
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
        "database_secret": serialize_secret(config.database_secret),
    }
    if config.read_replica is not None and config.read_replica.database_secret is not None:
        secrets["read_replica_database_secret"] = serialize_secret(config.read_replica.database_secret)
    return payload, secrets


def _encode_bigquery_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = _require_config_type(definition, BigQueryAdapterConfig, kind="bigquery")
    payload = {
        "project_id": config.project_id,
        "location": config.location,
        "lookback_hours": config.lookback_hours,
        "max_job_rows": config.max_job_rows,
        "max_column_rows": config.max_column_rows,
        "maximum_bytes_billed": config.maximum_bytes_billed,
        "default_job_timeout_ms": config.default_job_timeout_ms,
        "include_job_cost_stats": config.include_job_cost_stats,
        "probe_target": config.probe_target,
    }
    secrets = (
        {"service_account_secret": serialize_secret(config.service_account_secret)}
        if config.service_account_secret is not None
        else {}
    )
    return payload, secrets


def _encode_snowflake_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = _require_config_type(definition, SnowflakeAdapterConfig, kind="snowflake")
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
    secrets = {"account_secret": serialize_secret(config.account_secret)}
    return payload, secrets


def _encode_dbt_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    del serialize_secret
    config = _require_config_type(definition, DbtAdapterConfig, kind="dbt")
    return (
        {
            "manifest_path": config.manifest_path,
            "catalog_path": config.catalog_path,
            "run_results_path": config.run_results_path,
            "project_name": config.project_name,
        },
        {},
    )


def _encode_airflow_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = _require_config_type(definition, AirflowAdapterConfig, kind="airflow")
    payload = {
        "base_url": config.base_url,
        "username": config.username,
        "timeout_seconds": config.timeout_seconds,
    }
    secrets: dict[str, dict[str, Any]] = {}
    if config.auth_token_secret is not None:
        secrets["auth_token_secret"] = serialize_secret(config.auth_token_secret)
    if config.password_secret is not None:
        secrets["password_secret"] = serialize_secret(config.password_secret)
    return payload, secrets


def _encode_looker_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = _require_config_type(definition, LookerAdapterConfig, kind="looker")
    return (
        {
            "instance_url": config.instance_url,
            "port": config.port,
            "timeout_seconds": config.timeout_seconds,
        },
        {
            "client_id": serialize_secret(config.client_id),
            "client_secret": serialize_secret(config.client_secret),
        },
    )


def _encode_fivetran_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = _require_config_type(definition, FivetranAdapterConfig, kind="fivetran")
    return (
        {
            "api_base": config.api_base,
            "timeout_seconds": config.timeout_seconds,
        },
        {
            "api_key": serialize_secret(config.api_key),
            "api_secret": serialize_secret(config.api_secret),
        },
    )


def _encode_metabase_definition(
    definition: SourceAdapterDefinition,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    config = _require_config_type(definition, MetabaseAdapterConfig, kind="metabase")
    payload = {
        "instance_url": config.instance_url,
        "username": config.username,
        "timeout_seconds": config.timeout_seconds,
    }
    secrets: dict[str, dict[str, Any]] = {}
    if config.api_key is not None:
        secrets["api_key"] = serialize_secret(config.api_key)
    if config.password is not None:
        secrets["password"] = serialize_secret(config.password)
    return payload, secrets


def _decode_postgres_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    log_capture_payload = config.get("log_capture")
    log_capture = None
    if log_capture_payload:
        log_capture = PostgresLogCaptureConfig(
            log_path=str(log_capture_payload.get("log_path", "")),
            default_source=log_capture_payload.get("default_source"),
            default_database_name=log_capture_payload.get("default_database_name"),
            default_database_user=log_capture_payload.get("default_database_user"),
        )
    read_replica_payload = config.get("read_replica")
    read_replica = None
    if read_replica_payload or secrets.get("read_replica_database_secret"):
        read_replica_data = dict(read_replica_payload or {})
        read_replica_secret_payload = secrets.get("read_replica_database_secret")
        read_replica = PostgresReadReplicaConfig(
            database_secret=(
                deserialize_secret(dict(read_replica_secret_payload or {}))
                if read_replica_secret_payload is not None
                else None
            ),
            host=read_replica_data.get("host"),
            port=(int(read_replica_data["port"]) if read_replica_data.get("port") is not None else None),
            expected_lag_seconds=int(read_replica_data.get("expected_lag_seconds") or 0),
        )
    return PostgresAdapterConfig(
        database_secret=deserialize_secret(dict(secrets.get("database_secret") or {})),
        include_schemas=tuple(
            str(item)
            for item in (
                DEFAULT_POSTGRES_INCLUDE_SCHEMAS
                if config.get("include_schemas") is None
                else tuple(config.get("include_schemas") or [])
            )
        ),
        exclude_schemas=tuple(
            str(item)
            for item in (
                DEFAULT_POSTGRES_EXCLUDE_SCHEMAS
                if config.get("exclude_schemas") is None
                else tuple(config.get("exclude_schemas") or [])
            )
        ),
        log_capture=log_capture,
        probe_target=(str(config["probe_target"]) if config.get("probe_target") is not None else None),
        read_replica=read_replica,
    )


def _decode_bigquery_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    service_account_secret = secrets.get("service_account_secret")
    return BigQueryAdapterConfig(
        project_id=str(config.get("project_id", "")),
        service_account_secret=(
            deserialize_secret(dict(service_account_secret or {})) if service_account_secret is not None else None
        ),
        location=str(config.get("location") or DEFAULT_BIGQUERY_LOCATION),
        lookback_hours=int(config.get("lookback_hours") or DEFAULT_BIGQUERY_LOOKBACK_HOURS),
        max_job_rows=int(config.get("max_job_rows") or DEFAULT_BIGQUERY_MAX_JOB_ROWS),
        max_column_rows=int(config.get("max_column_rows") or DEFAULT_BIGQUERY_MAX_COLUMN_ROWS),
        maximum_bytes_billed=(
            int(config["maximum_bytes_billed"])
            if config.get("maximum_bytes_billed") is not None
            else None
        ),
        default_job_timeout_ms=int(
            config.get("default_job_timeout_ms") or DEFAULT_BIGQUERY_DEFAULT_JOB_TIMEOUT_MS
        ),
        include_job_cost_stats=_coerce_bool(config.get("include_job_cost_stats"), default=True),
        probe_target=(str(config["probe_target"]) if config.get("probe_target") is not None else None),
    )


def _decode_snowflake_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    return SnowflakeAdapterConfig(
        account_secret=deserialize_secret(dict(secrets.get("account_secret") or {})),
        account=str(config.get("account", "")),
        warehouse=str(config.get("warehouse") or DEFAULT_SNOWFLAKE_WAREHOUSE),
        database=str(config.get("database") or ""),
        role=str(config.get("role") or ""),
        include_schemas=tuple(str(item) for item in (config.get("include_schemas") or [])),
        exclude_schemas=tuple(
            str(item) for item in (config.get("exclude_schemas") or DEFAULT_SNOWFLAKE_EXCLUDE_SCHEMAS)
        ),
        lookback_hours=int(config.get("lookback_hours") or DEFAULT_SNOWFLAKE_LOOKBACK_HOURS),
        max_query_rows=int(config.get("max_query_rows") or DEFAULT_SNOWFLAKE_MAX_QUERY_ROWS),
        probe_target=(str(config["probe_target"]) if config.get("probe_target") is not None else None),
    )


def _decode_dbt_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    del secrets, deserialize_secret
    return DbtAdapterConfig(
        manifest_path=str(config.get("manifest_path", "")),
        catalog_path=(str(config["catalog_path"]) if config.get("catalog_path") is not None else None),
        run_results_path=(str(config["run_results_path"]) if config.get("run_results_path") is not None else None),
        project_name=(str(config["project_name"]) if config.get("project_name") is not None else None),
    )


def _decode_airflow_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    auth_token_secret = secrets.get("auth_token_secret")
    password_secret = secrets.get("password_secret")
    return AirflowAdapterConfig(
        base_url=str(config.get("base_url", "")),
        auth_token_secret=(
            deserialize_secret(dict(auth_token_secret or {})) if auth_token_secret is not None else None
        ),
        username=(str(config["username"]) if config.get("username") is not None else None),
        password_secret=(deserialize_secret(dict(password_secret or {})) if password_secret is not None else None),
        timeout_seconds=int(config.get("timeout_seconds") or 30),
    )


def _decode_looker_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    return LookerAdapterConfig(
        instance_url=str(config.get("instance_url", "")),
        client_id=deserialize_secret(dict(secrets.get("client_id") or {})),
        client_secret=deserialize_secret(dict(secrets.get("client_secret") or {})),
        port=int(config.get("port") or DEFAULT_LOOKER_PORT),
        timeout_seconds=int(config.get("timeout_seconds") or 30),
    )


def _decode_fivetran_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    return FivetranAdapterConfig(
        api_key=deserialize_secret(dict(secrets.get("api_key") or {})),
        api_secret=deserialize_secret(dict(secrets.get("api_secret") or {})),
        api_base=str(config.get("api_base") or "https://api.fivetran.com"),
        timeout_seconds=int(config.get("timeout_seconds") or 30),
    )


def _decode_metabase_config(
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    api_key_secret = secrets.get("api_key")
    password_secret = secrets.get("password")
    return MetabaseAdapterConfig(
        instance_url=str(config.get("instance_url", "")),
        api_key=(deserialize_secret(dict(api_key_secret or {})) if api_key_secret is not None else None),
        username=(str(config["username"]) if config.get("username") is not None else None),
        password=(deserialize_secret(dict(password_secret or {})) if password_secret is not None else None),
        timeout_seconds=int(config.get("timeout_seconds") or 30),
    )


def _postgres_setup_instructions() -> SetupInstructions:
    return PostgresAdapter(resolve_secret=lambda secret: "").get_setup_instructions()


def _bigquery_setup_instructions() -> SetupInstructions:
    return BigQueryAdapter(resolve_secret=lambda secret: "").get_setup_instructions()


def _snowflake_setup_instructions() -> SetupInstructions:
    return SnowflakeAdapter(resolve_secret=lambda secret: "").get_setup_instructions()


def _dbt_setup_instructions() -> SetupInstructions:
    return DbtAdapter(manifest_path="placeholder/manifest.json").get_setup_instructions()


def _airflow_setup_instructions() -> SetupInstructions:
    return AirflowAdapter(base_url="https://airflow.example.com", auth_token="token").get_setup_instructions()


def _looker_setup_instructions() -> SetupInstructions:
    return LookerAdapter(
        instance_url="https://looker.example.com",
        client_id="client-id",
        client_secret="client-secret",
    ).get_setup_instructions()


def _fivetran_setup_instructions() -> SetupInstructions:
    return FivetranAdapter(api_key="api-key", api_secret="api-secret").get_setup_instructions()


def _metabase_setup_instructions() -> SetupInstructions:
    return MetabaseAdapter(instance_url="https://metabase.example.com", api_key="api-key").get_setup_instructions()


CONNECTOR_SPECS: dict[str, ConnectorSpec] = {
    "bigquery": ConnectorSpec(
        kind="bigquery",
        adapter_kind=SourceAdapterKind.BIGQUERY,
        allowed_params=frozenset(
            {
                "credentials",
                "lookback_hours",
                "location",
                "max_column_rows",
                "max_job_rows",
                "maximum_bytes_billed",
                "default_job_timeout_ms",
                "include_job_cost_stats",
                "observation_cursor",
                "probe_target",
                "project",
                "project_id",
                "service_account_env",
            }
        ),
        secret_paths=(),
        build_config=_build_bigquery_config,
        runtime_factory=_build_bigquery_runtime,
        encode_definition=_encode_bigquery_definition,
        decode_config=_decode_bigquery_config,
        setup_instructions_factory=_bigquery_setup_instructions,
    ),
    "postgres": ConnectorSpec(
        kind="postgres",
        adapter_kind=SourceAdapterKind.POSTGRES,
        allowed_params=frozenset(
            {
                "dsn",
                "dsn_env",
                "exclude_schemas",
                "include_schemas",
                "log_capture",
                "observation_cursor",
                "probe_target",
                "read_replica",
                "schema",
            }
        ),
        secret_paths=(("dsn",), ("read_replica", "dsn")),
        build_config=_build_postgres_config,
        runtime_factory=_build_postgres_runtime,
        encode_definition=_encode_postgres_definition,
        decode_config=_decode_postgres_config,
        setup_instructions_factory=_postgres_setup_instructions,
    ),
    "dbt": ConnectorSpec(
        kind="dbt",
        adapter_kind=SourceAdapterKind.DBT,
        allowed_params=frozenset(
            {
                "catalog_path",
                "manifest_path",
                "observation_cursor",
                "project_name",
                "run_results_path",
            }
        ),
        secret_paths=(),
        build_config=_build_dbt_config,
        runtime_factory=_build_dbt_runtime,
        encode_definition=_encode_dbt_definition,
        decode_config=_decode_dbt_config,
        setup_instructions_factory=_dbt_setup_instructions,
    ),
    "snowflake": ConnectorSpec(
        kind="snowflake",
        adapter_kind=SourceAdapterKind.SNOWFLAKE,
        allowed_params=frozenset(
            {
                "account",
                "account_secret_env",
                "database",
                "exclude_schemas",
                "include_schemas",
                "lookback_hours",
                "max_query_rows",
                "observation_cursor",
                "probe_target",
                "role",
                "warehouse",
            }
        ),
        secret_paths=(),
        build_config=_build_snowflake_config,
        runtime_factory=_build_snowflake_runtime,
        encode_definition=_encode_snowflake_definition,
        decode_config=_decode_snowflake_config,
        setup_instructions_factory=_snowflake_setup_instructions,
    ),
    "airflow": ConnectorSpec(
        kind="airflow",
        adapter_kind=SourceAdapterKind.AIRFLOW,
        allowed_params=frozenset(
            {
                "auth_token",
                "auth_token_env",
                "base_url",
                "observation_cursor",
                "password",
                "password_env",
                "username",
            }
        ),
        secret_paths=(("auth_token",), ("password",)),
        build_config=_build_airflow_config,
        runtime_factory=_build_airflow_runtime,
        encode_definition=_encode_airflow_definition,
        decode_config=_decode_airflow_config,
        setup_instructions_factory=_airflow_setup_instructions,
    ),
    "looker": ConnectorSpec(
        kind="looker",
        adapter_kind=SourceAdapterKind.LOOKER,
        allowed_params=frozenset(
            {
                "client_id",
                "client_id_env",
                "client_secret",
                "client_secret_env",
                "instance_url",
                "observation_cursor",
                "port",
            }
        ),
        secret_paths=(("client_id",), ("client_secret",)),
        build_config=_build_looker_config,
        runtime_factory=_build_looker_runtime,
        encode_definition=_encode_looker_definition,
        decode_config=_decode_looker_config,
        setup_instructions_factory=_looker_setup_instructions,
    ),
    "fivetran": ConnectorSpec(
        kind="fivetran",
        adapter_kind=SourceAdapterKind.FIVETRAN,
        allowed_params=frozenset(
            {
                "api_key",
                "api_key_env",
                "api_secret",
                "api_secret_env",
                "observation_cursor",
            }
        ),
        secret_paths=(("api_key",), ("api_secret",)),
        build_config=_build_fivetran_config,
        runtime_factory=_build_fivetran_runtime,
        encode_definition=_encode_fivetran_definition,
        decode_config=_decode_fivetran_config,
        setup_instructions_factory=_fivetran_setup_instructions,
    ),
    "metabase": ConnectorSpec(
        kind="metabase",
        adapter_kind=SourceAdapterKind.METABASE,
        allowed_params=frozenset(
            {
                "api_key",
                "api_key_env",
                "instance_url",
                "observation_cursor",
                "password",
                "password_env",
                "username",
            }
        ),
        secret_paths=(("api_key",), ("password",)),
        build_config=_build_metabase_config,
        runtime_factory=_build_metabase_runtime,
        encode_definition=_encode_metabase_definition,
        decode_config=_decode_metabase_config,
        setup_instructions_factory=_metabase_setup_instructions,
    ),
}


SUPPORTED_SOURCE_KINDS = frozenset(CONNECTOR_SPECS)


def get_connector_spec(kind: str | SourceAdapterKind) -> ConnectorSpec:
    key = kind.value if isinstance(kind, SourceAdapterKind) else kind
    try:
        return CONNECTOR_SPECS[key]
    except KeyError as exc:
        supported = ", ".join(sorted(SUPPORTED_SOURCE_KINDS))
        raise ValueError(f"Unknown source kind: {key!r}. Supported: {supported}") from exc


def build_adapter_definition(
    source_id: str,
    kind: str,
    params: dict[str, Any],
    *,
    display_name: str | None = None,
    description: str | None = None,
    metadata: Mapping[str, str] | None = None,
) -> SourceAdapterDefinition:
    spec = get_connector_spec(kind)
    config = spec.build_config(dict(params))
    return SourceAdapterDefinition(
        key=source_id.replace(":", "-"),
        display_name=display_name or source_id,
        kind=spec.adapter_kind,
        target_id=source_id,
        description=description,
        metadata=dict(metadata or {}),
        config=config,
    )


def build_persisted_adapter(
    source_id: str,
    kind: str,
    params: dict[str, Any],
    *,
    observation_cursor: dict[str, object] | None = None,
    display_name: str | None = None,
    description: str | None = None,
    metadata: Mapping[str, str] | None = None,
    adapter_id: str | None = None,
    status: SourceAdapterStatus = SourceAdapterStatus.READY,
    status_message: str | None = None,
    last_tested_at: Any = None,
    last_observed_at: Any = None,
    created_at: Any = None,
    updated_at: Any = None,
) -> PersistedSourceAdapter:
    definition = build_adapter_definition(
        source_id,
        kind,
        params,
        display_name=display_name,
        description=description,
        metadata=metadata,
    )
    return PersistedSourceAdapter(
        id=adapter_id or str(uuid.uuid5(uuid.NAMESPACE_URL, source_id)),
        key=definition.key,
        display_name=definition.display_name,
        kind=definition.kind,
        target_id=definition.target_id,
        description=definition.description,
        metadata=definition.metadata,
        config=definition.config,
        status=status,
        status_message=status_message,
        observation_cursor=observation_cursor,
        last_tested_at=last_tested_at,
        last_observed_at=last_observed_at,
        created_at=created_at,
        updated_at=updated_at,
    )


def instantiate_runtime_adapter(
    adapter: PersistedSourceAdapter,
    *,
    resolve_secret: SecretResolver,
) -> RuntimeSourceAdapter:
    spec = get_connector_spec(adapter.kind)
    return spec.runtime_factory(adapter.config, resolve_secret)


def serialize_definition(
    definition: SourceAdapterDefinition,
    *,
    serialize_secret: SerializeSecret,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    spec = get_connector_spec(definition.kind)
    return spec.encode_definition(definition, serialize_secret)


def deserialize_config(
    *,
    kind: str | SourceAdapterKind,
    config: dict[str, Any],
    secrets: dict[str, Any],
    deserialize_secret: DeserializeSecret,
) -> SourceAdapterConfig:
    spec = get_connector_spec(kind)
    return spec.decode_config(config, secrets, deserialize_secret)


def get_setup_instructions(kind: str | SourceAdapterKind) -> SetupInstructions:
    spec = get_connector_spec(kind)
    return spec.setup_instructions_factory()
