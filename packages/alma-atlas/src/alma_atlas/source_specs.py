"""Canonical source kind defaults, registration helpers, and param allowlists."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from alma_atlas.config import SourceConfig

DEFAULT_BIGQUERY_LOCATION = "us"
DEFAULT_BIGQUERY_SERVICE_ACCOUNT_ENV = "BQ_SERVICE_ACCOUNT_JSON"
DEFAULT_BIGQUERY_LOOKBACK_HOURS = 24
DEFAULT_BIGQUERY_MAX_JOB_ROWS = 10_000
DEFAULT_BIGQUERY_MAX_COLUMN_ROWS = 20_000

DEFAULT_POSTGRES_SCHEMA = "public"
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

SOURCE_ALLOWED_PARAMS: dict[str, frozenset[str]] = {
    "bigquery": frozenset(
        {
            "credentials",
            "lookback_hours",
            "location",
            "max_column_rows",
            "max_job_rows",
            "observation_cursor",
            "probe_target",
            "project",
            "project_id",
            "service_account_env",
        }
    ),
    "postgres": frozenset(
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
    "dbt": frozenset(
        {
            "catalog_path",
            "manifest_path",
            "observation_cursor",
            "project_name",
            "run_results_path",
        }
    ),
    "snowflake": frozenset(
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
    "airflow": frozenset(
        {
            "auth_token",
            "auth_token_env",
            "base_url",
            "observation_cursor",
            "password",
            "username",
        }
    ),
    "looker": frozenset(
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
    "fivetran": frozenset(
        {
            "api_key",
            "api_key_env",
            "api_secret",
            "api_secret_env",
            "observation_cursor",
        }
    ),
    "metabase": frozenset(
        {
            "api_key",
            "api_key_env",
            "instance_url",
            "observation_cursor",
            "password",
            "username",
        }
    ),
}

SUPPORTED_SOURCE_KINDS = frozenset(SOURCE_ALLOWED_PARAMS)


def source_slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or url
    return host.replace(".", "-")


def allowed_source_params(kind: str) -> frozenset[str]:
    try:
        return SOURCE_ALLOWED_PARAMS[kind]
    except KeyError as exc:
        raise ValueError(
            f"Unknown source kind: {kind!r}. Supported: {', '.join(sorted(SUPPORTED_SOURCE_KINDS))}"
        ) from exc


def ensure_source_params_allowed(source: SourceConfig) -> None:
    allowed = allowed_source_params(source.kind)
    unknown = set(source.params) - set(allowed)
    if unknown:
        raise ValueError(
            f"{source.kind} source {source.id!r} has unsupported param(s): {sorted(unknown)}. "
            f"Allowed params: {sorted(allowed)}"
        )


def make_bigquery_source(
    *,
    project: str,
    credentials: str | None = None,
    service_account_env: str | None = None,
    location: str = DEFAULT_BIGQUERY_LOCATION,
) -> SourceConfig:
    if credentials and service_account_env:
        raise ValueError("Use either credentials or service_account_env, not both")
    params: dict[str, str] = {"project_id": project, "location": location}
    if credentials:
        params["credentials"] = credentials
    if service_account_env:
        params["service_account_env"] = service_account_env
    return SourceConfig(id=f"bigquery:{project}", kind="bigquery", params=params)


def make_postgres_source(*, dsn: str, schema: str = DEFAULT_POSTGRES_SCHEMA) -> SourceConfig:
    db_name = dsn.rsplit("/", 1)[-1].split("?", 1)[0]
    source_id = f"postgres:{db_name}" if schema == DEFAULT_POSTGRES_SCHEMA else f"postgres:{db_name}:{schema}"
    return SourceConfig(
        id=source_id,
        kind="postgres",
        params={"dsn": dsn, "include_schemas": [schema]},
    )


def make_snowflake_source(
    *,
    account: str,
    account_secret_env: str = DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV,
    warehouse: str | None = None,
    database: str | None = None,
    role: str | None = None,
    schema: str | None = None,
) -> SourceConfig:
    params: dict[str, object] = {
        "account": account,
        "account_secret_env": account_secret_env,
    }
    if warehouse is not None:
        params["warehouse"] = warehouse
    if database is not None:
        params["database"] = database
    if role is not None:
        params["role"] = role
    if schema is not None:
        params["include_schemas"] = [schema]
    return SourceConfig(id=f"snowflake:{account}", kind="snowflake", params=params)


def make_dbt_source(
    *,
    manifest_path: str,
    project_name: str,
    catalog_path: str | None = None,
    run_results_path: str | None = None,
) -> SourceConfig:
    params: dict[str, object] = {
        "manifest_path": manifest_path,
        "project_name": project_name,
    }
    if catalog_path is not None:
        params["catalog_path"] = catalog_path
    if run_results_path is not None:
        params["run_results_path"] = run_results_path
    return SourceConfig(id=f"dbt:{project_name}", kind="dbt", params=params)


def make_airflow_source(
    *,
    base_url: str,
    auth_token_env: str | None = DEFAULT_AIRFLOW_AUTH_TOKEN_ENV,
    username: str | None = None,
    password: str | None = None,
) -> SourceConfig:
    params: dict[str, object] = {"base_url": base_url}
    if auth_token_env:
        params["auth_token_env"] = auth_token_env
    if username is not None:
        params["username"] = username
    if password is not None:
        params["password"] = password
    return SourceConfig(
        id=f"airflow:{source_slug_from_url(base_url)}",
        kind="airflow",
        params=params,
    )


def make_looker_source(
    *,
    instance_url: str,
    client_id_env: str = DEFAULT_LOOKER_CLIENT_ID_ENV,
    client_secret_env: str = DEFAULT_LOOKER_CLIENT_SECRET_ENV,
    port: int = DEFAULT_LOOKER_PORT,
) -> SourceConfig:
    return SourceConfig(
        id=f"looker:{source_slug_from_url(instance_url)}",
        kind="looker",
        params={
            "instance_url": instance_url,
            "client_id_env": client_id_env,
            "client_secret_env": client_secret_env,
            "port": port,
        },
    )


def make_fivetran_source(
    *,
    api_key_env: str = DEFAULT_FIVETRAN_API_KEY_ENV,
    api_secret_env: str = DEFAULT_FIVETRAN_API_SECRET_ENV,
) -> SourceConfig:
    return SourceConfig(
        id="fivetran:default",
        kind="fivetran",
        params={
            "api_key_env": api_key_env,
            "api_secret_env": api_secret_env,
        },
    )


def make_metabase_source(
    *,
    instance_url: str,
    api_key_env: str | None = None,
    username: str | None = None,
    password: str | None = None,
) -> SourceConfig:
    if api_key_env is None and (username is None or password is None):
        raise ValueError("Provide either api_key_env or both username and password")
    params: dict[str, object] = {"instance_url": instance_url}
    if api_key_env is not None:
        params["api_key_env"] = api_key_env
    if username is not None:
        params["username"] = username
    if password is not None:
        params["password"] = password
    return SourceConfig(
        id=f"metabase:{source_slug_from_url(instance_url)}",
        kind="metabase",
        params=params,
    )


def resolve_dbt_auxiliary_paths(manifest_path: str) -> tuple[str | None, str | None]:
    project_dir_path = Path(manifest_path).resolve().parent
    catalog_path = project_dir_path / "catalog.json"
    run_results_path = project_dir_path / "run_results.json"
    return (
        str(catalog_path) if catalog_path.exists() else None,
        str(run_results_path) if run_results_path.exists() else None,
    )
