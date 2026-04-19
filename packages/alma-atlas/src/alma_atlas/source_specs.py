"""Canonical source kind defaults, registration helpers, and param allowlists."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from alma_atlas.config import SourceConfig
from alma_connectors.catalog import (
    DEFAULT_AIRFLOW_AUTH_TOKEN_ENV,
    DEFAULT_BIGQUERY_LOCATION,
    DEFAULT_FIVETRAN_API_KEY_ENV,
    DEFAULT_FIVETRAN_API_SECRET_ENV,
    DEFAULT_LOOKER_CLIENT_ID_ENV,
    DEFAULT_LOOKER_CLIENT_SECRET_ENV,
    DEFAULT_LOOKER_PORT,
    DEFAULT_POSTGRES_SCHEMA,
    DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV,
)
from alma_connectors.catalog import (
    allowed_source_params as _allowed_source_params,
)


def source_slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or url
    return host.replace(".", "-")


def allowed_source_params(kind: str) -> frozenset[str]:
    return _allowed_source_params(kind)


def ensure_source_params_allowed(source: SourceConfig) -> None:
    allowed = allowed_source_params(source.kind)
    unknown = set(source.params) - set(allowed)
    if unknown:
        raise ValueError(
            f"{source.kind} source has unsupported param(s): {sorted(unknown)}. "
            f"Allowed params: {sorted(allowed)}"
        )


def make_bigquery_source(
    *,
    project: str,
    credentials: str | None = None,
    service_account_env: str | None = None,
    location: str = DEFAULT_BIGQUERY_LOCATION,
) -> SourceConfig:
    """Build a BigQuery source config.

    When credentials are omitted, the runtime uses Application Default Credentials.
    """
    if credentials and service_account_env:
        raise ValueError("Use either credentials or service_account_env, not both")
    params: dict[str, str] = {"project_id": project, "location": location}
    if credentials:
        params["credentials"] = credentials
    if service_account_env:
        params["service_account_env"] = service_account_env
    return SourceConfig(id=f"bigquery:{project}", kind="bigquery", params=params)


def make_postgres_source(
    *,
    dsn: str | None = None,
    dsn_env: str | None = None,
    schema: str = DEFAULT_POSTGRES_SCHEMA,
) -> SourceConfig:
    if dsn and dsn_env:
        raise ValueError("Use either dsn or dsn_env, not both")
    if not dsn and not dsn_env:
        raise ValueError("Provide either dsn or dsn_env")
    db_locator = dsn or dsn_env or "postgres"
    db_name = db_locator.rsplit("/", 1)[-1].split("?", 1)[0]
    source_id = f"postgres:{db_name}" if schema == DEFAULT_POSTGRES_SCHEMA else f"postgres:{db_name}:{schema}"
    params: dict[str, object] = {"include_schemas": [schema]}
    if dsn is not None:
        params["dsn"] = dsn
    if dsn_env is not None:
        params["dsn_env"] = dsn_env
    return SourceConfig(
        id=source_id,
        kind="postgres",
        params=params,
    )


def make_sqlite_source(
    *,
    path: str,
    source_id: str | None = None,
) -> SourceConfig:
    resolved_path = str(Path(path).expanduser().resolve())
    default_source_id = f"sqlite:{Path(resolved_path).stem}"
    return SourceConfig(
        id=source_id or default_source_id,
        kind="sqlite",
        params={"path": resolved_path},
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
    password_env: str | None = None,
) -> SourceConfig:
    params: dict[str, object] = {"base_url": base_url}
    if auth_token_env:
        params["auth_token_env"] = auth_token_env
    if username is not None:
        params["username"] = username
    if password is not None:
        params["password"] = password
    if password_env is not None:
        params["password_env"] = password_env
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
    password_env: str | None = None,
) -> SourceConfig:
    has_password = password is not None or password_env is not None
    if api_key_env is None and (username is None or not has_password):
        raise ValueError("Provide either api_key_env or both username and a password/password_env")
    params: dict[str, object] = {"instance_url": instance_url}
    if api_key_env is not None:
        params["api_key_env"] = api_key_env
    if username is not None:
        params["username"] = username
    if password is not None:
        params["password"] = password
    if password_env is not None:
        params["password_env"] = password_env
    return SourceConfig(
        id=f"metabase:{source_slug_from_url(instance_url)}",
        kind="metabase",
        params=params,
    )




DEFAULT_GITHUB_PRIVATE_KEY_ENV = "GITHUB_APP_PRIVATE_KEY"
DEFAULT_GITHUB_TOKEN_ENV = "GITHUB_TOKEN"


def make_github_source(
    *,
    app_id: str | None = None,
    installation_id: str | None = None,
    private_key_env: str | None = DEFAULT_GITHUB_PRIVATE_KEY_ENV,
    token_env: str | None = None,
    base_url: str = "https://api.github.com",
    repos: list[str] | None = None,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    max_file_size_bytes: int = 1_000_000,
    branch: str | None = None,
    source_id: str | None = None,
) -> SourceConfig:
    """Build a GitHub source config.

    Auth:
    - GitHub App mode: app_id + installation_id + private_key_env
    - Token mode: token_env
    """
    repos = list(repos or [])
    if token_env:
        # token mode
        params: dict[str, object] = {
            "base_url": base_url,
            "token_env": token_env,
            "repos": repos,
            "max_file_size_bytes": max_file_size_bytes,
        }
    else:
        if not app_id or not installation_id:
            raise ValueError("Provide either token_env, or app_id + installation_id (+ private_key_env)")
        if not private_key_env:
            raise ValueError("private_key_env is required for GitHub App auth")
        params = {
            "base_url": base_url,
            "app_id": app_id,
            "installation_id": installation_id,
            "private_key_env": private_key_env,
            "repos": repos,
            "max_file_size_bytes": max_file_size_bytes,
        }

    if include_patterns:
        params["include_patterns"] = list(include_patterns)
    if exclude_patterns:
        params["exclude_patterns"] = list(exclude_patterns)
    if branch:
        params["branch"] = branch

    default_id = "github:default"
    if repos:
        default_id = "github:" + repos[0].replace("/", "-")
    return SourceConfig(id=source_id or default_id, kind="github", params=params)
def resolve_dbt_auxiliary_paths(manifest_path: str) -> tuple[str | None, str | None]:
    project_dir_path = Path(manifest_path).resolve().parent
    catalog_path = project_dir_path / "catalog.json"
    run_results_path = project_dir_path / "run_results.json"
    return (
        str(catalog_path) if catalog_path.exists() else None,
        str(run_results_path) if run_results_path.exists() else None,
    )
