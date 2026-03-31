"""Canonical registry for Atlas source kinds.

This module is the single source of truth for:
- supported source kinds
- allowed persisted params
- which params are secrets and should be redacted or stored securely
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SourceDefinition:
    """Declarative metadata for one Atlas source kind."""

    kind: str
    allowed_params: frozenset[str]
    secret_paths: tuple[tuple[str, ...], ...] = ()


SOURCE_DEFINITIONS: dict[str, SourceDefinition] = {
    "bigquery": SourceDefinition(
        kind="bigquery",
        allowed_params=frozenset(
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
    ),
    "postgres": SourceDefinition(
        kind="postgres",
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
    ),
    "dbt": SourceDefinition(
        kind="dbt",
        allowed_params=frozenset(
            {
                "catalog_path",
                "manifest_path",
                "observation_cursor",
                "project_name",
                "run_results_path",
            }
        ),
    ),
    "snowflake": SourceDefinition(
        kind="snowflake",
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
    ),
    "airflow": SourceDefinition(
        kind="airflow",
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
    ),
    "looker": SourceDefinition(
        kind="looker",
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
    ),
    "fivetran": SourceDefinition(
        kind="fivetran",
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
    ),
    "metabase": SourceDefinition(
        kind="metabase",
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
    ),
}

SUPPORTED_SOURCE_KINDS = frozenset(SOURCE_DEFINITIONS)


def get_source_definition(kind: str) -> SourceDefinition:
    try:
        return SOURCE_DEFINITIONS[kind]
    except KeyError as exc:
        supported = ", ".join(sorted(SUPPORTED_SOURCE_KINDS))
        raise ValueError(f"Unknown source kind: {kind!r}. Supported: {supported}") from exc


def allowed_source_params(kind: str) -> frozenset[str]:
    return get_source_definition(kind).allowed_params


def source_secret_paths(kind: str) -> tuple[tuple[str, ...], ...]:
    return get_source_definition(kind).secret_paths


def ensure_source_params_allowed(kind: str, params: dict[str, Any]) -> None:
    allowed = allowed_source_params(kind)
    unknown = set(params) - set(allowed)
    if unknown:
        raise ValueError(
            f"{kind} source has unsupported param(s): {sorted(unknown)}. "
            f"Allowed params: {sorted(allowed)}"
        )


def redact_source_params(kind: str, params: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of source params with secret values redacted."""
    redacted = deepcopy(params)
    for path in source_secret_paths(kind):
        current: Any = redacted
        for key in path[:-1]:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if not isinstance(current, dict):
            continue
        leaf = path[-1]
        if leaf in current:
            current[leaf] = "***"
    return redacted
