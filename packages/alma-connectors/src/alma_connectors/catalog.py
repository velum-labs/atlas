"""Public connector catalog API for Atlas source semantics.

This module is the stable place to ask connector-focused questions such as:
- which source kinds are supported
- which params are allowed for a kind
- which persisted params are secrets
- what the default env var names and source defaults are
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from alma_connectors.registry import (
    CONNECTOR_SPECS,
    DEFAULT_AIRFLOW_AUTH_TOKEN_ENV,
    DEFAULT_BIGQUERY_LOCATION,
    DEFAULT_FIVETRAN_API_KEY_ENV,
    DEFAULT_FIVETRAN_API_SECRET_ENV,
    DEFAULT_LOOKER_CLIENT_ID_ENV,
    DEFAULT_LOOKER_CLIENT_SECRET_ENV,
    DEFAULT_LOOKER_PORT,
    DEFAULT_POSTGRES_SCHEMA,
    DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV,
    SUPPORTED_SOURCE_KINDS,
    ConnectorSpec,
    get_connector_spec,
    get_setup_instructions,
)

__all__ = [
    "CONNECTOR_SPECS",
    "ConnectorSpec",
    "DEFAULT_AIRFLOW_AUTH_TOKEN_ENV",
    "DEFAULT_BIGQUERY_LOCATION",
    "DEFAULT_FIVETRAN_API_KEY_ENV",
    "DEFAULT_FIVETRAN_API_SECRET_ENV",
    "DEFAULT_LOOKER_CLIENT_ID_ENV",
    "DEFAULT_LOOKER_CLIENT_SECRET_ENV",
    "DEFAULT_LOOKER_PORT",
    "DEFAULT_POSTGRES_SCHEMA",
    "DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV",
    "SUPPORTED_SOURCE_KINDS",
    "allowed_source_params",
    "get_connector_spec",
    "get_setup_instructions",
    "redact_source_params",
    "source_secret_paths",
]


def allowed_source_params(kind: str) -> frozenset[str]:
    """Return the allowed persisted params for one source kind."""
    return get_connector_spec(kind).allowed_params


def source_secret_paths(kind: str) -> tuple[tuple[str, ...], ...]:
    """Return nested param paths that should be treated as secrets."""
    return get_connector_spec(kind).secret_paths


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
