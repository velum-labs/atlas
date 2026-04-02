"""Tests for source runtime normalization and adapter construction."""

from __future__ import annotations

import pytest

from alma_atlas.config import SourceConfig
from alma_atlas.source_runtime import _resolve_env, build_runtime_adapter
from alma_atlas.source_specs import (
    DEFAULT_AIRFLOW_AUTH_TOKEN_ENV,
    DEFAULT_FIVETRAN_API_KEY_ENV,
    DEFAULT_FIVETRAN_API_SECRET_ENV,
    DEFAULT_LOOKER_CLIENT_ID_ENV,
    DEFAULT_LOOKER_CLIENT_SECRET_ENV,
    DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV,
)
from alma_connectors.source_adapter import (
    AirflowAdapterConfig,
    BigQueryAdapterConfig,
    ExternalSecretRef,
    FivetranAdapterConfig,
    LookerAdapterConfig,
    MetabaseAdapterConfig,
    PostgresAdapterConfig,
    SnowflakeAdapterConfig,
)


def test_resolve_env_raises_for_missing_variable() -> None:
    with pytest.raises(ValueError, match="environment variable 'MISSING_SECRET' is not configured"):
        _resolve_env(ExternalSecretRef(provider="env", reference="MISSING_SECRET"))


def test_build_runtime_adapter_defaults_bigquery_to_adc() -> None:
    source = SourceConfig(id="bigquery:analytics", kind="bigquery", params={"project_id": "acme-project"})

    _adapter, persisted = build_runtime_adapter(source)

    assert isinstance(persisted.config, BigQueryAdapterConfig)
    assert persisted.config.project_id == "acme-project"
    assert persisted.config.service_account_secret is None


def test_build_runtime_adapter_normalizes_scalar_schema_values() -> None:
    source = SourceConfig(
        id="postgres:warehouse",
        kind="postgres",
        params={
            "dsn_env": "PG_DATABASE_URL",
            "include_schemas": "analytics",
            "exclude_schemas": "internal",
        },
    )

    _adapter, persisted = build_runtime_adapter(source)

    assert isinstance(persisted.config, PostgresAdapterConfig)
    assert persisted.config.include_schemas == ("analytics",)
    assert persisted.config.exclude_schemas == ("internal",)


def test_build_runtime_adapter_uses_default_secret_envs_for_runtime_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV, '{"account":"xy12345","user":"atlas","password":"secret"}')
    monkeypatch.setenv(DEFAULT_AIRFLOW_AUTH_TOKEN_ENV, "token")
    monkeypatch.setenv(DEFAULT_LOOKER_CLIENT_ID_ENV, "client-id")
    monkeypatch.setenv(DEFAULT_LOOKER_CLIENT_SECRET_ENV, "client-secret")
    monkeypatch.setenv(DEFAULT_FIVETRAN_API_KEY_ENV, "api-key")
    monkeypatch.setenv(DEFAULT_FIVETRAN_API_SECRET_ENV, "api-secret")

    cases = [
        (
            SourceConfig(id="snowflake:prod", kind="snowflake", params={"account": "xy12345.us-east-1"}),
            SnowflakeAdapterConfig,
            "account_secret",
            DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV,
        ),
        (
            SourceConfig(id="airflow:prod", kind="airflow", params={"base_url": "https://airflow.example.com"}),
            AirflowAdapterConfig,
            "auth_token_secret",
            DEFAULT_AIRFLOW_AUTH_TOKEN_ENV,
        ),
        (
            SourceConfig(id="looker:prod", kind="looker", params={"instance_url": "https://looker.example.com"}),
            LookerAdapterConfig,
            "client_id",
            DEFAULT_LOOKER_CLIENT_ID_ENV,
        ),
        (
            SourceConfig(id="fivetran:default", kind="fivetran", params={}),
            FivetranAdapterConfig,
            "api_key",
            DEFAULT_FIVETRAN_API_KEY_ENV,
        ),
    ]

    for source, config_type, field_name, expected_ref in cases:
        _adapter, persisted = build_runtime_adapter(source)
        assert isinstance(persisted.config, config_type)
        secret = getattr(persisted.config, field_name)
        assert isinstance(secret, ExternalSecretRef)
        assert secret.reference == expected_ref

    _adapter, persisted = build_runtime_adapter(
        SourceConfig(id="looker:prod", kind="looker", params={"instance_url": "https://looker.example.com"})
    )
    assert isinstance(persisted.config, LookerAdapterConfig)
    assert isinstance(persisted.config.client_secret, ExternalSecretRef)
    assert persisted.config.client_secret.reference == DEFAULT_LOOKER_CLIENT_SECRET_ENV

    _adapter, persisted = build_runtime_adapter(SourceConfig(id="fivetran:default", kind="fivetran", params={}))
    assert isinstance(persisted.config, FivetranAdapterConfig)
    assert isinstance(persisted.config.api_secret, ExternalSecretRef)
    assert persisted.config.api_secret.reference == DEFAULT_FIVETRAN_API_SECRET_ENV


def test_build_runtime_adapter_accepts_password_env_for_basic_auth_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_PASSWORD", "secret")
    monkeypatch.setenv("METABASE_PASSWORD", "secret")

    airflow_source = SourceConfig(
        id="airflow:prod",
        kind="airflow",
        params={
            "base_url": "https://airflow.example.com",
            "username": "atlas",
            "password_env": "AIRFLOW_PASSWORD",
        },
    )
    _adapter, airflow_persisted = build_runtime_adapter(airflow_source)
    assert isinstance(airflow_persisted.config, AirflowAdapterConfig)
    assert isinstance(airflow_persisted.config.password_secret, ExternalSecretRef)
    assert airflow_persisted.config.password_secret.reference == "AIRFLOW_PASSWORD"

    metabase_source = SourceConfig(
        id="metabase:prod",
        kind="metabase",
        params={
            "instance_url": "https://metabase.example.com",
            "username": "atlas",
            "password_env": "METABASE_PASSWORD",
        },
    )
    _adapter, metabase_persisted = build_runtime_adapter(metabase_source)
    assert isinstance(metabase_persisted.config, MetabaseAdapterConfig)
    assert isinstance(metabase_persisted.config.password, ExternalSecretRef)
    assert metabase_persisted.config.password.reference == "METABASE_PASSWORD"
