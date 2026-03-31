"""Runtime adapter instantiation from canonical persisted configs."""

from __future__ import annotations

from collections.abc import Callable

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
    SnowflakeAdapterConfig,
    SourceAdapter,
)
from alma_connectors.source_adapter_v2 import SourceAdapterV2

type SecretResolver = Callable[[ManagedSecret | ExternalSecretRef], str]
type RuntimeSourceAdapter = SourceAdapter | SourceAdapterV2


def _resolve_optional_secret(
    secret: ManagedSecret | ExternalSecretRef | None,
    *,
    resolve_secret: SecretResolver,
) -> str | None:
    if secret is None:
        return None
    return resolve_secret(secret)


def instantiate_runtime_adapter(
    adapter: PersistedSourceAdapter,
    *,
    resolve_secret: SecretResolver,
) -> RuntimeSourceAdapter:
    """Instantiate the runtime adapter for one canonical persisted adapter."""

    config = adapter.config

    if isinstance(config, PostgresAdapterConfig):
        return PostgresAdapter(resolve_secret=resolve_secret)

    if isinstance(config, BigQueryAdapterConfig):
        return BigQueryAdapter(resolve_secret=resolve_secret)

    if isinstance(config, DbtAdapterConfig):
        return DbtAdapter(
            manifest_path=config.manifest_path,
            catalog_path=config.catalog_path,
            run_results_path=config.run_results_path,
            project_name=config.project_name,
        )

    if isinstance(config, SnowflakeAdapterConfig):
        return SnowflakeAdapter(resolve_secret=resolve_secret)

    if isinstance(config, AirflowAdapterConfig):
        return AirflowAdapter(
            base_url=config.base_url,
            auth_token=_resolve_optional_secret(
                config.auth_token_secret,
                resolve_secret=resolve_secret,
            ),
            username=config.username,
            password=_resolve_optional_secret(
                config.password_secret,
                resolve_secret=resolve_secret,
            ),
            timeout_seconds=config.timeout_seconds,
        )

    if isinstance(config, LookerAdapterConfig):
        return LookerAdapter(
            instance_url=config.instance_url,
            client_id=resolve_secret(config.client_id),
            client_secret=resolve_secret(config.client_secret),
            port=config.port,
            timeout_seconds=config.timeout_seconds,
        )

    if isinstance(config, FivetranAdapterConfig):
        return FivetranAdapter(
            api_key=resolve_secret(config.api_key),
            api_secret=resolve_secret(config.api_secret),
            api_base=config.api_base,
            timeout_seconds=config.timeout_seconds,
        )

    if isinstance(config, MetabaseAdapterConfig):
        return MetabaseAdapter(
            instance_url=config.instance_url,
            api_key=_resolve_optional_secret(config.api_key, resolve_secret=resolve_secret),
            username=config.username,
            password=_resolve_optional_secret(config.password, resolve_secret=resolve_secret),
            timeout_seconds=config.timeout_seconds,
        )

    raise ValueError(f"unsupported adapter config type: {type(config).__name__}")
