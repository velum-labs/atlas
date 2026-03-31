from __future__ import annotations

from uuid import uuid4

from cryptography.fernet import Fernet

from alma_connectors.source_adapter import (
    AirflowAdapterConfig,
    ExternalSecretRef,
    FivetranAdapterConfig,
    LookerAdapterConfig,
    MetabaseAdapterConfig,
    SourceAdapterDefinition,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.source_adapter_service import SourceAdapterService


def _make_service() -> SourceAdapterService:
    return SourceAdapterService(encryption_key=Fernet.generate_key().decode("utf-8"))


def _row_from_definition(
    definition: SourceAdapterDefinition,
    service: SourceAdapterService,
) -> dict[str, object]:
    config, secrets = service.serialize_definition(definition)
    return {
        "id": str(uuid4()),
        "key": definition.key,
        "display_name": definition.display_name,
        "kind": definition.kind.value,
        "target_id": definition.target_id,
        "description": definition.description,
        "metadata": {},
        "config": config,
        "secrets": secrets,
        "status": SourceAdapterStatus.READY.value,
    }


def test_row_to_adapter_roundtrip_airflow() -> None:
    service = _make_service()
    definition = SourceAdapterDefinition(
        key="airflow-prod",
        display_name="Airflow Prod",
        kind=SourceAdapterKind.AIRFLOW,
        target_id="airflow-prod",
        config=AirflowAdapterConfig(
            base_url="https://airflow.example.com",
            auth_token_secret=ExternalSecretRef(provider="env", reference="AIRFLOW_AUTH_TOKEN"),
        ),
    )

    adapter = service.row_to_adapter(_row_from_definition(definition, service))

    assert adapter.kind == SourceAdapterKind.AIRFLOW
    assert isinstance(adapter.config, AirflowAdapterConfig)
    assert adapter.config.base_url == "https://airflow.example.com"
    assert adapter.config.auth_token_secret is not None
    assert adapter.config.auth_token_secret.reference == "AIRFLOW_AUTH_TOKEN"


def test_row_to_adapter_roundtrip_looker() -> None:
    service = _make_service()
    definition = SourceAdapterDefinition(
        key="looker-prod",
        display_name="Looker Prod",
        kind=SourceAdapterKind.LOOKER,
        target_id="looker-prod",
        config=LookerAdapterConfig(
            instance_url="https://looker.example.com",
            client_id=ExternalSecretRef(provider="env", reference="LOOKER_CLIENT_ID"),
            client_secret=ExternalSecretRef(provider="env", reference="LOOKER_CLIENT_SECRET"),
            port=443,
        ),
    )

    adapter = service.row_to_adapter(_row_from_definition(definition, service))

    assert adapter.kind == SourceAdapterKind.LOOKER
    assert isinstance(adapter.config, LookerAdapterConfig)
    assert adapter.config.port == 443
    assert adapter.config.client_id.reference == "LOOKER_CLIENT_ID"
    assert adapter.config.client_secret.reference == "LOOKER_CLIENT_SECRET"


def test_row_to_adapter_roundtrip_fivetran() -> None:
    service = _make_service()
    definition = SourceAdapterDefinition(
        key="fivetran-prod",
        display_name="Fivetran Prod",
        kind=SourceAdapterKind.FIVETRAN,
        target_id="fivetran-prod",
        config=FivetranAdapterConfig(
            api_key=ExternalSecretRef(provider="env", reference="FIVETRAN_API_KEY"),
            api_secret=ExternalSecretRef(provider="env", reference="FIVETRAN_API_SECRET"),
        ),
    )

    adapter = service.row_to_adapter(_row_from_definition(definition, service))

    assert adapter.kind == SourceAdapterKind.FIVETRAN
    assert isinstance(adapter.config, FivetranAdapterConfig)
    assert adapter.config.api_key.reference == "FIVETRAN_API_KEY"
    assert adapter.config.api_secret.reference == "FIVETRAN_API_SECRET"


def test_row_to_adapter_roundtrip_metabase() -> None:
    service = _make_service()
    definition = SourceAdapterDefinition(
        key="metabase-prod",
        display_name="Metabase Prod",
        kind=SourceAdapterKind.METABASE,
        target_id="metabase-prod",
        config=MetabaseAdapterConfig(
            instance_url="https://metabase.example.com",
            api_key=ExternalSecretRef(provider="env", reference="METABASE_API_KEY"),
        ),
    )

    adapter = service.row_to_adapter(_row_from_definition(definition, service))

    assert adapter.kind == SourceAdapterKind.METABASE
    assert isinstance(adapter.config, MetabaseAdapterConfig)
    assert adapter.config.instance_url == "https://metabase.example.com"
    assert adapter.config.api_key is not None
    assert adapter.config.api_key.reference == "METABASE_API_KEY"


def test_get_setup_instructions_supports_community_kinds() -> None:
    service = _make_service()

    for kind in (
        SourceAdapterKind.AIRFLOW,
        SourceAdapterKind.LOOKER,
        SourceAdapterKind.FIVETRAN,
        SourceAdapterKind.METABASE,
    ):
        instructions = service.get_setup_instructions(kind)
        assert instructions.summary
