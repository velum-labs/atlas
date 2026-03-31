from __future__ import annotations

from cryptography.fernet import Fernet

from alma_connectors.registry import (
    build_adapter_definition,
    build_persisted_adapter,
    deserialize_config,
    serialize_definition,
)
from alma_connectors.source_adapter import ExternalSecretRef, SourceAdapterKind
from alma_connectors.source_adapter_service import SourceAdapterService


def _service() -> SourceAdapterService:
    return SourceAdapterService(encryption_key=Fernet.generate_key().decode("utf-8"))


def test_registry_builds_persisted_adapter_with_cursor() -> None:
    adapter = build_persisted_adapter(
        "postgres:warehouse",
        "postgres",
        {"dsn_env": "PG_URL", "include_schemas": ["public"]},
        observation_cursor={"cursor": "1"},
    )

    assert adapter.kind == SourceAdapterKind.POSTGRES
    assert adapter.observation_cursor == {"cursor": "1"}


def test_registry_serializes_and_deserializes_definition_roundtrip() -> None:
    service = _service()
    definition = build_adapter_definition(
        "looker:prod",
        "looker",
        {
            "instance_url": "https://looker.example.com",
            "client_id_env": "LOOKER_CLIENT_ID",
            "client_secret_env": "LOOKER_CLIENT_SECRET",
        },
    )

    config_payload, secrets_payload = serialize_definition(
        definition,
        serialize_secret=service._serialize_secret,
    )
    restored = deserialize_config(
        kind="looker",
        config=config_payload,
        secrets=secrets_payload,
        deserialize_secret=service._secret_from_storage_payload,
    )

    assert restored == definition.config
    assert isinstance(restored.client_id, ExternalSecretRef)
