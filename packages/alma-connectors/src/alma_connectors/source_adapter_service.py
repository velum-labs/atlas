"""Application helpers for persisted source adapters."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from alma_connectors.credentials import decrypt_credential, encrypt_credential
from alma_connectors.registry import (
    deserialize_config as deserialize_connector_config,
)
from alma_connectors.registry import (
    get_setup_instructions as get_connector_setup_instructions,
)
from alma_connectors.registry import (
    serialize_definition as serialize_connector_definition,
)
from alma_connectors.source_adapter import (
    ConnectionTestResult,
    ExternalSecretRef,
    ManagedSecret,
    PersistedSourceAdapter,
    QueryResult,
    SchemaSnapshot,
    SetupInstructions,
    SourceAdapter,
    SourceAdapterCapabilities,
    SourceAdapterDefinition,
    SourceAdapterKind,
    SourceAdapterStatus,
    TrafficObservationResult,
)
from alma_connectors.source_adapter_runtime import instantiate_runtime_adapter
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    DefinitionSnapshot,
    DiscoverySnapshot,
    LineageSnapshot,
    OrchestrationSnapshot,
    SourceAdapterV2,
    TrafficExtractionResult,
)


def _require_dict(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


def _normalize_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return None


def _normalize_cursor(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    return None


def _iterable_or_empty(value: object) -> tuple[object, ...]:
    if isinstance(value, list | tuple):
        return tuple(value)
    return ()


class SourceAdapterService:
    """Maps persisted adapter rows onto typed runtime behavior."""

    def __init__(self, *, encryption_key: str) -> None:
        self._encryption_key = encryption_key

    def _get_adapter(self, adapter: PersistedSourceAdapter) -> Any:
        """Return the runtime adapter for a persisted adapter."""
        return instantiate_runtime_adapter(adapter, resolve_secret=self.resolve_secret)

    def _get_v2_adapter(self, adapter: PersistedSourceAdapter) -> SourceAdapterV2:
        runtime = self._get_adapter(adapter)
        if not isinstance(runtime, SourceAdapterV2):
            raise ValueError(f"{type(runtime).__name__} does not implement SourceAdapterV2")
        return runtime

    def _get_v1_adapter(self, adapter: PersistedSourceAdapter) -> SourceAdapter:
        runtime = self._get_adapter(adapter)
        if not isinstance(runtime, SourceAdapter):
            raise ValueError(f"{type(runtime).__name__} does not implement the legacy SourceAdapter protocol")
        return runtime

    def encrypt_secret(self, secret: str) -> bytes:
        return encrypt_credential(secret, key=self._encryption_key)

    def decrypt_secret(self, ciphertext: bytes) -> str:
        return decrypt_credential(ciphertext, key=self._encryption_key)

    def encrypt_dsn(self, dsn: str) -> bytes:
        return self.encrypt_secret(dsn)

    def decrypt_dsn(self, ciphertext: bytes) -> str:
        return self.decrypt_secret(ciphertext)

    def resolve_secret(self, secret: ManagedSecret | ExternalSecretRef) -> str:
        if isinstance(secret, ManagedSecret):
            return self.decrypt_secret(secret.ciphertext)
        provider = secret.provider.lower()
        if provider == "literal":
            return secret.reference
        if provider in {"env", "environment"}:
            resolved = os.getenv(secret.reference)
            if resolved is None:
                raise ValueError(f"environment variable '{secret.reference}' is not configured for adapter secret")
            return resolved
        raise ValueError(f"external secret provider '{secret.provider}' is not supported for live resolution")

    def get_capabilities(self, adapter: PersistedSourceAdapter) -> SourceAdapterCapabilities:
        runtime_adapter = self._get_adapter(adapter)
        capabilities = getattr(runtime_adapter, "capabilities", None)
        if capabilities is not None:
            return capabilities

        declared_capabilities = getattr(runtime_adapter, "declared_capabilities", frozenset())
        return SourceAdapterCapabilities(
            can_test_connection=True,
            can_introspect_schema=AdapterCapability.SCHEMA in declared_capabilities,
            can_observe_traffic=AdapterCapability.TRAFFIC in declared_capabilities,
            can_execute_query=False,
        )

    def get_setup_instructions(self, kind: SourceAdapterKind) -> SetupInstructions:
        return get_connector_setup_instructions(kind)

    def serialize_definition(
        self,
        definition: SourceAdapterDefinition,
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        return serialize_connector_definition(
            definition,
            serialize_secret=self._serialize_secret,
        )

    def row_to_adapter(self, row: dict[str, Any]) -> PersistedSourceAdapter:
        adapter_id = str(row.get("id", "")).strip()
        UUID(adapter_id)
        kind = SourceAdapterKind(str(row.get("kind", "")).strip())
        config = _require_dict(row.get("config") or {}, field_name="config")
        secrets = _require_dict(row.get("secrets") or {}, field_name="secrets")
        adapter_config = deserialize_connector_config(
            kind=kind,
            config=config,
            secrets=secrets,
            deserialize_secret=self._secret_from_storage_payload,
        )

        return PersistedSourceAdapter(
            id=adapter_id,
            key=str(row.get("key", "")),
            display_name=str(row.get("display_name", "")),
            kind=kind,
            target_id=str(row.get("target_id", "")),
            description=row.get("description"),
            metadata={
                str(key): str(value)
                for key, value in _require_dict(
                    row.get("metadata") or {},
                    field_name="metadata",
                ).items()
            },
            config=adapter_config,
            status=SourceAdapterStatus(str(row.get("status", "pending"))),
            status_message=row.get("status_message"),
            last_tested_at=_normalize_datetime(row.get("last_tested_at")),
            last_observed_at=_normalize_datetime(row.get("last_observed_at")),
            observation_cursor=_normalize_cursor(row.get("observation_cursor")),
            created_at=_normalize_datetime(row.get("created_at")),
            updated_at=_normalize_datetime(row.get("updated_at")),
        )

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        return await self._get_adapter(adapter).test_connection(adapter)

    async def introspect_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshot:
        return await self._get_v1_adapter(adapter).introspect_schema(adapter)

    async def observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        return await self._get_v1_adapter(adapter).observe_traffic(adapter, since=since)

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
    ) -> QueryResult:
        return await self._get_adapter(adapter).execute_query(
            adapter,
            sql,
            max_rows=max_rows,
            probe_target=probe_target,
        )

    async def discover(self, adapter: PersistedSourceAdapter) -> DiscoverySnapshot:
        return await self._get_v2_adapter(adapter).discover(adapter)

    async def extract_schema(self, adapter: PersistedSourceAdapter):
        return await self._get_v2_adapter(adapter).extract_schema(adapter)

    async def extract_definitions(self, adapter: PersistedSourceAdapter) -> DefinitionSnapshot:
        return await self._get_v2_adapter(adapter).extract_definitions(adapter)

    async def extract_lineage(self, adapter: PersistedSourceAdapter) -> LineageSnapshot:
        return await self._get_v2_adapter(adapter).extract_lineage(adapter)

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        return await self._get_v2_adapter(adapter).extract_traffic(adapter, since=since)

    async def extract_orchestration(self, adapter: PersistedSourceAdapter) -> OrchestrationSnapshot:
        return await self._get_v2_adapter(adapter).extract_orchestration(adapter)

    def _serialize_secret(self, secret: ManagedSecret | ExternalSecretRef) -> dict[str, Any]:
        if isinstance(secret, ManagedSecret):
            return {
                "storage_kind": "managed_secret",
                "ciphertext": secret.ciphertext,
                "external_secret_provider": None,
                "external_secret_reference": None,
            }
        return {
            "storage_kind": "external_secret_ref",
            "ciphertext": None,
            "external_secret_provider": secret.provider,
            "external_secret_reference": secret.reference,
        }

    def _secret_from_storage_payload(
        self,
        payload: dict[str, Any],
    ) -> ManagedSecret | ExternalSecretRef:
        storage_kind = str(payload.get("storage_kind", "")).strip()
        if storage_kind == "managed_secret":
            ciphertext = payload.get("ciphertext")
            if not isinstance(ciphertext, bytes):
                raise ValueError("managed secrets require ciphertext bytes")
            return ManagedSecret(ciphertext=ciphertext)
        if storage_kind == "external_secret_ref":
            return ExternalSecretRef(
                provider=str(payload.get("external_secret_provider", "")),
                reference=str(payload.get("external_secret_reference", "")),
            )
        raise ValueError(f"unsupported source adapter secret kind: {storage_kind}")
