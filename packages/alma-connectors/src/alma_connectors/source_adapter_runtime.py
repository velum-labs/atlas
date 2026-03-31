"""Runtime adapter instantiation from canonical persisted configs."""

from __future__ import annotations

from collections.abc import Callable

from alma_connectors.registry import instantiate_runtime_adapter as instantiate_runtime_adapter_from_registry
from alma_connectors.source_adapter import ExternalSecretRef, ManagedSecret, PersistedSourceAdapter

type SecretResolver = Callable[[ManagedSecret | ExternalSecretRef], str]
type RuntimeSourceAdapter = object


def instantiate_runtime_adapter(
    adapter: PersistedSourceAdapter,
    *,
    resolve_secret: SecretResolver,
) -> RuntimeSourceAdapter:
    """Instantiate the runtime adapter for one canonical persisted adapter."""
    return instantiate_runtime_adapter_from_registry(adapter, resolve_secret=resolve_secret)  # type: ignore[return-value]
