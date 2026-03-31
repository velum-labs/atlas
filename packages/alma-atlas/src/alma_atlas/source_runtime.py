"""SourceConfig -> persisted adapter + runtime adapter construction."""

from __future__ import annotations

import os

from alma_atlas.config import SourceConfig
from alma_atlas.source_specs import ensure_source_params_allowed
from alma_connectors.registry import build_persisted_adapter, instantiate_runtime_adapter
from alma_connectors.source_adapter import PersistedSourceAdapter

type RuntimeSourceAdapter = object


def _resolve_env(secret: object) -> str:
    provider = getattr(secret, "provider", "env")
    ref = getattr(secret, "reference", None)
    if provider == "literal":
        value = str(ref or "")
        if not value:
            raise ValueError("literal secrets must be non-empty")
        return value
    if not ref:
        raise ValueError("environment-backed secrets require a reference")
    value = os.environ.get(str(ref))
    if value is None:
        raise ValueError(f"environment variable {ref!r} is not configured for adapter secret")
    return value


def build_runtime_adapter(source: SourceConfig) -> tuple[RuntimeSourceAdapter, PersistedSourceAdapter]:
    """Construct both the runtime adapter and canonical persisted adapter record."""
    ensure_source_params_allowed(source)
    observation_cursor = source.params.get("observation_cursor")
    persisted = build_persisted_adapter(
        source.id,
        source.kind,
        source.params,
        observation_cursor=dict(observation_cursor) if isinstance(observation_cursor, dict) else None,
        display_name=source.id,
    )
    runtime = instantiate_runtime_adapter(persisted, resolve_secret=_resolve_env)
    return runtime, persisted
