"""Public adapter definition and persistence codec API."""

from __future__ import annotations

from alma_connectors.registry import (
    build_adapter_definition,
    build_persisted_adapter,
    deserialize_config,
    serialize_definition,
)

__all__ = [
    "build_adapter_definition",
    "build_persisted_adapter",
    "deserialize_config",
    "serialize_definition",
]
