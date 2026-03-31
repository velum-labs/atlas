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

from alma_connectors.registry import CONNECTOR_SPECS, SUPPORTED_SOURCE_KINDS


@dataclass(frozen=True)
class SourceDefinition:
    """Declarative metadata for one Atlas source kind."""

    kind: str
    allowed_params: frozenset[str]
    secret_paths: tuple[tuple[str, ...], ...] = ()


SOURCE_DEFINITIONS: dict[str, SourceDefinition] = {
    kind: SourceDefinition(
        kind=kind,
        allowed_params=spec.allowed_params,
        secret_paths=spec.secret_paths,
    )
    for kind, spec in CONNECTOR_SPECS.items()
}


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
