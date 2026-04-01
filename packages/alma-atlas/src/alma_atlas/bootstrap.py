"""Centralized runtime/bootstrap helpers for Atlas entrypoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from alma_atlas.config import AtlasConfig, SourceConfig, get_config, load_atlas_yml


def _require_yaml_module() -> Any:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ValueError(
            "PyYAML is required for Atlas config parsing. "
            "Install it with: pip install pyyaml"
        ) from exc
    return yaml


def _read_structured_input(raw_value: str) -> Any:
    candidate_path = Path(raw_value)
    raw_payload = candidate_path.read_text(encoding="utf-8") if candidate_path.exists() else raw_value

    try:
        return json.loads(raw_payload)
    except json.JSONDecodeError:
        yaml = _require_yaml_module()
        return yaml.safe_load(raw_payload)


def _coerce_source_configs(payload: Any) -> list[SourceConfig]:
    if isinstance(payload, dict):
        raw_sources = payload.get("sources")
        if isinstance(raw_sources, list):
            payload = raw_sources
        else:
            payload = [
                {"id": source_id, **source_payload}
                for source_id, source_payload in payload.items()
                if isinstance(source_payload, dict)
            ]

    if not isinstance(payload, list):
        raise ValueError("connections input must resolve to a list or {sources:[...]} object")

    sources: list[SourceConfig] = []
    for raw_source in payload:
        if not isinstance(raw_source, dict):
            raise ValueError("each source config must be an object")
        raw_id = raw_source.get("id")
        raw_kind = raw_source.get("kind")
        if not isinstance(raw_id, str) or not raw_id.strip():
            raise ValueError("each source config requires a non-empty id")
        if not isinstance(raw_kind, str) or not raw_kind.strip():
            raise ValueError(f"source {raw_id!r} requires a non-empty kind")

        raw_params = raw_source.get("params")
        params: dict[str, Any]
        if raw_params is None:
            params = {
                str(key): value
                for key, value in raw_source.items()
                if key not in {"id", "kind"}
            }
        elif isinstance(raw_params, dict):
            params = dict(raw_params)
        else:
            raise ValueError(f"source {raw_id!r} params must be an object when provided")

        sources.append(
            SourceConfig(
                id=raw_id.strip(),
                kind=raw_kind.strip(),
                params=params,
            )
        )
    return sources


def load_config(*, config_file: str | None = None) -> AtlasConfig:
    """Load Atlas config from an explicit file or the default runtime location."""
    if config_file is not None:
        return load_atlas_yml(Path(config_file))
    return get_config()


def resolve_runtime_sources(
    *,
    config_file: str | None = None,
    connections: str | None = None,
) -> tuple[AtlasConfig, list[SourceConfig]]:
    """Resolve the runtime Atlas config plus the sources to scan."""
    cfg = load_config(config_file=config_file)
    sources = list(cfg.sources) if config_file is not None else cfg.resolved_sources()

    if connections is not None and connections.strip():
        sources = _coerce_source_configs(_read_structured_input(connections))

    return cfg, sources
