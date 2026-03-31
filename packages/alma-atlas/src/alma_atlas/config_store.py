"""Persistence helpers for Atlas local state.

`AtlasConfig` owns in-memory state and precedence. This module owns the
filesystem layout and persistence format for that state.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from alma_atlas.local_secrets import LocalSecretStore
from alma_atlas.source_registry import source_secret_paths


@dataclass(frozen=True)
class AtlasPaths:
    """Concrete filesystem paths for one Atlas config directory."""

    config_dir: Path

    @property
    def sources_file(self) -> Path:
        return self.config_dir / "sources.json"

    @property
    def config_file(self) -> Path:
        return self.config_dir / "config.json"

    @property
    def sync_cursor_file(self) -> Path:
        return self.config_dir / "sync_cursor.json"


class AtlasConfigStore:
    """Read/write Atlas config state with secret-aware persistence."""

    def __init__(self, config_dir: Path) -> None:
        self.paths = AtlasPaths(config_dir)
        self._secret_store = LocalSecretStore(config_dir)

    def ensure_dir(self) -> None:
        self.paths.config_dir.mkdir(parents=True, exist_ok=True)

    def load_sources(self) -> list[dict[str, Any]]:
        if not self.paths.sources_file.exists():
            return []
        raw = json.loads(self.paths.sources_file.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("sources.json must contain a JSON array")
        loaded: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                raise ValueError("each persisted source must be a JSON object")
            kind = str(item["kind"])
            source_id = str(item["id"])
            params = self._deserialize_source_params(kind, source_id, item.get("params", {}))
            loaded.append({"id": source_id, "kind": kind, "params": params})
        return loaded

    def save_sources(self, sources: list[dict[str, Any]]) -> None:
        self.ensure_dir()
        payload = []
        for source in sources:
            kind = str(source["kind"])
            source_id = str(source["id"])
            params = self._serialize_source_params(kind, source_id, source.get("params", {}))
            payload.append({"id": source_id, "kind": kind, "params": params})
        self.paths.sources_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def load_team_config(self) -> dict[str, Any]:
        if not self.paths.config_file.exists():
            return {}
        data = json.loads(self.paths.config_file.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("config.json must contain a JSON object")
        team = data.get("team", {})
        if not isinstance(team, dict):
            raise ValueError("team config must be a JSON object")
        loaded = dict(team)
        loaded["api_key"] = self._secret_store.resolve(team.get("api_key"))
        return loaded

    def save_team_config(self, team_data: dict[str, Any]) -> None:
        self.ensure_dir()
        data: dict[str, Any] = {}
        if self.paths.config_file.exists():
            existing = json.loads(self.paths.config_file.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                data = existing
        serialized = dict(team_data)
        api_key = serialized.get("api_key")
        if isinstance(api_key, str) and api_key:
            serialized["api_key"] = self._secret_store.store("team.api_key", api_key)
        data["team"] = serialized
        self.paths.config_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def load_sync_cursor(self) -> str | None:
        if not self.paths.sync_cursor_file.exists():
            return None
        data = json.loads(self.paths.sync_cursor_file.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("sync_cursor.json must contain a JSON object")
        cursor = data.get("cursor")
        return cursor if isinstance(cursor, str) else None

    def save_sync_cursor(self, cursor: str) -> None:
        self.ensure_dir()
        self.paths.sync_cursor_file.write_text(json.dumps({"cursor": cursor}), encoding="utf-8")

    def _serialize_source_params(self, kind: str, source_id: str, params: object) -> dict[str, Any]:
        if not isinstance(params, dict):
            raise ValueError("source params must be a JSON object")
        serialized = cast(dict[str, Any], deepcopy(params))
        for path in source_secret_paths(kind):
            value = _get_nested(serialized, path)
            if not isinstance(value, str) or not value:
                continue
            secret_id = f"source.{source_id}.{'.'.join(path)}"
            _set_nested(serialized, path, self._secret_store.store(secret_id, value))
        return serialized

    def _deserialize_source_params(self, kind: str, source_id: str, params: object) -> dict[str, Any]:
        if not isinstance(params, dict):
            raise ValueError(f"persisted params for {source_id!r} must be a JSON object")
        loaded = cast(dict[str, Any], deepcopy(params))
        for path in source_secret_paths(kind):
            value = _get_nested(loaded, path)
            if self._secret_store.is_marker(value):
                _set_nested(loaded, path, self._secret_store.resolve(value))
        return loaded


def _get_nested(data: dict[str, Any], path: tuple[str, ...]) -> object:
    current: object = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _set_nested(data: dict[str, Any], path: tuple[str, ...], value: object) -> None:
    current: dict[str, Any] = data
    for key in path[:-1]:
        nested = current.get(key)
        if not isinstance(nested, dict):
            nested = {}
            current[key] = nested
        current = nested
    current[path[-1]] = value
