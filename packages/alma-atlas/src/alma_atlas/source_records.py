"""Typed local source definition and runtime state records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AtlasSourceState:
    """Mutable runtime state associated with a registered source."""

    observation_cursor: dict[str, object] | None = None


@dataclass
class AtlasSourceDefinition:
    """Persisted user-owned source definition."""

    id: str
    kind: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class AtlasSourceRecord:
    """Full local source record: definition plus runtime state."""

    definition: AtlasSourceDefinition
    state: AtlasSourceState = field(default_factory=AtlasSourceState)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> AtlasSourceRecord:
        params = dict(raw.get("params", {}))
        raw_state = raw.get("state")
        observation_cursor = None
        if isinstance(raw_state, dict):
            cursor = raw_state.get("observation_cursor")
            if isinstance(cursor, dict):
                observation_cursor = dict(cursor)
        elif isinstance(params.get("observation_cursor"), dict):
            observation_cursor = dict(params.pop("observation_cursor"))

        return cls(
            definition=AtlasSourceDefinition(
                id=str(raw["id"]),
                kind=str(raw["kind"]),
                params=params,
            ),
            state=AtlasSourceState(observation_cursor=observation_cursor),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "id": self.definition.id,
            "kind": self.definition.kind,
            "params": dict(self.definition.params),
        }
        if self.state.observation_cursor is not None:
            payload["state"] = {
                "observation_cursor": dict(self.state.observation_cursor),
            }
        return payload
