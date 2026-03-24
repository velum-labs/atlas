"""Sync wire protocol — payload and response dataclasses for team sync."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SyncPayload:
    """Data pushed to or pulled from the team sync server."""

    assets: list[dict] = field(default_factory=list)
    edges: list[dict] = field(default_factory=list)
    contracts: list[dict] = field(default_factory=list)
    violations: list[dict] = field(default_factory=list)
    sync_cursor: str = "1970-01-01T00:00:00Z"  # ISO timestamp — send only changes after this
    team_id: str = ""

    def to_dict(self) -> dict:
        return {
            "assets": self.assets,
            "edges": self.edges,
            "contracts": self.contracts,
            "violations": self.violations,
            "sync_cursor": self.sync_cursor,
            "team_id": self.team_id,
        }


@dataclass
class RejectedItem:
    """An item the server declined to accept, with a reason."""

    id: str
    reason: str


@dataclass
class SyncResponse:
    """Response returned by the team sync server after a push."""

    accepted_count: int = 0
    rejected: list[RejectedItem] = field(default_factory=list)
    new_cursor: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> SyncResponse:
        rejected = [RejectedItem(**r) for r in data.get("rejected", [])]
        return cls(
            accepted_count=data.get("accepted_count", 0),
            rejected=rejected,
            new_cursor=data.get("new_cursor", ""),
        )
