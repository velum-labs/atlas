"""Typed ports for sync application use cases."""

from __future__ import annotations

from typing import Protocol

from alma_atlas.sync.protocol import SyncResponse


class TeamSyncApi(Protocol):
    async def push_assets(self, assets: list[dict], cursor: str) -> SyncResponse: ...

    async def push_edges(self, edges: list[dict], cursor: str) -> SyncResponse: ...

    async def push_contracts(self, contracts: list[dict], cursor: str) -> SyncResponse: ...

    async def push_violations(self, violations: list[dict], cursor: str) -> SyncResponse: ...

    async def pull_assets(self, cursor: str) -> list[dict]: ...

    async def pull_contracts(self, cursor: str) -> list[dict]: ...
