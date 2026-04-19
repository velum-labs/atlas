"""SyncClient — async HTTP client for team graph sync.

Responsibilities:
    push_assets / push_edges / push_contracts / push_violations
        Send local records to the team server.
    pull_assets / pull_contracts
        Fetch team records from the server since a cursor timestamp.
    full_sync
        Orchestrate a complete push+pull cycle against the local Database.
"""

from __future__ import annotations

import dataclasses
import logging
import uuid as _uuid_mod
import warnings
from typing import TYPE_CHECKING, Any

from alma_atlas.application.sync.mappers import parse_sync_timestamp
from alma_atlas.application.sync.use_cases import run_full_sync
from alma_atlas.http_utils import async_request_with_retry
from alma_atlas.sync.auth import TeamAuth
from alma_atlas.sync.protocol import SyncPayload, SyncResponse

if TYPE_CHECKING:
    import httpx

    from alma_atlas.config import AtlasConfig
    from alma_atlas_store.db import Database

log = logging.getLogger(__name__)

@dataclasses.dataclass(frozen=True)
class SyncRuntimeConfig:
    """Operational settings for team-sync HTTP behavior."""

    timeout_seconds: float = 30.0
    connect_timeout_seconds: float = 30.0
    read_timeout_seconds: float = 120.0
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    retry_status_codes: frozenset[int] = frozenset({429, 500, 502, 503, 504})


DEFAULT_SYNC_RUNTIME_CONFIG = SyncRuntimeConfig()


def _parse_ts(ts: str | None):
    """Compatibility helper for callers that still import `_parse_ts`."""
    return parse_sync_timestamp(ts)


def _validate_response(data: Any) -> dict:
    """Validate that a server response is a non-null dict. Returns the dict."""
    if not isinstance(data, dict):
        raise ValueError(f"Server returned unexpected response type: {type(data).__name__}")
    return data

class SyncClient:
    """Async HTTP client for syncing Atlas graphs with a team server."""

    def __init__(
        self,
        server_url: str,
        auth: TeamAuth,
        team_id: str,
        http_client: httpx.AsyncClient | None = None,
        runtime: SyncRuntimeConfig = DEFAULT_SYNC_RUNTIME_CONFIG,
    ) -> None:
        self._server_url = server_url.rstrip("/")
        self._auth = auth
        self._team_id = team_id
        self._http_client = http_client  # injected for testing; created lazily otherwise
        self._owns_client = False
        self._runtime = runtime

    async def __aenter__(self) -> SyncClient:
        import httpx

        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    self._runtime.timeout_seconds,
                    connect=self._runtime.connect_timeout_seconds,
                    read=self._runtime.read_timeout_seconds,
                )
            )
            self._owns_client = True
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying HTTP client if this instance owns it."""
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
            self._owns_client = False

    def __del__(self) -> None:
        if self._owns_client and self._http_client is not None:
            warnings.warn(
                "SyncClient was not closed; call aclose() or use 'async with'",
                ResourceWarning,
                stacklevel=1,
            )

    def _get_client(self) -> httpx.AsyncClient:
        import httpx

        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    self._runtime.timeout_seconds,
                    connect=self._runtime.connect_timeout_seconds,
                    read=self._runtime.read_timeout_seconds,
                )
            )
            self._owns_client = True
        return self._http_client

    async def _post(self, path: str, body: dict) -> dict:
        client = self._get_client()
        request_id = str(_uuid_mod.uuid4())
        headers = {
            **self._auth.headers(),
            "Content-Type": "application/json",
            "X-Request-ID": request_id,
        }
        url = f"{self._server_url}{path}"
        response = await async_request_with_retry(
            client,
            method="POST",
            url=url,
            headers=headers,
            json_body=body,
            logger=log,
            request_name=path,
            max_retries=self._runtime.max_retries,
            backoff_base=self._runtime.backoff_base_seconds,
            retry_status_codes=self._runtime.retry_status_codes,
        )
        try:
            data = response.json()
        except Exception as exc:
            raise ValueError(f"Server returned non-JSON response for POST {path}") from exc
        return _validate_response(data)

    async def _get(self, path: str, params: dict | None = None) -> dict:
        client = self._get_client()
        request_id = str(_uuid_mod.uuid4())
        headers = {**self._auth.headers(), "X-Request-ID": request_id}
        url = f"{self._server_url}{path}"
        response = await async_request_with_retry(
            client,
            method="GET",
            url=url,
            headers=headers,
            params=params,
            logger=log,
            request_name=path,
            max_retries=self._runtime.max_retries,
            backoff_base=self._runtime.backoff_base_seconds,
            retry_status_codes=self._runtime.retry_status_codes,
        )
        try:
            data = response.json()
        except Exception as exc:
            raise ValueError(f"Server returned non-JSON response for GET {path}") from exc
        return _validate_response(data)

    # ------------------------------------------------------------------ push

    async def push_assets(self, assets: list[dict], cursor: str) -> SyncResponse:
        """Push local asset records to the team server."""
        payload = SyncPayload(assets=assets, sync_cursor=cursor, team_id=self._team_id)
        data = await self._post(f"/teams/{self._team_id}/sync/assets", payload.to_dict())
        return SyncResponse.from_dict(data)

    async def push_edges(self, edges: list[dict], cursor: str) -> SyncResponse:
        """Push local edge records to the team server."""
        payload = SyncPayload(edges=edges, sync_cursor=cursor, team_id=self._team_id)
        data = await self._post(f"/teams/{self._team_id}/sync/edges", payload.to_dict())
        return SyncResponse.from_dict(data)

    async def push_contracts(self, contracts: list[dict], cursor: str) -> SyncResponse:
        """Push local contract definitions to the team server."""
        payload = SyncPayload(contracts=contracts, sync_cursor=cursor, team_id=self._team_id)
        data = await self._post(f"/teams/{self._team_id}/sync/contracts", payload.to_dict())
        return SyncResponse.from_dict(data)

    async def push_violations(self, violations: list[dict], cursor: str) -> SyncResponse:
        """Push local enforcement violations to the team server."""
        payload = SyncPayload(violations=violations, sync_cursor=cursor, team_id=self._team_id)
        data = await self._post(f"/teams/{self._team_id}/sync/violations", payload.to_dict())
        return SyncResponse.from_dict(data)

    # ------------------------------------------------------------------ pull

    async def pull_assets(self, cursor: str) -> list[dict]:
        """Fetch asset records from the team server updated since *cursor*."""
        data = await self._get(
            f"/teams/{self._team_id}/sync/assets",
            params={"since": cursor},
        )
        return data.get("assets", [])

    async def pull_contracts(self, cursor: str) -> list[dict]:
        """Fetch contract definitions from the team server updated since *cursor*."""
        data = await self._get(
            f"/teams/{self._team_id}/sync/contracts",
            params={"since": cursor},
        )
        return data.get("contracts", [])

    # ------------------------------------------------------------------ full sync

    async def full_sync(self, db: Database, cfg: AtlasConfig) -> SyncResponse:
        """Run the canonical application-layer full sync use case."""
        return await run_full_sync(self, db, cfg)
