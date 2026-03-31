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
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from alma_atlas.http_utils import async_request_with_retry
from alma_atlas.sync.auth import TeamAuth
from alma_atlas.sync.conflict import ConflictResolver
from alma_atlas.sync.protocol import SyncPayload, SyncResponse

if TYPE_CHECKING:
    import httpx

    from alma_atlas.config import AtlasConfig
    from alma_atlas_store.db import Database

log = logging.getLogger(__name__)

_NULL_CURSOR = "1970-01-01T00:00:00Z"


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


def _parse_ts(ts: str | None) -> datetime:
    if not ts:
        return datetime.min.replace(tzinfo=UTC)
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _asset_to_dict(a: Any) -> dict:
    return _to_dict(a)


def _edge_to_dict(e: Any) -> dict:
    d = _to_dict(e)
    # include computed id for convenience
    d.setdefault("id", e.id if hasattr(e, "id") else "")
    return d


def _contract_to_dict(c: Any) -> dict:
    return _to_dict(c)


def _violation_to_dict(v: Any) -> dict:
    return _to_dict(v)


def _validate_response(data: Any) -> dict:
    """Validate that a server response is a non-null dict. Returns the dict."""
    if not isinstance(data, dict):
        raise ValueError(f"Server returned unexpected response type: {type(data).__name__}")
    return data


def _to_dict(value: Any) -> dict[str, Any]:
    return dataclasses.asdict(value) if dataclasses.is_dataclass(value) else dict(value)


def _latest_cursor(*cursors: str) -> str:
    latest = ""
    latest_dt = datetime.min.replace(tzinfo=UTC)
    for cursor in cursors:
        if not cursor:
            continue
        parsed = _parse_ts(cursor)
        if parsed >= latest_dt:
            latest_dt = parsed
            latest = cursor
    return latest


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
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
            self._owns_client = False

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
        """Push all local changes and pull team contracts/assets.

        Uses the stored sync cursor so only records changed since the last
        sync are transmitted.  Saves the new cursor only after all pushes
        succeed (atomicity — a partial failure leaves the cursor unchanged
        so the next sync retries).
        """
        from alma_atlas_store.asset_repository import AssetRepository
        from alma_atlas_store.contract_repository import ContractRepository
        from alma_atlas_store.edge_repository import EdgeRepository
        from alma_atlas_store.violation_repository import ViolationRepository

        cursor = cfg.load_sync_cursor() or _NULL_CURSOR

        # Collect local records changed since cursor
        all_assets = AssetRepository(db).list_all()
        all_edges = EdgeRepository(db).list_all()
        all_contracts = ContractRepository(db).list_all()
        all_violations = ViolationRepository(db).list_recent(limit=1000)

        assets = [a for a in all_assets if _parse_ts(a.last_seen) >= _parse_ts(cursor)]
        edges = [e for e in all_edges if _parse_ts(e.last_seen) >= _parse_ts(cursor)]
        contracts = [c for c in all_contracts if _parse_ts(c.updated_at) >= _parse_ts(cursor)]
        violations = [v for v in all_violations if _parse_ts(v.detected_at) >= _parse_ts(cursor)]

        log.info(
            "[sync] pushing %d assets, %d edges, %d contracts, %d violations (cursor=%s)",
            len(assets),
            len(edges),
            len(contracts),
            len(violations),
            cursor,
        )

        # Push all record types atomically — if any push fails the exception
        # propagates and the cursor is NOT updated, so the next sync retries.
        asset_resp = await self.push_assets([_asset_to_dict(a) for a in assets], cursor)
        if asset_resp.rejected:
            log.warning("[sync] server rejected %d asset(s)", len(asset_resp.rejected))
        edge_resp = await self.push_edges([_edge_to_dict(e) for e in edges], cursor)
        if edge_resp.rejected:
            log.warning("[sync] server rejected %d edge(s)", len(edge_resp.rejected))
        contract_push_resp = await self.push_contracts([_contract_to_dict(c) for c in contracts], cursor)
        if contract_push_resp.rejected:
            log.warning("[sync] server rejected %d contract(s)", len(contract_push_resp.rejected))
        violation_resp = await self.push_violations([_violation_to_dict(v) for v in violations], cursor)
        if violation_resp.rejected:
            log.warning("[sync] server rejected %d violation(s)", len(violation_resp.rejected))

        new_cursor = _latest_cursor(
            asset_resp.new_cursor,
            edge_resp.new_cursor,
            contract_push_resp.new_cursor,
            violation_resp.new_cursor,
        ) or cursor

        # Pull team contracts (server-wins)
        resolver = ConflictResolver()
        contract_repo = ContractRepository(db)
        pulled_contracts = await self.pull_contracts(cursor)
        for remote in pulled_contracts:
            local = contract_repo.get(remote["id"])
            resolved = resolver.resolve_contract(_contract_to_dict(local) if local else {}, remote)
            contract_repo.upsert(_dict_to_contract(resolved))
        log.info("[sync] pulled %d contract(s) from team", len(pulled_contracts))

        # Pull team assets (last-write-wins)
        asset_repo = AssetRepository(db)
        pulled_assets = await self.pull_assets(cursor)
        for remote in pulled_assets:
            local = asset_repo.get(remote["id"])
            resolved = resolver.resolve_asset(_asset_to_dict(local) if local else {}, remote)
            asset_repo.upsert(_dict_to_asset(resolved))
        log.info("[sync] pulled %d asset(s) from team", len(pulled_assets))

        # Save cursor only after all pushes and pulls succeeded.
        if new_cursor:
            cfg.save_sync_cursor(new_cursor)

        return SyncResponse(
            accepted_count=(
                asset_resp.accepted_count
                + edge_resp.accepted_count
                + contract_push_resp.accepted_count
                + violation_resp.accepted_count
            ),
            rejected=[
                *asset_resp.rejected,
                *edge_resp.rejected,
                *contract_push_resp.rejected,
                *violation_resp.rejected,
            ],
            new_cursor=new_cursor,
        )


# ------------------------------------------------------------------ helpers


def _dict_to_asset(d: dict) -> Any:
    from alma_atlas_store.asset_repository import Asset

    return Asset(
        id=d["id"],
        source=d.get("source", ""),
        kind=d.get("kind", ""),
        name=d.get("name", ""),
        description=d.get("description"),
        tags=d.get("tags", []),
        metadata=d.get("metadata", {}),
        first_seen=d.get("first_seen"),
        last_seen=d.get("last_seen"),
    )


def _dict_to_contract(d: dict) -> Any:
    from alma_atlas_store.contract_repository import Contract

    return Contract(
        id=d["id"],
        asset_id=d.get("asset_id", ""),
        version=d.get("version", "1"),
        spec=d.get("spec", {}),
        status=d.get("status", "draft"),
        mode=d.get("mode", "shadow"),
        created_at=d.get("created_at"),
        updated_at=d.get("updated_at"),
    )
