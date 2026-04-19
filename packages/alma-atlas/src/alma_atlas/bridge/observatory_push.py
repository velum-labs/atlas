"""Push Atlas scan results to a running Alma Observatory instance.

Uses the Observatory Connect-RPC JSON endpoints to upsert assets,
ingest query observations, and optionally trigger analysis/derivation.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import httpx

from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.query_repository import QueryRepository

logger = logging.getLogger(__name__)

_RPC_BASE = "velum.observatory.v1.ObservatoryService"
_DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


class ObservatoryBridge:
    """Pushes Atlas store data to an Observatory instance over HTTP."""

    def __init__(
        self,
        base_url: str,
        auth_token: str | None = None,
        *,
        timeout: httpx.Timeout = _DEFAULT_TIMEOUT,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> ObservatoryBridge:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------
    # Low-level RPC helper
    # ------------------------------------------------------------------

    async def _call_rpc(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST a JSON-encoded Connect-RPC request and return the response body."""
        url = f"{self._base_url}/{_RPC_BASE}/{method}"
        response = await self._client.post(url, json=payload)
        if response.status_code != 200:
            body_text = response.text[:500]
            raise ObservatoryRpcError(
                f"{method} failed (HTTP {response.status_code}): {body_text}"
            )
        return response.json()  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # High-level operations
    # ------------------------------------------------------------------

    async def upsert_assets_from_db(
        self,
        db: Database,
        *,
        target_id: str,
    ) -> int:
        """Upsert all Atlas assets into Observatory. Returns the count pushed."""
        assets = AssetRepository(db).list_all()
        for asset in assets:
            await self._call_rpc(
                "UpsertAsset",
                {
                    "targetId": target_id,
                    "assetId": asset.id,
                    "name": asset.name,
                    "kind": asset.kind,
                    "source": asset.source,
                    "description": asset.description or "",
                },
            )
        logger.info("Upserted %d asset(s) to Observatory", len(assets))
        return len(assets)

    async def ingest_queries_from_db(
        self,
        db: Database,
        *,
        target_id: str,
        backend_system: str,
        ingest_source_id: str = "atlas",
        ingest_source_kind: str = "atlas_scan",
        batch_size: int = 200,
    ) -> int:
        """Ingest all Atlas query observations into Observatory. Returns the count pushed."""
        queries = QueryRepository(db).list_all()
        if not queries:
            return 0

        now_rfc3339 = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        events: list[dict[str, Any]] = []
        for q in queries:
            event: dict[str, Any] = {
                "id": q.fingerprint,
                "sql": q.sql_text,
                "sourceName": q.source,
                "targetId": target_id,
                "backendSystem": backend_system,
                "fingerprintHash": q.fingerprint,
                "capturedAt": now_rfc3339,
            }
            metadata: dict[str, Any] = {}
            if q.execution_count > 1:
                metadata["executionCount"] = q.execution_count
            if q.tables:
                metadata["tables"] = q.tables
            if metadata:
                event["metadata"] = metadata
            events.append(event)

        pushed = 0
        for i in range(0, len(events), batch_size):
            batch = events[i : i + batch_size]
            await self._call_rpc(
                "Ingest",
                {
                    "sourceId": ingest_source_id,
                    "sourceKind": ingest_source_kind,
                    "events": batch,
                },
            )
            pushed += len(batch)

        logger.info("Ingested %d query event(s) to Observatory", pushed)
        return pushed

    async def analyze_and_derive(
        self,
        *,
        min_cluster_size: int = 10,
        support_threshold: float | None = None,
    ) -> dict[str, Any]:
        """Run Analyze then DeriveProposals and return both responses."""
        analyze_payload: dict[str, Any] = {"minClusterSize": min_cluster_size}
        if support_threshold is not None:
            analyze_payload["supportThreshold"] = support_threshold

        analyze_resp = await self._call_rpc("Analyze", analyze_payload)
        derive_resp = await self._call_rpc("DeriveProposals", {})
        return {"analyze": analyze_resp, "derive": derive_resp}


class ObservatoryRpcError(Exception):
    """Raised when an Observatory RPC call returns a non-200 status."""
