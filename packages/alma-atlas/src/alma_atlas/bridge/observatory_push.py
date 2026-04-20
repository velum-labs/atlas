"""Push Atlas scan results to a running Alma Observatory instance.

Uses the Observatory Connect-RPC JSON endpoints (served as plain JSON over HTTP)
so Atlas can integrate without generated protobuf client stubs.

This module:
- upserts assets via `UpsertAsset`
- ingests query observations via `Ingest`
- optionally triggers `Analyze` + `DeriveProposals`
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
        self._client = httpx.AsyncClient(headers=headers, timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> ObservatoryBridge:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def _call_rpc(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST a JSON-encoded RPC request and return the response body."""
        url = f"{self._base_url}/{_RPC_BASE}/{method}"
        response = await self._client.post(url, json=payload)
        if response.status_code != 200:
            body_text = response.text[:500]
            raise ObservatoryRpcError(f"{method} failed (HTTP {response.status_code}): {body_text}")
        parsed = response.json()
        if not isinstance(parsed, dict):
            raise ObservatoryRpcError(f"{method} response must be an object")
        return parsed

    async def upsert_assets_from_db(
        self, db: Database, *, target_id: str
    ) -> dict[str, str]:
        """Upsert all Atlas assets into Observatory.

        Returns a mapping of atlas_asset_id -> observatory_uuid.
        """
        assets = AssetRepository(db).list_all()
        id_map: dict[str, str] = {}  # atlas id -> observatory uuid
        for asset in assets:
            resp = await self._call_rpc(
                "UpsertAsset",
                {
                    "asset": {
                        "canonicalName": asset.id,
                        "layer": "raw",
                        "contractStatus": "unobserved",
                        "sourceProvenance": "atlas",
                        "qualityFlags": [],
                        "physicalNames": [
                            {
                                "targetId": target_id,
                                "system": asset.source,
                                "physicalName": asset.name,
                            }
                        ],
                        "columns": [],
                    }
                },
            )
            obs_id = resp.get("id")
            if obs_id:
                id_map[asset.id] = obs_id
        logger.info("Upserted %d asset(s) to Observatory", len(id_map))
        return id_map

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
                "capturedAt": now_rfc3339,
                "sourceName": q.source,
                "sql": q.sql_text,
                "targetId": target_id,
                "captureSourceId": ingest_source_id,
                "captureSourceKind": ingest_source_kind,
                "backendSystem": backend_system,
                "fingerprintHash": q.fingerprint,
            }
            metadata: dict[str, str] = {}
            if q.execution_count > 1:
                metadata["executionCount"] = str(q.execution_count)
            if q.tables:
                metadata["tables"] = ",".join(q.tables)
            if metadata:
                event["metadata"] = metadata
            events.append(event)

        pushed = 0
        for i in range(0, len(events), batch_size):
            batch_events = events[i : i + batch_size]
            await self._call_rpc(
                "Ingest",
                {
                    "batch": {
                        "events": batch_events,
                        "ingestSourceId": ingest_source_id,
                        "ingestSourceKind": ingest_source_kind,
                        "targetId": target_id,
                        "backendSystem": backend_system,
                    }
                },
            )
            pushed += len(batch_events)

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
        analysis_run_id = analyze_resp.get("analysisRunId")
        if not isinstance(analysis_run_id, str) or not analysis_run_id.strip():
            raise ObservatoryRpcError("Analyze response missing analysisRunId")

        derive_resp = await self._call_rpc(
            "DeriveProposals",
            {
                "analysisRunId": analysis_run_id,
                "dryRun": False,
            },
        )
        return {"analyze": analyze_resp, "derive": derive_resp}


class ObservatoryRpcError(Exception):
    """Raised when an Observatory RPC call returns a non-200 status."""
