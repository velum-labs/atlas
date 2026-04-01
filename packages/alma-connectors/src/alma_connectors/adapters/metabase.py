"""Metabase source adapter — community contribution.

This adapter authenticates with the Metabase API using a session token and
extracts database/collection metadata and query execution history.

Metabase API reference:
    https://www.metabase.com/docs/latest/api-documentation

Authentication:
    Metabase uses a session-token model.  A token is obtained via
    ``POST /api/session`` with username and password and included in every
    subsequent request as the ``X-Metabase-Session`` header.  Tokens expire
    after 14 days of inactivity by default.

    Alternatively, pass a pre-issued ``api_key`` (Metabase 0.49+) which is
    sent as the ``x-api-key`` header — this is the preferred approach for
    service accounts.

Capabilities declared:
    DISCOVER  — list databases and collections as containers
    SCHEMA    — table and field metadata per connected database
    TRAFFIC   — query execution history (QueryExecution table)

Example usage::

    adapter = MetabaseAdapter(
        instance_url="https://metabase.example.com",
        api_key="mb_abc123...",
    )
    snapshot = asyncio.run(adapter.discover(persisted))
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

import httpx

from alma_connectors.adapters._base import BaseAdapterV2
from alma_connectors.source_adapter import (
    ConnectionTestResult,
    ObservedQueryEvent,
    PersistedSourceAdapter,
    SetupInstructions,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    ColumnSchema,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionScope,
    SchemaObject,
    SchemaObjectKind,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

logger = logging.getLogger(__name__)


class MetabaseAdapter(BaseAdapterV2):
    """Metabase source adapter.

    Implements the SourceAdapterV2 protocol against the Metabase REST API.
    Connected databases and collections are mapped to ``DiscoveredContainer``
    objects; field metadata drives SCHEMA; and query execution logs drive
    TRAFFIC.

    Capabilities
    ------------
    DISCOVER
        Lists all connected databases via ``GET /api/database`` and all
        collections via ``GET /api/collection``.  Each database and root-level
        (non-archived) collection becomes a ``DiscoveredContainer``.

    SCHEMA
        For each database, fetches table list via
        ``GET /api/database/{db_id}/metadata`` and maps tables + fields to
        ``SchemaObject`` / ``ColumnSchema`` records.

    TRAFFIC
        Reads query execution history from ``GET /api/activity``.  On
        Pro/Enterprise instances the adapter tries the audit endpoint first:
        ``GET /api/ee/audit-app/query_execution``.

    Args:
        instance_url: Root URL of the Metabase instance, e.g.
            ``"https://metabase.example.com"``.  Must not include a trailing
            slash.
        api_key: API key for Metabase 0.49+ (preferred for service accounts).
        username: Username for session-token authentication.
        password: Password for session-token authentication.
        timeout_seconds: Per-request HTTP timeout in seconds (default 30).
    """

    kind = SourceAdapterKindV2.METABASE

    declared_capabilities: frozenset[AdapterCapability] = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.SCHEMA,
        AdapterCapability.TRAFFIC,
    })

    def __init__(
        self,
        instance_url: str,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        if not instance_url or not instance_url.strip():
            raise ValueError("instance_url must be a non-empty string")
        if api_key is None and (username is None or password is None):
            raise ValueError(
                "Provide either api_key or both username and password"
            )
        self._instance_url = instance_url.rstrip("/")
        self._api_key = api_key
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds
        self._session_token: str | None = None
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scope_identifiers(self) -> dict[str, str]:
        return {"instance_url": self._instance_url}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_auth_headers(self) -> dict[str, str]:
        """Return the correct authentication headers for API requests."""
        if self._api_key:
            return {"x-api-key": self._api_key}
        if not self._session_token:
            client = await self._get_client()
            resp = await client.post(
                f"{self._instance_url}/api/session",
                json={"username": self._username, "password": self._password},
                timeout=self._timeout_seconds,
            )
            resp.raise_for_status()
            token = resp.json().get("id")
            if token is None:
                raise ValueError(
                    "Metabase session endpoint did not return an 'id' — check credentials"
                )
            self._session_token = token
        return {"X-Metabase-Session": self._session_token}

    async def _api_get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Execute an authenticated GET request against the Metabase API.

        On a 401 response, clears the session token, re-authenticates, and
        retries once.
        """
        headers = await self._get_auth_headers()
        client = await self._get_client()
        resp = await client.get(
            f"{self._instance_url}/api/{path}",
            headers=headers,
            params=params,
            timeout=self._timeout_seconds,
        )
        if resp.status_code == 401:
            self._session_token = None
            headers = await self._get_auth_headers()
            resp = await client.get(
                f"{self._instance_url}/api/{path}",
                headers=headers,
                params=params,
                timeout=self._timeout_seconds,
            )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # v2 protocol — lifecycle
    # ------------------------------------------------------------------

    async def _validate_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate connectivity by calling ``GET /api/user/current``."""
        try:
            me = await self._api_get("user/current")
            return ConnectionTestResult(
                success=True,
                message=f"Metabase reachable; logged in as {me.get('email', 'unknown')}",
            )
        except Exception as exc:
            return ConnectionTestResult(success=False, message=str(exc))

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe capability availability by verifying API access."""
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities

        # Verify basic connectivity
        try:
            await self._api_get("database")
            db_ok = True
        except Exception:
            db_ok = False

        # Check for Pro/Enterprise audit feature
        enterprise_traffic = False
        if db_ok and AdapterCapability.TRAFFIC in caps_to_probe:
            try:
                settings = await self._api_get("setting")
                features = {}
                if isinstance(settings, list):
                    features = {
                        s["key"]: s.get("value")
                        for s in settings
                        if isinstance(s, dict) and "key" in s
                    }
                elif isinstance(settings, dict):
                    features = settings.get("token-features", {}) or {}
                enterprise_traffic = bool(features.get("audit-app-subscription"))
            except Exception:
                pass

        results: list[CapabilityProbeResult] = []
        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"instance_url": self._instance_url},
        )
        for cap in caps_to_probe:
            if cap == AdapterCapability.TRAFFIC:
                results.append(CapabilityProbeResult(
                    capability=cap,
                    available=db_ok,
                    scope=ExtractionScope.GLOBAL,
                    scope_context=scope_ctx,
                    fallback_used=not enterprise_traffic,
                    message=(
                        "Enterprise audit endpoint available"
                        if enterprise_traffic
                        else "Using OSS activity feed (limited history)"
                    ),
                ))
            else:
                results.append(CapabilityProbeResult(
                    capability=cap,
                    available=db_ok,
                    scope=ExtractionScope.GLOBAL,
                    scope_context=scope_ctx,
                ))
        return tuple(results)

    # ------------------------------------------------------------------
    # v2 protocol — DISCOVER
    # ------------------------------------------------------------------

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: list databases and collections → DiscoveredContainers."""
        t0 = time.monotonic()

        db_response = await self._api_get("database")
        databases = db_response.get("data", db_response) if isinstance(db_response, dict) else db_response

        collections = await self._api_get("collection")

        containers: list[DiscoveredContainer] = []
        for db in databases:
            containers.append(DiscoveredContainer(
                container_id=f"metabase://database/{db['id']}",
                container_type="database",
                display_name=db["name"],
                metadata={
                    "engine": db.get("engine") or "",
                    "is_full_sync": db.get("is_full_sync", False),
                    "is_sample": db.get("is_sample", False),
                },
            ))
        for coll in collections:
            if coll.get("archived"):
                continue
            containers.append(DiscoveredContainer(
                container_id=f"metabase://collection/{coll['id']}",
                container_type="collection",
                display_name=coll["name"],
                metadata={
                    "slug": coll.get("slug") or "",
                    "location": coll.get("location") or "/",
                },
            ))

        duration_ms = (time.monotonic() - t0) * 1000
        return DiscoverySnapshot(
            meta=self._make_meta(adapter, AdapterCapability.DISCOVER, len(containers), duration_ms),
            containers=tuple(containers),
        )

    # ------------------------------------------------------------------
    # v2 protocol — SCHEMA
    # ------------------------------------------------------------------

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """SCHEMA: table and field definitions for each connected database."""
        t0 = time.monotonic()

        db_response = await self._api_get("database")
        databases = db_response.get("data", db_response) if isinstance(db_response, dict) else db_response

        objects: list[SchemaObject] = []
        for db in databases:
            db_id = db["id"]
            db_name = db["name"]
            try:
                meta = await self._api_get(f"database/{db_id}/metadata", params={"include_hidden": "false"})
            except Exception:
                logger.warning("Failed to fetch metadata for database %s (%s)", db_id, db_name)
                continue

            for table in meta.get("tables", []):
                columns: list[ColumnSchema] = []
                for field in table.get("fields", []):
                    base_type = (field.get("base_type") or "unknown").replace("type/", "")
                    columns.append(ColumnSchema(
                        name=field["name"],
                        data_type=base_type,
                        description=field.get("description") or None,
                    ))
                entity_type = (table.get("entity_type") or "").lower()
                kind = SchemaObjectKind.VIEW if "view" in entity_type else SchemaObjectKind.TABLE
                schema_name = table.get("schema") or db_name
                objects.append(SchemaObject(
                    schema_name=schema_name,
                    object_name=table["name"],
                    kind=kind,
                    columns=tuple(columns),
                    description=table.get("description") or None,
                ))

        duration_ms = (time.monotonic() - t0) * 1000
        return SchemaSnapshotV2(
            meta=self._make_meta(adapter, AdapterCapability.SCHEMA, len(objects), duration_ms),
            objects=tuple(objects),
        )

    # ------------------------------------------------------------------
    # v2 protocol — TRAFFIC
    # ------------------------------------------------------------------

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        """TRAFFIC: query execution history → ObservedQueryEvent records.

        Tries the Enterprise audit endpoint first; falls back to the OSS
        activity feed if unavailable.
        """
        t0 = time.monotonic()
        events: list[ObservedQueryEvent] = []

        # Try Enterprise audit endpoint first
        enterprise_ok = False
        try:
            params: dict[str, Any] = {"limit": 1000}
            if since:
                params["start_date"] = since.isoformat()
            ee_response = await self._api_get("ee/audit-app/query_execution", params=params)
            entries = ee_response.get("data", []) if isinstance(ee_response, dict) else ee_response
            for entry in entries:
                started_raw = entry.get("started_at") or ""
                try:
                    captured_at = datetime.fromisoformat(started_raw.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    captured_at = datetime.now(UTC)
                sql = entry.get("native") or ""
                events.append(ObservedQueryEvent(
                    captured_at=captured_at,
                    sql=sql or "-- no native query",
                    source_name=self._instance_url,
                    query_type="card-query",
                    event_id=entry.get("query_hash") or None,
                    database_name=str(entry.get("database_id") or ""),
                    database_user=str(entry.get("executor_id") or "unknown"),
                    duration_ms=float(entry.get("running_time") or 0),
                ))
            enterprise_ok = True
        except Exception:
            pass

        if not enterprise_ok:
            # Fall back to OSS activity feed
            activity = await self._api_get("activity", params={"limit": 1000})
            if isinstance(activity, dict):
                activity = activity.get("data", [])
            for entry in activity:
                if entry.get("topic") not in ("card-query", "query"):
                    continue
                ts_raw = entry.get("timestamp") or ""
                try:
                    captured_at = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    captured_at = datetime.now(UTC)
                if since and captured_at < since:
                    continue
                details = entry.get("details") or {}
                sql = details.get("native") or ""
                events.append(ObservedQueryEvent(
                    captured_at=captured_at,
                    sql=sql or "-- no native query",
                    source_name=self._instance_url,
                    query_type=entry.get("topic") or "card-query",
                    event_id=str(entry["id"]) if entry.get("id") is not None else None,
                    database_name=str(entry.get("model_id") or ""),
                    database_user=(entry.get("user") or {}).get("email") or "unknown",
                    duration_ms=float(details.get("running_time") or 0),
                ))

        duration_ms = (time.monotonic() - t0) * 1000
        return TrafficExtractionResult(
            meta=self._make_meta(adapter, AdapterCapability.TRAFFIC, len(events), duration_ms),
            events=tuple(events),
        )

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling the Metabase adapter."""
        return SetupInstructions(
            title="Metabase REST API Adapter",
            summary=(
                "Extracts connected database metadata, table/field schemas, and "
                "query execution history from the Metabase API."
            ),
            steps=(
                "Create a dedicated Metabase service account with Admin or at minimum 'All Users' group access",
                "For Metabase 0.49+: Admin → Settings → Authentication → API Keys → Create API Key (preferred)",
                "For older versions: use username/password — the adapter will obtain a session token via POST /api/session",
                "For TRAFFIC capability on OSS Metabase, the activity feed is limited to recent events; consider Pro/Enterprise for full audit logs",
                "Provide instance_url (e.g. https://metabase.example.com) and api_key or username/password when constructing MetabaseAdapter",
            ),
        )
