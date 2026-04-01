"""Fivetran source adapter.

Extracts connector inventory, table-level lineage (source → destination), and
sync-schedule metadata from the Fivetran REST API.

Fivetran API reference:
    https://fivetran.com/docs/rest-api

Authentication:
    Basic auth using an API key and API secret obtained from the Fivetran
    dashboard (Settings → API Config).

Capabilities declared:
    DISCOVER      — list connectors as DiscoveredContainers
    LINEAGE       — source table → destination table edges per connector
    ORCHESTRATION — connector sync schedule as OrchestrationUnit

Example usage::

    adapter = FivetranAdapter(
        api_key="my_key",
        api_secret="my_secret",
    )
    snapshot = asyncio.run(adapter.discover(persisted))
"""

from __future__ import annotations

import contextlib
import logging
import time
from datetime import datetime
from typing import Any

import httpx

from alma_connectors.adapters._base import BaseAdapterV2
from alma_connectors.source_adapter import (
    ConnectionTestResult,
    PersistedSourceAdapter,
    SetupInstructions,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    OrchestrationSnapshot,
    OrchestrationUnit,
    ScopeContext,
    SourceAdapterKindV2,
)

logger = logging.getLogger(__name__)

_FIVETRAN_API_BASE = "https://api.fivetran.com"


class FivetranAdapter(BaseAdapterV2):
    """Fivetran source adapter.

    Implements the SourceAdapterV2 protocol against the Fivetran REST API v1.

    Capabilities
    ------------
    DISCOVER
        Lists all connectors in the account via ``GET /v1/connectors`` and
        returns one ``DiscoveredContainer`` per connector.

    LINEAGE
        For each connector, fetches schema configuration via
        ``GET /v1/connectors/{id}/schemas`` and emits one ``LineageEdge`` per
        enabled source table pointing to the corresponding destination table.

    ORCHESTRATION
        Fetches connector details via ``GET /v1/connectors/{id}`` and maps
        the sync schedule (``sync_frequency``, ``schedule_type``) to an
        ``OrchestrationUnit``.

    Args:
        api_key: Fivetran API key (from Settings → API Config).
        api_secret: Fivetran API secret.
        api_base: Override the Fivetran API base URL (default:
            ``"https://api.fivetran.com"``).
        timeout_seconds: Per-request HTTP timeout in seconds (default 30).
    """

    kind = SourceAdapterKindV2.FIVETRAN

    declared_capabilities: frozenset[AdapterCapability] = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.LINEAGE,
        AdapterCapability.ORCHESTRATION,
    })

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_base: str = _FIVETRAN_API_BASE,
        timeout_seconds: int = 30,
    ) -> None:
        if not api_key or not api_key.strip():
            raise ValueError("api_key must be a non-empty string")
        if not api_secret or not api_secret.strip():
            raise ValueError("api_secret must be a non-empty string")
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_base = api_base.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scope_identifiers(self) -> dict[str, str]:
        return {"api_base": self._api_base}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _api_get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Execute an authenticated GET request against the Fivetran API."""
        client = await self._get_client()
        resp = await client.get(
            f"{self._api_base}/{path}",
            auth=(self._api_key, self._api_secret),
            params=params,
            timeout=self._timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()

    async def _get_all_connectors(self) -> list[dict[str, Any]]:
        """Paginate through GET /v1/connectors and return all connectors."""
        connectors: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": 100}
            if cursor:
                params["cursor"] = cursor
            response = await self._api_get("v1/connectors", params=params)
            data = response.get("data", {})
            items = data.get("items", [])
            connectors.extend(items)
            next_cursor = data.get("next_cursor")
            if not next_cursor:
                break
            cursor = next_cursor
        return connectors

    # ------------------------------------------------------------------
    # v2 protocol — lifecycle
    # ------------------------------------------------------------------

    async def _validate_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate credentials by fetching the user/account info."""
        try:
            resp = await self._api_get("v1/account/info")
            account = (resp.get("data") or {}).get("account_name") or "unknown"
            return ConnectionTestResult(
                success=True,
                message=f"Fivetran API reachable; account: {account}",
            )
        except Exception as exc:
            return ConnectionTestResult(success=False, message=str(exc))

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe capability availability."""
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities

        try:
            await self._api_get("v1/connectors", params={"limit": 1})
            available = True
        except Exception:
            available = False

        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"api_base": self._api_base},
        )
        return tuple(
            CapabilityProbeResult(
                capability=cap,
                available=available,
                scope=ExtractionScope.GLOBAL,
                scope_context=scope_ctx,
            )
            for cap in caps_to_probe
        )

    # ------------------------------------------------------------------
    # v2 protocol — DISCOVER
    # ------------------------------------------------------------------

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: list connectors → DiscoveredContainers.

        Fivetran API endpoint:
            GET /v1/connectors

        Response shape (single item)::

            {
              "id": "iodize_impressive",
              "service": "postgres",
              "schema": "production_pg",
              "status": {"sync_state": "scheduled", "setup_state": "connected"},
              "succeeded_at": "2024-01-15T10:00:00.000Z",
              "group_id": "projected_morning"
            }
        """
        t0 = time.monotonic()
        connectors = await self._get_all_connectors()

        containers = [
            DiscoveredContainer(
                container_id=f"fivetran://connector/{c['id']}",
                container_type="connector",
                display_name=c.get("schema") or c["id"],
                metadata={
                    "service": c.get("service") or "",
                    "group_id": c.get("group_id") or "",
                    "sync_state": (c.get("status") or {}).get("sync_state") or "",
                    "setup_state": (c.get("status") or {}).get("setup_state") or "",
                },
            )
            for c in connectors
        ]

        duration_ms = (time.monotonic() - t0) * 1000
        return DiscoverySnapshot(
            meta=self._make_meta(adapter, AdapterCapability.DISCOVER, len(containers), duration_ms),
            containers=tuple(containers),
        )

    # ------------------------------------------------------------------
    # v2 protocol — LINEAGE
    # ------------------------------------------------------------------

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """LINEAGE: source table → destination table edges per connector.

        Fivetran API endpoint:
            GET /v1/connectors/{connector_id}/schemas

        Response shape::

            {
              "data": {
                "schemas": {
                  "public": {
                    "name_in_destination": "public",
                    "enabled": true,
                    "tables": {
                      "orders": {
                        "name_in_destination": "orders",
                        "enabled": true
                      }
                    }
                  }
                }
              }
            }
        """
        t0 = time.monotonic()
        connectors = await self._get_all_connectors()
        edges: list[LineageEdge] = []

        for connector in connectors:
            connector_id = connector["id"]
            dest_schema_prefix = connector.get("schema") or connector_id
            try:
                resp = await self._api_get(f"v1/connectors/{connector_id}/schemas")
                schemas = (resp.get("data") or {}).get("schemas") or {}
            except Exception:
                logger.warning("Failed to fetch schemas for connector %s", connector_id)
                continue

            for src_schema_name, schema_obj in schemas.items():
                if not isinstance(schema_obj, dict):
                    continue
                if not schema_obj.get("enabled", True):
                    continue
                dest_schema_name = schema_obj.get("name_in_destination") or src_schema_name
                for src_table_name, table_obj in (schema_obj.get("tables") or {}).items():
                    if not isinstance(table_obj, dict):
                        continue
                    if not table_obj.get("enabled", True):
                        continue
                    dest_table_name = table_obj.get("name_in_destination") or src_table_name
                    edges.append(LineageEdge(
                        source_object=f"{src_schema_name}.{src_table_name}",
                        target_object=f"{dest_schema_prefix}__{dest_schema_name}.{dest_table_name}",
                        edge_kind=LineageEdgeKind.CONNECTOR_API,
                        confidence=1.0,
                        metadata={"connector_id": connector_id, "service": connector.get("service") or ""},
                    ))

        duration_ms = (time.monotonic() - t0) * 1000
        return LineageSnapshot(
            meta=self._make_meta(adapter, AdapterCapability.LINEAGE, len(edges), duration_ms),
            edges=tuple(edges),
        )

    # ------------------------------------------------------------------
    # v2 protocol — ORCHESTRATION
    # ------------------------------------------------------------------

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """ORCHESTRATION: connector sync schedule → OrchestrationUnit.

        Fivetran API endpoint:
            GET /v1/connectors/{connector_id}

        Response shape (relevant fields)::

            {
              "data": {
                "id": "iodize_impressive",
                "service": "postgres",
                "schema": "production_pg",
                "sync_frequency": 360,
                "schedule_type": "auto",
                "succeeded_at": "2024-01-15T10:00:00.000Z",
                "status": {"sync_state": "scheduled"}
              }
            }
        """
        t0 = time.monotonic()
        connectors = await self._get_all_connectors()
        units: list[OrchestrationUnit] = []

        for connector in connectors:
            connector_id = connector["id"]
            try:
                resp = await self._api_get(f"v1/connectors/{connector_id}")
                detail = resp.get("data") or {}
            except Exception:
                logger.warning("Failed to fetch details for connector %s", connector_id)
                detail = connector

            sync_frequency = detail.get("sync_frequency")
            schedule_type = detail.get("schedule_type") or "auto"
            schedule = (
                f"every_{sync_frequency}min" if sync_frequency is not None else schedule_type
            )

            succeeded_raw = detail.get("succeeded_at")
            last_run_at: datetime | None = None
            if succeeded_raw:
                with contextlib.suppress(ValueError, AttributeError):
                    last_run_at = datetime.fromisoformat(succeeded_raw.replace("Z", "+00:00"))

            sync_state = (detail.get("status") or {}).get("sync_state") or None

            units.append(OrchestrationUnit(
                unit_id=f"fivetran://connector/{connector_id}",
                unit_type="connector_sync",
                display_name=detail.get("schema") or connector_id,
                schedule=schedule,
                last_run_at=last_run_at,
                last_run_status=sync_state,
                metadata={
                    "service": detail.get("service") or "",
                    "group_id": detail.get("group_id") or "",
                    "schedule_type": schedule_type,
                },
            ))

        duration_ms = (time.monotonic() - t0) * 1000
        return OrchestrationSnapshot(
            meta=self._make_meta(adapter, AdapterCapability.ORCHESTRATION, len(units), duration_ms),
            units=tuple(units),
        )

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling the Fivetran adapter."""
        return SetupInstructions(
            title="Fivetran REST API Adapter",
            summary=(
                "Extracts connector inventory, table-level lineage (source → destination), "
                "and sync schedules from the Fivetran API."
            ),
            steps=(
                "Log in to the Fivetran dashboard and navigate to Settings → API Config",
                "Copy the API Key and generate an API Secret",
                "Provide api_key and api_secret when constructing FivetranAdapter",
                "The service account needs at least read access to all connectors",
            ),
        )
