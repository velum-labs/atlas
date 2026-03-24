"""Metabase source adapter stub — community contribution welcome.

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

from alma_connectors.source_adapter import (
    ConnectionTestResult,
    ObservedQueryEvent,
    PersistedSourceAdapter,
    QueryResult,
    SetupInstructions,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    ColumnSchema,
    DefinitionSnapshot,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionMeta,
    ExtractionScope,
    LineageSnapshot,
    OrchestrationSnapshot,
    SchemaObject,
    SchemaObjectKind,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

logger = logging.getLogger(__name__)


class MetabaseAdapter:
    """Community stub — Metabase source adapter.

    Implements the SourceAdapterV2 protocol against the Metabase REST API.
    Connected databases and collections are mapped to ``DiscoveredContainer``
    objects; field metadata drives SCHEMA; and query execution logs drive
    TRAFFIC.

    Capabilities
    ------------
    DISCOVER
        Lists all connected databases via ``GET /api/database`` and all
        collections via ``GET /api/collection``.  Each database and root-level
        collection becomes a ``DiscoveredContainer``.

    SCHEMA
        For each database, fetches table list via
        ``GET /api/database/{db_id}/metadata`` and maps tables + fields to
        ``SchemaObject`` / ``ColumnSchema`` records.

    TRAFFIC
        Reads query execution history from the Metabase internal API:
        ``GET /api/activity`` (older) or the Audit/Analytics endpoints
        (Metabase Pro/Enterprise).  Returns ``ObservedQueryEvent`` records
        representing user queries against connected databases.

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_meta(
        self,
        adapter: PersistedSourceAdapter,
        capability: AdapterCapability,
        row_count: int,
        duration_ms: float,
    ) -> ExtractionMeta:
        return ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.METABASE,
            capability=capability,
            scope_context=ScopeContext(
                scope=ExtractionScope.GLOBAL,
                identifiers={"instance_url": self._instance_url},
            ),
            captured_at=datetime.now(UTC),
            duration_ms=duration_ms,
            row_count=row_count,
        )

    def _get_auth_headers(self) -> dict[str, str]:
        """Return the correct authentication headers for API requests.

        TODO: If api_key is set, return the x-api-key header (Metabase 0.49+).
        Otherwise, obtain a session token via POST /api/session and return the
        X-Metabase-Session header.  Cache the session token:

            if self._api_key:
                return {"x-api-key": self._api_key}

            if not self._session_token:
                import requests
                resp = requests.post(
                    f"{self._instance_url}/api/session",
                    json={"username": self._username, "password": self._password},
                    timeout=self._timeout_seconds,
                )
                resp.raise_for_status()
                self._session_token = resp.json()["id"]
            return {"X-Metabase-Session": self._session_token}

        Returns:
            Dict of HTTP headers for authentication.
        """
        raise NotImplementedError(
            "MetabaseAdapter._get_auth_headers() is not implemented. "
            "Return {'x-api-key': ...} or obtain a session token via POST /api/session."
        )

    def _api_get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Execute an authenticated GET request against the Metabase API.

        TODO: Build the request with auth headers from ``_get_auth_headers()``:

            import requests
            resp = requests.get(
                f"{self._instance_url}/api/{path}",
                headers=self._get_auth_headers(),
                params=params,
                timeout=self._timeout_seconds,
            )
            resp.raise_for_status()
            return resp.json()

        Args:
            path: API path relative to /api/ (no leading slash).
            params: Optional query parameters.

        Returns:
            Parsed JSON response (dict or list).
        """
        raise NotImplementedError(
            "MetabaseAdapter._api_get() is not implemented. "
            "See the docstring for implementation guidance."
        )

    # ------------------------------------------------------------------
    # v2 protocol — lifecycle
    # ------------------------------------------------------------------

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate connectivity by calling ``GET /api/user/current``.

        TODO: Call ``self._api_get("user/current")`` and inspect the response:

            try:
                me = self._api_get("user/current")
                return ConnectionTestResult(
                    success=True,
                    message=f"Metabase reachable; logged in as {me.get('email', 'unknown')}",
                )
            except Exception as exc:
                return ConnectionTestResult(success=False, message=str(exc))

        Returns:
            ConnectionTestResult indicating success or failure.
        """
        raise NotImplementedError(
            "MetabaseAdapter.test_connection() is not implemented. "
            "Call GET /api/user/current to verify credentials and connectivity."
        )

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe capability availability by verifying API access.

        TODO: Attempt ``self._api_get("database")`` to confirm connectivity.
        For TRAFFIC, check whether the instance is running Pro/Enterprise by
        calling ``GET /api/setting`` and inspecting ``"token-features"`` for
        ``"audit-app-subscription"``.  Return a CapabilityProbeResult per cap.

        Returns:
            Tuple of CapabilityProbeResult — one per requested capability.
        """
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities
        raise NotImplementedError(
            "MetabaseAdapter.probe() is not implemented. "
            f"Must return a CapabilityProbeResult for each of: {caps_to_probe}"
        )

    # ------------------------------------------------------------------
    # v2 protocol — DISCOVER
    # ------------------------------------------------------------------

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: list databases and collections → DiscoveredContainers.

        Metabase API endpoints:
            GET /api/database
            GET /api/collection

        Response shape (database list)::

            {
              "data": [
                {
                  "id": 1,
                  "name": "Production PostgreSQL",
                  "engine": "postgres",
                  "details": {"host": "db.example.com", "port": 5432, "dbname": "app"},
                  "is_full_sync": true,
                  "is_sample": false
                }
              ]
            }

        Response shape (collection list)::

            [
              {
                "id": 1,
                "name": "Our analytics",
                "slug": "our_analytics",
                "location": "/",
                "archived": false
              }
            ]

        TODO: Build a DiscoveredContainer for each database and each top-level
        (non-archived) collection:

            containers = []
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
                    metadata={"slug": coll.get("slug") or "", "location": coll.get("location") or "/"},
                ))

        Returns:
            DiscoverySnapshot with containers for all databases and collections.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "MetabaseAdapter.discover() is not implemented. "
            "Fetch /api/database and /api/collection and convert to DiscoveredContainers. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — SCHEMA
    # ------------------------------------------------------------------

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """SCHEMA: table and field definitions for each connected database.

        Metabase API endpoint:
            GET /api/database/{db_id}/metadata?include_hidden=false

        Response shape::

            {
              "id": 1,
              "name": "Production PostgreSQL",
              "tables": [
                {
                  "id": 12,
                  "name": "orders",
                  "schema": "public",
                  "display_name": "Orders",
                  "entity_type": "entity/TransactionTable",
                  "fields": [
                    {
                      "id": 101,
                      "name": "id",
                      "display_name": "ID",
                      "base_type": "type/Integer",
                      "semantic_type": "type/PK",
                      "description": null
                    },
                    {
                      "id": 102,
                      "name": "user_id",
                      "base_type": "type/Integer",
                      "semantic_type": "type/FK"
                    }
                  ]
                }
              ]
            }

        TODO: For each database, fetch metadata and convert tables to SchemaObjects.
        Use ``base_type`` for ``data_type``; strip the ``"type/"`` prefix for readability:

            objects = []
            for table in db_metadata["tables"]:
                columns = []
                for field in table.get("fields", []):
                    base_type = (field.get("base_type") or "unknown").replace("type/", "")
                    columns.append(ColumnSchema(
                        name=field["name"],
                        data_type=base_type,
                        description=field.get("description") or None,
                    ))
                kind = SchemaObjectKind.VIEW if "view" in (table.get("entity_type") or "").lower() else SchemaObjectKind.TABLE
                objects.append(SchemaObject(
                    schema_name=table.get("schema") or db_name,
                    object_name=table["name"],
                    kind=kind,
                    columns=tuple(columns),
                    description=table.get("description") or None,
                ))

        Returns:
            SchemaSnapshotV2 with one SchemaObject per table across all databases.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "MetabaseAdapter.extract_schema() is not implemented. "
            "Fetch /api/database/{id}/metadata and map tables to SchemaObjects. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — DEFINITIONS (not declared)
    # ------------------------------------------------------------------

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """Not supported — Metabase does not expose DDL definitions.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "MetabaseAdapter does not support DEFINITIONS extraction "
            "(AdapterCapability.DEFINITIONS is not in declared_capabilities)"
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

        Metabase API endpoint (OSS, limited):
            GET /api/activity?limit=1000

        Metabase API endpoint (Pro/Enterprise — full audit log):
            GET /api/ee/audit-app/query_execution?start_date={since}&limit=1000

        OSS response shape (activity feed)::

            [
              {
                "id": 1234,
                "topic": "card-query",
                "timestamp": "2024-01-15T10:23:45.123Z",
                "user": {"id": 5, "email": "analyst@example.com"},
                "model": "card",
                "model_id": 77,
                "details": {"running_time": 1250, "result_rows": 42}
              }
            ]

        Enterprise query_execution response shape::

            {
              "data": [
                {
                  "query_hash": "abc...",
                  "started_at": "2024-01-15T10:23:45.123Z",
                  "running_time": 1250,
                  "result_rows": 42,
                  "native": "SELECT ...",
                  "executor_id": 5,
                  "card_name": "Revenue by Month",
                  "database_id": 1
                }
              ]
            }

        TODO: Prefer the Enterprise endpoint when available; fall back to the
        OSS activity feed.  Filter by ``since`` when provided:

            events = []
            for entry in activity_entries:
                if since and datetime.fromisoformat(entry["timestamp"]) < since:
                    continue
                events.append(ObservedQueryEvent(
                    event_id=str(entry["id"]),
                    executed_at=datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00")),
                    query_text=entry.get("details", {}).get("native") or "",
                    duration_ms=entry.get("details", {}).get("running_time") or 0,
                    user_name=entry.get("user", {}).get("email") or "unknown",
                    database_name=str(entry.get("model_id") or ""),
                    schema_name="",
                    row_count=entry.get("details", {}).get("result_rows"),
                ))

        Args:
            adapter: Persisted adapter record.
            since: Only fetch events after this timestamp.

        Returns:
            TrafficExtractionResult with ObservedQueryEvent per query execution.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "MetabaseAdapter.extract_traffic() is not implemented. "
            "Fetch query execution history from /api/activity or the enterprise audit endpoint. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — LINEAGE (not declared)
    # ------------------------------------------------------------------

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """Not supported in this stub — Metabase does not expose lineage natively.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "MetabaseAdapter does not support LINEAGE extraction "
            "(AdapterCapability.LINEAGE is not in declared_capabilities)"
        )

    # ------------------------------------------------------------------
    # v2 protocol — ORCHESTRATION (not declared)
    # ------------------------------------------------------------------

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """Not supported — Metabase is a BI tool, not an orchestration system.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "MetabaseAdapter does not support ORCHESTRATION extraction "
            "(AdapterCapability.ORCHESTRATION is not in declared_capabilities)"
        )

    # ------------------------------------------------------------------
    # v2 protocol — utility
    # ------------------------------------------------------------------

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
        dry_run: bool = False,
    ) -> QueryResult:
        """Not supported in this stub.

        Note: Metabase supports ad-hoc native queries via
        ``POST /api/dataset`` — implementors may add this capability.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "MetabaseAdapter does not support query execution in this stub"
        )

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling the Metabase adapter.

        Returns:
            SetupInstructions describing how to configure Metabase API access.
        """
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
