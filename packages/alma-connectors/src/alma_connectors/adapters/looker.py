"""Looker source adapter — community contribution.

This adapter authenticates with the Looker API using OAuth2 client credentials
and extracts project metadata, LookML model definitions, and lineage between
explores and underlying database views.

Looker API reference:
    https://developers.looker.com/api/explorer/4.0/methods

Authentication:
    Looker uses OAuth2 client credentials (client_id + client_secret).
    An access token is obtained via POST /api/4.0/login and refreshed when
    it expires (default lifetime: 1 hour).

Capabilities declared:
    DISCOVER     — list projects / models / explores as containers
    SCHEMA       — field definitions (dimensions, measures) for each explore
    DEFINITIONS  — LookML SQL expressions for models and views
    LINEAGE      — explore → underlying database view / table edges

Example usage::

    adapter = LookerAdapter(
        instance_url="https://mycompany.looker.com",
        client_id="abc123",
        client_secret="s3cr3t",
    )
    snapshot = asyncio.run(adapter.discover(persisted))
"""

from __future__ import annotations

import asyncio
import logging
import time
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
    ColumnSchema,
    DefinitionSnapshot,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    ObjectDefinition,
    SchemaObject,
    SchemaObjectKind,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
)

logger = logging.getLogger(__name__)


class LookerAdapter(BaseAdapterV2):
    """Looker source adapter.

    Implements the SourceAdapterV2 protocol against the Looker API (v4.0).
    LookML models and explores are mapped to containers; field definitions
    become schema objects; LookML SQL expressions drive definitions; and
    join/sql_table_name metadata produces declared lineage edges.

    Capabilities
    ------------
    DISCOVER
        Lists all LookML models via ``GET /api/4.0/lookml_models`` and maps
        projects, models, and explores to ``DiscoveredContainer`` objects.

    SCHEMA
        For each explore, fetches field metadata via
        ``GET /api/4.0/lookml_models/{model}/explores/{explore}`` and maps
        dimensions and measures to ``ColumnSchema`` objects on a
        ``SchemaObject`` of kind ``SEMANTIC_MODEL``.

    DEFINITIONS
        For each explore, extracts the ``sql`` field from dimensions and
        measures and returns one ``ObjectDefinition`` per explore with
        ``definition_language="lookml"``.

    LINEAGE
        Resolves ``sql_table_name`` on the primary view and explore joins to
        emit ``LineageEdge`` records with ``edge_kind=DECLARED`` pointing from
        the database table to the explore URI.

    Args:
        instance_url: Base URL of the Looker instance, e.g.
            ``"https://mycompany.looker.com"``.
        client_id: OAuth2 client ID from Admin → API → API3 Keys.
        client_secret: OAuth2 client secret.
        port: API port (default 19999 for standalone Looker; 443 for Looker
            Cloud behind HTTPS).
        timeout_seconds: Per-request HTTP timeout in seconds (default 30).
    """

    kind = SourceAdapterKindV2.LOOKER

    declared_capabilities: frozenset[AdapterCapability] = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.SCHEMA,
        AdapterCapability.DEFINITIONS,
        AdapterCapability.LINEAGE,
    })

    def __init__(
        self,
        instance_url: str,
        client_id: str,
        client_secret: str,
        port: int = 19999,
        timeout_seconds: int = 30,
    ) -> None:
        if not instance_url or not instance_url.strip():
            raise ValueError("instance_url must be a non-empty string")
        if not client_id or not client_id.strip():
            raise ValueError("client_id must be a non-empty string")
        if not client_secret or not client_secret.strip():
            raise ValueError("client_secret must be a non-empty string")
        self._instance_url = instance_url.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._port = port
        self._timeout_seconds = timeout_seconds
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0
        self._client: httpx.AsyncClient | None = None
        self._token_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scope_identifiers(self) -> dict[str, str]:
        return {"instance_url": self._instance_url}

    def _base_url(self) -> str:
        return f"{self._instance_url}:{self._port}/api/4.0"

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_access_token(self) -> str:
        """Obtain or refresh an OAuth2 access token via POST /api/4.0/login.

        Caches the token and only re-fetches when it is within 60 seconds of
        expiry.  An asyncio.Lock prevents concurrent coroutines from
        double-fetching tokens.

        Returns:
            Valid bearer token string.
        """
        async with self._token_lock:
            now = time.monotonic()
            if self._access_token and now < self._token_expires_at - 60:
                return self._access_token

            client = await self._get_client()
            resp = await client.post(
                f"{self._base_url()}/login",
                data={"client_id": self._client_id, "client_secret": self._client_secret},
                timeout=self._timeout_seconds,
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._token_expires_at = now + data.get("expires_in", 3600)
            return self._access_token

    async def _api_get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Execute an authenticated GET request against the Looker API.

        Automatically refreshes the access token on a 401 response and retries
        the request once.

        Args:
            path: API path relative to /api/4.0/ (no leading slash).
            params: Optional query parameters.

        Returns:
            Parsed JSON response (dict or list).
        """
        token = await self._get_access_token()
        client = await self._get_client()
        resp = await client.get(
            f"{self._base_url()}/{path}",
            headers={"Authorization": f"token {token}"},
            params=params,
            timeout=self._timeout_seconds,
        )
        if resp.status_code == 401:
            # Token expired mid-flight — clear cache and retry once.
            self._access_token = None
            self._token_expires_at = 0.0
            token = await self._get_access_token()
            resp = await client.get(
                f"{self._base_url()}/{path}",
                headers={"Authorization": f"token {token}"},
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
        """Validate credentials by calling ``GET /api/4.0/user`` (whoami).

        Returns:
            ConnectionTestResult indicating success or failure.
        """
        try:
            me = await self._api_get("user")
            return ConnectionTestResult(
                success=True,
                message=f"Looker instance reachable; logged in as {me.get('email', 'unknown')}",
            )
        except Exception as exc:
            return ConnectionTestResult(success=False, message=str(exc))

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe capability availability.

        Validates auth via ``GET /api/4.0/user``, then confirms model access
        via ``GET /api/4.0/lookml_models`` with ``limit=1``.

        Returns:
            Tuple of CapabilityProbeResult — one per requested capability.
        """
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities
        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"instance_url": self._instance_url},
        )

        available = True
        message: str | None = None

        try:
            await self._api_get("user")
        except Exception as exc:
            available = False
            message = f"auth check failed: {exc}"

        if available:
            try:
                await self._api_get("lookml_models", params={"limit": "1"})
            except Exception as exc:
                available = False
                message = f"model access check failed: {exc}"

        return tuple(
            CapabilityProbeResult(
                capability=cap,
                available=available,
                scope=ExtractionScope.GLOBAL,
                scope_context=scope_ctx,
                message=message,
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
        """DISCOVER: enumerate models and explores as containers.

        Fetches ``GET /api/4.0/lookml_models`` and builds three tiers of
        ``DiscoveredContainer``: project, model, explore.

        Returns:
            DiscoverySnapshot with containers for all projects, models, and explores.
        """
        t0 = time.monotonic()
        models = await self._api_get("lookml_models")

        containers: list[DiscoveredContainer] = []
        seen_projects: set[str] = set()

        for model in models:
            project_name = model.get("project_name") or ""
            if project_name and project_name not in seen_projects:
                seen_projects.add(project_name)
                containers.append(DiscoveredContainer(
                    container_id=f"looker://project/{project_name}",
                    container_type="project",
                    display_name=project_name,
                    metadata={},
                ))

            containers.append(DiscoveredContainer(
                container_id=f"looker://model/{model['name']}",
                container_type="model",
                display_name=model["name"],
                metadata={"project": project_name},
            ))

            for explore in model.get("explores", []):
                containers.append(DiscoveredContainer(
                    container_id=f"looker://explore/{model['name']}/{explore['name']}",
                    container_type="explore",
                    display_name=explore.get("label") or explore["name"],
                    metadata={"model": model["name"]},
                ))

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.DISCOVER, len(containers), duration_ms)
        return DiscoverySnapshot(meta=meta, containers=tuple(containers))

    # ------------------------------------------------------------------
    # v2 protocol — SCHEMA
    # ------------------------------------------------------------------

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """SCHEMA: field definitions for each explore → SchemaSnapshotV2.

        For each explore, fetches ``GET /api/4.0/lookml_models/{model}/explores/{explore}``
        and maps dimensions and measures to ``ColumnSchema`` objects on a
        ``SchemaObject`` of kind ``SEMANTIC_MODEL``.

        Returns:
            SchemaSnapshotV2 with one SchemaObject per explore.
        """
        t0 = time.monotonic()
        models = await self._api_get("lookml_models")

        objects: list[SchemaObject] = []
        for model in models:
            model_name = model["name"]
            for explore_stub in model.get("explores", []):
                explore_name = explore_stub["name"]
                explore = await self._api_get(
                    f"lookml_models/{model_name}/explores/{explore_name}",
                    params={"fields": "fields"},
                )
                fields = explore.get("fields", {})

                columns: list[ColumnSchema] = []
                for dim in fields.get("dimensions", []):
                    columns.append(ColumnSchema(
                        name=dim["name"],
                        data_type=dim.get("type") or "unknown",
                        description=dim.get("description") or None,
                    ))
                for measure in fields.get("measures", []):
                    columns.append(ColumnSchema(
                        name=measure["name"],
                        data_type=measure.get("type") or "unknown",
                        description=measure.get("description") or None,
                    ))

                objects.append(SchemaObject(
                    schema_name=model_name,
                    object_name=explore_name,
                    kind=SchemaObjectKind.SEMANTIC_MODEL,
                    columns=tuple(columns),
                ))

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.SCHEMA, len(objects), duration_ms)
        return SchemaSnapshotV2(meta=meta, objects=tuple(objects))

    # ------------------------------------------------------------------
    # v2 protocol — DEFINITIONS
    # ------------------------------------------------------------------

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """DEFINITIONS: LookML SQL expressions → DefinitionSnapshot.

        For each explore, fetches ``GET /api/4.0/lookml_models/{model}/explores/{explore}``
        and concatenates the ``sql`` field from each dimension and measure into
        one ``ObjectDefinition`` per explore.

        Returns:
            DefinitionSnapshot with one ObjectDefinition per explore.
        """
        t0 = time.monotonic()
        models = await self._api_get("lookml_models")

        definitions: list[ObjectDefinition] = []
        for model in models:
            model_name = model["name"]
            for explore_stub in model.get("explores", []):
                explore_name = explore_stub["name"]
                explore = await self._api_get(
                    f"lookml_models/{model_name}/explores/{explore_name}",
                    params={"fields": "fields"},
                )
                fields = explore.get("fields", {})

                sql_parts: list[str] = []
                for dim in fields.get("dimensions", []):
                    sql = (dim.get("sql") or "").strip()
                    if sql:
                        sql_parts.append(f"-- dimension: {dim['name']}\n{sql}")
                for measure in fields.get("measures", []):
                    sql = (measure.get("sql") or "").strip()
                    if sql:
                        sql_parts.append(f"-- measure: {measure['name']}\n{sql}")

                definition_text = "\n\n".join(sql_parts) or f"-- {explore_name}"

                definitions.append(ObjectDefinition(
                    schema_name=model_name,
                    object_name=explore_name,
                    object_kind=SchemaObjectKind.SEMANTIC_MODEL,
                    definition_text=definition_text,
                    definition_language="lookml",
                    metadata={"explore": explore_name, "model": model_name},
                ))

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.DEFINITIONS, len(definitions), duration_ms)
        return DefinitionSnapshot(meta=meta, definitions=tuple(definitions))

    # ------------------------------------------------------------------
    # v2 protocol — LINEAGE
    # ------------------------------------------------------------------

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """LINEAGE: map explores → underlying database tables via sql_table_name.

        For each explore, fetches join metadata and resolves ``sql_table_name``
        on the primary view and all joined views to emit ``LineageEdge`` records
        with ``edge_kind=DECLARED``.

        Returns:
            LineageSnapshot with edges from database tables to Looker explores.
        """
        t0 = time.monotonic()
        models = await self._api_get("lookml_models")

        edges: list[LineageEdge] = []
        for model in models:
            model_name = model["name"]
            for explore_stub in model.get("explores", []):
                explore_name = explore_stub["name"]
                explore = await self._api_get(
                    f"lookml_models/{model_name}/explores/{explore_name}",
                    params={"fields": "name,joins,view_name,sql_table_name"},
                )
                target = f"looker://explore/{model_name}/{explore_name}"

                # Primary view → source table
                primary_table = (explore.get("sql_table_name") or "").strip()
                if primary_table:
                    edges.append(LineageEdge(
                        source_object=primary_table,
                        target_object=target,
                        edge_kind=LineageEdgeKind.DECLARED,
                        confidence=0.95,
                        metadata={"view": explore.get("view_name") or explore_name},
                    ))

                # Joined views → source tables
                for join in explore.get("joins", []):
                    join_table = (join.get("sql_table_name") or "").strip()
                    if join_table:
                        edges.append(LineageEdge(
                            source_object=join_table,
                            target_object=target,
                            edge_kind=LineageEdgeKind.DECLARED,
                            confidence=0.95,
                            metadata={
                                "join_name": join.get("name") or "",
                                "join_type": join.get("type") or "",
                            },
                        ))

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.LINEAGE, len(edges), duration_ms)
        return LineageSnapshot(meta=meta, edges=tuple(edges))

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling the Looker adapter.

        Returns:
            SetupInstructions describing how to configure Looker API access.
        """
        return SetupInstructions(
            title="Looker API Adapter",
            summary=(
                "Extracts LookML project metadata, explore field definitions, "
                "raw LookML source, and database lineage via the Looker API v4.0."
            ),
            steps=(
                "In Looker: Admin → Users → select your service account → Edit → API3 Keys → New API3 Key",
                "Note the Client ID and Client Secret shown (secret is only shown once)",
                "Ensure the service account has the 'Explore' and 'see_lookml' permissions",
                "For DEFINITIONS capability, the account also needs 'develop' or 'see_lookml_files' permission",
                "Provide instance_url (e.g. https://mycompany.looker.com), client_id, and client_secret when constructing LookerAdapter",
                "For standalone Looker, the API port is typically 19999; for Looker Cloud use 443",
            ),
        )
