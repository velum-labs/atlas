"""Looker source adapter stub — community contribution welcome.

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
    DEFINITIONS  — raw LookML source code for models and views
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

import logging
import time
from datetime import UTC, datetime
from typing import Any

from alma_connectors.source_adapter import (
    ConnectionTestResult,
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
    ObjectDefinition,
    OrchestrationSnapshot,
    SchemaObject,
    SchemaObjectKind,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

logger = logging.getLogger(__name__)


class LookerAdapter:
    """Community stub — Looker source adapter.

    Implements the SourceAdapterV2 protocol against the Looker API (v4.0).
    LookML projects → models → explores are mapped to containers; field
    definitions become schema objects; and raw LookML source drives definitions.

    Capabilities
    ------------
    DISCOVER
        Lists all LookML projects via ``GET /api/4.0/all_projects``, and for
        each project fetches models (``GET /api/4.0/all_lookml_models``) and
        explores.  Each project, model, and explore becomes a
        ``DiscoveredContainer``.

    SCHEMA
        For each explore, fetches the field explorer via
        ``GET /api/4.0/lookml_model/{model}/explore/{explore}`` and maps
        dimensions and measures to ``ColumnSchema`` objects on a
        ``SchemaObject`` of kind ``SEMANTIC_MODEL``.

    DEFINITIONS
        Fetches raw LookML source files via
        ``GET /api/4.0/project/{project_id}/files/all`` and returns one
        ``ObjectDefinition`` per file with ``definition_language="lookml"``.

    LINEAGE
        Resolves ``sql_table_name`` and ``derived_table.sql`` in each view to
        map explores back to the underlying database tables, emitting
        ``LineageEdge`` records with ``edge_kind=CONNECTOR_API``.

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
            adapter_kind=SourceAdapterKindV2.LOOKER,
            capability=capability,
            scope_context=ScopeContext(
                scope=ExtractionScope.GLOBAL,
                identifiers={"instance_url": self._instance_url},
            ),
            captured_at=datetime.now(UTC),
            duration_ms=duration_ms,
            row_count=row_count,
        )

    def _get_access_token(self) -> str:
        """Obtain or refresh an OAuth2 access token.

        TODO: POST credentials to /api/4.0/login to exchange client_id and
        client_secret for a bearer token.  Cache the token and refresh when
        it is within 60 seconds of expiry:

            import requests, time
            now = time.monotonic()
            if self._access_token and now < self._token_expires_at - 60:
                return self._access_token

            resp = requests.post(
                f"{self._instance_url}:{self._port}/api/4.0/login",
                data={"client_id": self._client_id, "client_secret": self._client_secret},
                timeout=self._timeout_seconds,
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._token_expires_at = now + data.get("expires_in", 3600)
            return self._access_token

        Returns:
            Valid bearer token string.
        """
        raise NotImplementedError(
            "LookerAdapter._get_access_token() is not implemented. "
            "POST to /api/4.0/login with client_id and client_secret."
        )

    def _api_get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Execute an authenticated GET request against the Looker API.

        TODO: Include the bearer token in the Authorization header:

            import requests
            token = self._get_access_token()
            resp = requests.get(
                f"{self._instance_url}:{self._port}/api/4.0/{path}",
                headers={"Authorization": f"token {token}"},
                params=params,
                timeout=self._timeout_seconds,
            )
            resp.raise_for_status()
            return resp.json()

        Args:
            path: API path relative to /api/4.0/ (no leading slash).
            params: Optional query parameters.

        Returns:
            Parsed JSON response (dict or list).
        """
        raise NotImplementedError(
            "LookerAdapter._api_get() is not implemented. "
            "See the docstring for implementation guidance."
        )

    # ------------------------------------------------------------------
    # v2 protocol — lifecycle
    # ------------------------------------------------------------------

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate credentials by calling ``GET /api/4.0/user`` (me endpoint).

        TODO: Call ``self._api_get("user")`` after obtaining a token.  A 200
        response confirms both authentication and network connectivity:

            try:
                me = self._api_get("user")
                return ConnectionTestResult(
                    success=True,
                    message=f"Looker instance reachable; logged in as {me.get('email', 'unknown')}",
                )
            except Exception as exc:
                return ConnectionTestResult(success=False, message=str(exc))

        Returns:
            ConnectionTestResult indicating success or failure.
        """
        raise NotImplementedError(
            "LookerAdapter.test_connection() is not implemented. "
            "Call GET /api/4.0/user and check for a 200 response."
        )

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe capability availability by verifying API access.

        TODO: Attempt ``self._api_get("all_projects")`` to confirm connectivity,
        then check permission flags in the authenticated user's roles to confirm
        whether DEFINITIONS (requires access to project files) is available.
        Return one CapabilityProbeResult per requested capability.

        Returns:
            Tuple of CapabilityProbeResult — one per requested capability.
        """
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities
        raise NotImplementedError(
            "LookerAdapter.probe() is not implemented. "
            f"Must return a CapabilityProbeResult for each of: {caps_to_probe}"
        )

    # ------------------------------------------------------------------
    # v2 protocol — DISCOVER
    # ------------------------------------------------------------------

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: enumerate projects → models → explores as containers.

        Looker API endpoints:
            GET /api/4.0/all_projects
            GET /api/4.0/all_lookml_models?fields=name,project_name,explores

        Response shape (all_projects)::

            [
              {
                "id": "my_project",
                "name": "my_project",
                "pr_mode": "off",
                "git_remote_url": "git@github.com:acme/lookml.git"
              }
            ]

        Response shape (all_lookml_models)::

            [
              {
                "name": "ecommerce",
                "project_name": "my_project",
                "explores": [
                  {"name": "orders", "label": "Orders"},
                  {"name": "users", "label": "Users"}
                ]
              }
            ]

        TODO: Build three tiers of DiscoveredContainer — project, model, explore:

            containers = []
            for project in projects:
                containers.append(DiscoveredContainer(
                    container_id=f"looker://project/{project['id']}",
                    container_type="project",
                    display_name=project["id"],
                    metadata={"git_remote_url": project.get("git_remote_url") or ""},
                ))
            for model in models:
                containers.append(DiscoveredContainer(
                    container_id=f"looker://model/{model['name']}",
                    container_type="model",
                    display_name=model["name"],
                    metadata={"project": model.get("project_name") or ""},
                ))
                for explore in model.get("explores", []):
                    containers.append(DiscoveredContainer(
                        container_id=f"looker://explore/{model['name']}/{explore['name']}",
                        container_type="explore",
                        display_name=explore.get("label") or explore["name"],
                        metadata={"model": model["name"]},
                    ))

        Returns:
            DiscoverySnapshot with containers for all projects, models, and explores.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "LookerAdapter.discover() is not implemented. "
            "Fetch all_projects and all_lookml_models and convert to DiscoveredContainers. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — SCHEMA
    # ------------------------------------------------------------------

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """SCHEMA: field definitions for each explore → SchemaSnapshotV2.

        Looker API endpoint:
            GET /api/4.0/lookml_model/{model_name}/explore/{explore_name}
              ?fields=fields.dimensions.name,fields.dimensions.type,
                      fields.measures.name,fields.measures.type,
                      fields.dimensions.description

        Response shape (explore field metadata)::

            {
              "name": "orders",
              "model_name": "ecommerce",
              "fields": {
                "dimensions": [
                  {"name": "orders.id",   "type": "number", "description": "Order PK"},
                  {"name": "orders.date", "type": "date",   "description": null}
                ],
                "measures": [
                  {"name": "orders.count", "type": "count", "description": "Total orders"}
                ]
              }
            }

        TODO: For each explore, build a SchemaObject of kind SEMANTIC_MODEL with
        ColumnSchema entries for each dimension and measure:

            columns = []
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
            obj = SchemaObject(
                schema_name=model_name,
                object_name=explore_name,
                kind=SchemaObjectKind.SEMANTIC_MODEL,
                columns=tuple(columns),
            )

        Returns:
            SchemaSnapshotV2 with one SchemaObject per explore.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "LookerAdapter.extract_schema() is not implemented. "
            "Fetch field definitions for each explore and map to SchemaObjects. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — DEFINITIONS
    # ------------------------------------------------------------------

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """DEFINITIONS: raw LookML source files → DefinitionSnapshot.

        Looker API endpoints:
            GET /api/4.0/all_projects                          — list projects
            GET /api/4.0/project/{project_id}/files/all       — list files
            GET /api/4.0/project/{project_id}/files/content?file_id={file_id}
                                                               — file content

        Response shape (all files for a project)::

            [
              {"id": "models/ecommerce.model.lkml", "title": "ecommerce", "type": "model"},
              {"id": "views/orders.view.lkml",       "title": "orders",    "type": "view"}
            ]

        TODO: For each project, fetch file list and then content.  Use the file
        type ("model" vs "view") to set schema_name and object_name:

            for file_meta in project_files:
                content_resp = self._api_get(
                    f"project/{project_id}/files/content",
                    params={"file_id": file_meta["id"]},
                )
                definitions.append(ObjectDefinition(
                    schema_name=project_id,
                    object_name=file_meta["id"],
                    object_kind=SchemaObjectKind.SEMANTIC_MODEL,
                    definition_text=content_resp.get("content") or "",
                    definition_language="lookml",
                    metadata={"file_type": file_meta.get("type") or ""},
                ))

        Returns:
            DefinitionSnapshot with one ObjectDefinition per LookML file.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "LookerAdapter.extract_definitions() is not implemented. "
            "Fetch raw LookML file content from each project and wrap in ObjectDefinitions. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — TRAFFIC (not declared)
    # ------------------------------------------------------------------

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        """Not supported — Looker TRAFFIC extraction is not declared.

        Note: Looker System Activity explores (``system__activity.history``) do
        contain query history, but that capability is not declared in this stub.
        Future implementors may add AdapterCapability.TRAFFIC by querying
        ``GET /api/4.0/queries/run/json`` against the ``history`` explore.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "LookerAdapter does not declare TRAFFIC extraction. "
            "See method docstring for notes on implementing it via system__activity."
        )

    # ------------------------------------------------------------------
    # v2 protocol — LINEAGE
    # ------------------------------------------------------------------

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """LINEAGE: map explores → underlying database tables via view sql_table_name.

        Looker API endpoint:
            GET /api/4.0/lookml_model/{model_name}/explore/{explore_name}
              ?fields=name,joins,view_name,sql_table_name

        Response shape (explore with joins)::

            {
              "name": "orders",
              "model_name": "ecommerce",
              "view_name": "orders",
              "joins": [
                {
                  "name": "users",
                  "sql_table_name": "analytics.users",
                  "type": "left_outer",
                  "sql_on": "${users.id} = ${orders.user_id}"
                }
              ]
            }

        TODO: For each explore, collect the primary view and all joined views.
        Resolve ``sql_table_name`` to emit LineageEdge records pointing from the
        database table to the explore:

            from alma_connectors.source_adapter_v2 import LineageEdge, LineageEdgeKind
            edges = []
            for explore in explores:
                target = f"looker://explore/{model_name}/{explore['name']}"
                for join in ([explore] + explore.get("joins", [])):
                    table = join.get("sql_table_name", "")
                    if table:
                        edges.append(LineageEdge(
                            source_object=table,
                            target_object=target,
                            edge_kind=LineageEdgeKind.CONNECTOR_API,
                            confidence=0.95,
                            metadata={"join_type": join.get("type") or ""},
                        ))

        Returns:
            LineageSnapshot with edges from database tables to Looker explores.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "LookerAdapter.extract_lineage() is not implemented. "
            "Resolve sql_table_name in explore/join metadata to LineageEdges. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — ORCHESTRATION (not declared)
    # ------------------------------------------------------------------

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """Not supported — Looker is a BI tool, not an orchestration system.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "LookerAdapter does not support ORCHESTRATION extraction "
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

        Note: Looker does support running queries via ``POST /api/4.0/queries``
        followed by ``GET /api/4.0/queries/{query_id}/run/json``, but that is
        not implemented here.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "LookerAdapter does not support query execution in this stub"
        )

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
