"""Apache Airflow source adapter stub — community contribution welcome.

This adapter connects to the Airflow REST API to extract DAG metadata and
orchestration task graphs.  All extraction methods are stubbed with clear
TODO comments explaining the Airflow API endpoints and data shapes needed
to complete each capability.

Airflow REST API reference:
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

Authentication:
    Basic auth (username + password) or a Bearer token can be used.
    Pass ``auth_token`` for token-based auth or ``username``/``password``
    for basic auth — the adapter uses whichever is non-None.

Capabilities declared:
    DISCOVER      — list all DAGs as DiscoveredContainers
    TRAFFIC       — query execution / task-instance history
    LINEAGE       — infer upstream→downstream data flow between DAGs
    ORCHESTRATION — full DAG task-graph with schedules and upstream deps

Example usage::

    adapter = AirflowAdapter(
        base_url="https://airflow.example.com",
        auth_token="your-airflow-api-token",
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
    DiscoveredContainer,
    DiscoverySnapshot,
    DefinitionSnapshot,
    ExtractionMeta,
    ExtractionScope,
    LineageSnapshot,
    OrchestrationSnapshot,
    OrchestrationTask,
    OrchestrationUnit,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

logger = logging.getLogger(__name__)


class AirflowAdapter:
    """Community stub — Apache Airflow source adapter.

    Implements the SourceAdapterV2 protocol against the Airflow stable REST API
    (v1, available since Airflow 2.0).

    Capabilities
    ------------
    DISCOVER
        Lists all DAGs via ``GET /api/v1/dags`` and returns one
        ``DiscoveredContainer`` per DAG.

    TRAFFIC
        Reads task-instance execution history via
        ``GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances`` to build
        ``ObservedQueryEvent`` records representing pipeline executions.

    LINEAGE
        Infers cross-DAG data dependencies from ``ExternalTaskSensor``
        configurations and manually declared upstream DAG IDs found in task
        metadata.  Returns ``LineageEdge`` records with
        ``edge_kind=CONNECTOR_API``.

    ORCHESTRATION
        Fetches the full task graph for every DAG via
        ``GET /api/v1/dags/{dag_id}/tasks`` and returns ``OrchestrationUnit``
        objects with ``OrchestrationTask`` records preserving
        ``upstream_task_ids`` ordering.

    Args:
        base_url: Root URL of the Airflow webserver, e.g.
            ``"https://airflow.example.com"``.  Must not include a trailing
            slash.
        auth_token: Bearer token for token-based authentication.  Mutually
            exclusive with ``username``/``password``.
        username: Username for HTTP Basic auth.
        password: Password for HTTP Basic auth.
        timeout_seconds: Per-request HTTP timeout in seconds (default 30).
    """

    kind = SourceAdapterKindV2.AIRFLOW

    declared_capabilities: frozenset[AdapterCapability] = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.TRAFFIC,
        AdapterCapability.LINEAGE,
        AdapterCapability.ORCHESTRATION,
    })

    def __init__(
        self,
        base_url: str,
        auth_token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        if not base_url or not base_url.strip():
            raise ValueError("base_url must be a non-empty string")
        if auth_token is None and (username is None or password is None):
            raise ValueError(
                "Provide either auth_token or both username and password"
            )
        self._base_url = base_url.rstrip("/")
        self._auth_token = auth_token
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

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
            adapter_kind=SourceAdapterKindV2.AIRFLOW,
            capability=capability,
            scope_context=ScopeContext(
                scope=ExtractionScope.GLOBAL,
                identifiers={"base_url": self._base_url},
            ),
            captured_at=datetime.now(UTC),
            duration_ms=duration_ms,
            row_count=row_count,
        )

    def _get_http_session(self) -> Any:
        """Build and return an authenticated HTTP session.

        TODO: Import ``requests`` (or ``httpx`` for async) and construct a
        session with the appropriate auth headers:

            import requests
            session = requests.Session()
            if self._auth_token:
                session.headers["Authorization"] = f"Bearer {self._auth_token}"
            else:
                session.auth = (self._username, self._password)
            session.headers["Content-Type"] = "application/json"
            return session
        """
        raise NotImplementedError(
            "AirflowAdapter._get_http_session() is not implemented. "
            "See the docstring for implementation guidance."
        )

    def _api_get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Execute a GET request against the Airflow REST API.

        TODO: Use the session from ``_get_http_session()`` to call
        ``{base_url}/api/v1/{path}``.  Handle pagination using the
        ``offset`` / ``limit`` query parameters:

            url = f"{self._base_url}/api/v1/{path}"
            resp = session.get(url, params=params, timeout=self._timeout_seconds)
            resp.raise_for_status()
            return resp.json()

        For paginated endpoints (e.g., ``/dags``, ``/dagRuns``), iterate
        until ``total_entries`` is exhausted.
        """
        raise NotImplementedError(
            "AirflowAdapter._api_get() is not implemented. "
            "See the docstring for implementation guidance."
        )

    # ------------------------------------------------------------------
    # v2 protocol — lifecycle
    # ------------------------------------------------------------------

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate connectivity by calling ``GET /api/v1/health``.

        TODO: Call ``self._api_get("health")`` and inspect the response:

            data = self._api_get("health")
            if data.get("metadatabase", {}).get("status") == "healthy":
                return ConnectionTestResult(success=True, message="Airflow is healthy")
            return ConnectionTestResult(success=False, message=str(data))

        Returns:
            ConnectionTestResult indicating success or failure.
        """
        raise NotImplementedError(
            "AirflowAdapter.test_connection() is not implemented. "
            "Call GET /api/v1/health and inspect the metadatabase.status field."
        )

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe capability availability by attempting a lightweight API call.

        TODO: Call ``GET /api/v1/config`` or ``GET /api/v1/health`` to confirm
        connectivity, then mark all requested capabilities available/unavailable
        based on the result.  TRAFFIC may additionally require checking that the
        Airflow instance has ``[core] store_serialized_dags = True`` or the
        Task Instances API is enabled.

        Returns:
            Tuple of CapabilityProbeResult — one per requested capability.
        """
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities
        raise NotImplementedError(
            "AirflowAdapter.probe() is not implemented. "
            f"Must return a CapabilityProbeResult for each of: {caps_to_probe}"
        )

    # ------------------------------------------------------------------
    # v2 protocol — DISCOVER
    # ------------------------------------------------------------------

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: list all DAGs → one DiscoveredContainer per DAG.

        Airflow API endpoint:
            GET /api/v1/dags?limit=100&offset=0

        Response shape::

            {
              "dags": [
                {
                  "dag_id": "my_pipeline",
                  "description": "Loads raw events",
                  "is_active": true,
                  "is_paused": false,
                  "schedule_interval": {"__type": "CronExpression", "value": "0 * * * *"},
                  "tags": [{"name": "etl"}],
                  "owners": ["data-eng"],
                  "next_dagrun": "2024-01-02T00:00:00+00:00"
                },
                ...
              ],
              "total_entries": 42
            }

        TODO: Paginate through all DAGs and build a DiscoveredContainer for each:

            containers = []
            for dag in paginated_dags:
                containers.append(DiscoveredContainer(
                    container_id=f"airflow://{self._base_url}/{dag['dag_id']}",
                    container_type="dag",
                    display_name=dag["dag_id"],
                    metadata={
                        "description": dag.get("description") or "",
                        "is_paused": dag.get("is_paused", False),
                        "owners": dag.get("owners", []),
                        "tags": [t["name"] for t in dag.get("tags", [])],
                        "schedule": str(dag.get("schedule_interval") or ""),
                    },
                ))

        Returns:
            DiscoverySnapshot with one container per DAG.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "AirflowAdapter.discover() is not implemented. "
            "Call GET /api/v1/dags (paginated) and convert each DAG to a DiscoveredContainer. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — SCHEMA (not declared — raises NotImplementedError)
    # ------------------------------------------------------------------

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """Not supported — Airflow is an orchestrator, not a data warehouse.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "AirflowAdapter does not support SCHEMA extraction "
            "(AdapterCapability.SCHEMA is not in declared_capabilities)"
        )

    # ------------------------------------------------------------------
    # v2 protocol — DEFINITIONS (not declared)
    # ------------------------------------------------------------------

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """Not supported — Airflow DAG source is Python, not SQL DDL.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "AirflowAdapter does not support DEFINITIONS extraction "
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
        """TRAFFIC: fetch task-instance execution logs → ObservedQueryEvent records.

        Airflow API endpoints:
            GET /api/v1/dags/{dag_id}/dagRuns?execution_date_gte={since}
            GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances

        Response shape (taskInstances)::

            {
              "task_instances": [
                {
                  "task_id": "load_users",
                  "dag_id": "my_pipeline",
                  "dag_run_id": "scheduled__2024-01-01T00:00:00+00:00",
                  "start_date": "2024-01-01T00:01:03.123456+00:00",
                  "end_date":   "2024-01-01T00:04:17.654321+00:00",
                  "duration":   194.531,
                  "state": "success",
                  "operator": "BigQueryOperator",
                  "rendered_fields": {"sql": "SELECT ..."}
                }
              ]
            }

        TODO: For each DAG run since *since*, collect task instances and convert
        them to ObservedQueryEvent records.  Use rendered_fields.sql when present
        to populate the query_text field:

            events = []
            for ti in task_instances:
                events.append(ObservedQueryEvent(
                    event_id=f"{ti['dag_id']}/{ti['dag_run_id']}/{ti['task_id']}",
                    executed_at=datetime.fromisoformat(ti["start_date"]),
                    query_text=ti.get("rendered_fields", {}).get("sql") or "",
                    duration_ms=int((ti.get("duration") or 0) * 1000),
                    user_name=ti.get("operator") or "unknown",
                    database_name=ti["dag_id"],
                    schema_name="",
                    row_count=None,
                ))

        Args:
            adapter: Persisted adapter record.
            since: Only fetch runs executed after this timestamp.

        Returns:
            TrafficExtractionResult with ObservedQueryEvent per task instance.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "AirflowAdapter.extract_traffic() is not implemented. "
            "Fetch DAG runs and task instances from the Airflow API. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — LINEAGE
    # ------------------------------------------------------------------

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """LINEAGE: infer cross-DAG data flow from ExternalTaskSensor configs.

        Airflow API endpoint:
            GET /api/v1/dags/{dag_id}/tasks

        Response shape (tasks)::

            {
              "tasks": [
                {
                  "task_id": "wait_for_upstream",
                  "class_ref": {"module_path": "...", "class_name": "ExternalTaskSensor"},
                  "extra_links": [],
                  "template_fields": {"external_dag_id": "upstream_pipeline", ...}
                }
              ]
            }

        TODO: Iterate all DAG tasks and identify ExternalTaskSensor operators.
        For each, emit a LineageEdge from the upstream DAG to this DAG:

            from alma_connectors.source_adapter_v2 import LineageEdge, LineageEdgeKind
            edges = []
            for dag in dags:
                for task in dag_tasks:
                    if task["class_ref"]["class_name"] == "ExternalTaskSensor":
                        upstream_dag = task["template_fields"]["external_dag_id"]
                        edges.append(LineageEdge(
                            source_object=f"airflow://{upstream_dag}",
                            target_object=f"airflow://{dag['dag_id']}",
                            edge_kind=LineageEdgeKind.CONNECTOR_API,
                            confidence=0.9,
                            metadata={"task_id": task["task_id"]},
                        ))

        Returns:
            LineageSnapshot with one edge per detected cross-DAG dependency.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "AirflowAdapter.extract_lineage() is not implemented. "
            "Identify ExternalTaskSensor tasks and map them to LineageEdges. "
            "See method docstring for the expected request/response shape."
        )

    # ------------------------------------------------------------------
    # v2 protocol — ORCHESTRATION
    # ------------------------------------------------------------------

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """ORCHESTRATION: full task graph for every DAG → OrchestrationSnapshot.

        Airflow API endpoints:
            GET /api/v1/dags                       — all DAGs (paginated)
            GET /api/v1/dags/{dag_id}/tasks        — tasks for one DAG
            GET /api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date
                                                   — last run (for status)

        Response shape (tasks for one DAG)::

            {
              "tasks": [
                {
                  "task_id": "extract_users",
                  "downstream_task_ids": ["transform_users"],
                  "depends_on_past": false,
                  "operator": "PythonOperator",
                  "trigger_rule": "all_success"
                },
                {
                  "task_id": "transform_users",
                  "downstream_task_ids": ["load_users"],
                  ...
                }
              ]
            }

        TODO: Build an OrchestrationUnit per DAG, then an OrchestrationTask per
        task.  Derive ``upstream_task_ids`` by inverting ``downstream_task_ids``:

            # Build reverse mapping: task_id → [upstream task_ids]
            upstream_map: dict[str, list[str]] = {t["task_id"]: [] for t in tasks}
            for task in tasks:
                for downstream_id in task.get("downstream_task_ids", []):
                    upstream_map[downstream_id].append(task["task_id"])

            orch_tasks = tuple(
                OrchestrationTask(
                    task_id=task["task_id"],
                    task_type=task.get("operator") or "unknown",
                    upstream_task_ids=tuple(upstream_map.get(task["task_id"], [])),
                    metadata={"trigger_rule": task.get("trigger_rule", "")},
                )
                for task in tasks
            )
            unit = OrchestrationUnit(
                unit_id=f"airflow://{self._base_url}/{dag['dag_id']}",
                unit_type="dag",
                display_name=dag["dag_id"],
                schedule=str(dag.get("schedule_interval") or ""),
                tasks=orch_tasks,
                last_run_at=last_run_at,
                last_run_status=last_run_status,
            )

        Returns:
            OrchestrationSnapshot with one OrchestrationUnit per DAG.
        """
        t0 = time.monotonic()
        raise NotImplementedError(
            "AirflowAdapter.extract_orchestration() is not implemented. "
            "Fetch tasks per DAG and build OrchestrationUnit/OrchestrationTask objects. "
            "See method docstring for the expected request/response shape."
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
        """Not supported — Airflow is an orchestrator and does not execute SQL.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "AirflowAdapter does not support query execution"
        )

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling the Airflow adapter.

        Returns:
            SetupInstructions describing how to configure Airflow API access.
        """
        return SetupInstructions(
            title="Apache Airflow REST API Adapter",
            summary=(
                "Extracts DAG metadata, task graphs, and execution history "
                "from the Airflow stable REST API (v1, requires Airflow 2.0+)."
            ),
            steps=(
                "Enable the REST API in airflow.cfg: [api] auth_backends = airflow.api.auth.backend.basic_auth",
                "Create a dedicated service account with Viewer role in Airflow Admin → Security → Users",
                "Generate an API token or note the username/password for basic auth",
                "Provide base_url (e.g. https://airflow.example.com) and credentials when constructing AirflowAdapter",
                "Ensure network connectivity from the alma-connectors host to the Airflow webserver port (default 8080)",
            ),
        )
