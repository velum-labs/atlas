"""Apache Airflow source adapter — community contribution welcome.

This adapter connects to the Airflow REST API to extract DAG metadata and
orchestration task graphs.

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

import base64
import logging
import time
from datetime import datetime
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
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    OrchestrationSnapshot,
    OrchestrationTask,
    OrchestrationUnit,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

logger = logging.getLogger(__name__)

_PAGE_SIZE = 100


class AirflowAdapter(BaseAdapterV2):
    """Apache Airflow source adapter.

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
        configurations found in task metadata.  Returns ``LineageEdge`` records
        with ``edge_kind=CONNECTOR_API``.

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
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _auth_headers(self) -> dict[str, str]:
        """Return Authorization header dict for the configured auth method."""
        if self._auth_token:
            return {"Authorization": f"Bearer {self._auth_token}"}
        # Basic auth
        creds = base64.b64encode(
            f"{self._username}:{self._password}".encode()
        ).decode()
        return {"Authorization": f"Basic {creds}"}

    async def _api_get(
        self, path: str, *, params: dict[str, Any] | None = None
    ) -> Any:
        """Execute a single GET request against ``/api/v1/{path}``."""
        url = f"{self._base_url}/api/v1/{path}"
        client = await self._get_client()
        resp = await client.get(
            url,
            headers=self._auth_headers(),
            params=params,
            timeout=self._timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()

    async def _api_get_all(
        self, path: str, list_key: str, *, extra_params: dict[str, Any] | None = None
    ) -> list[Any]:
        """Paginate through all results for a list endpoint.

        Uses ``offset``/``limit`` pagination until ``total_entries`` is
        exhausted.
        """
        results: list[Any] = []
        offset = 0
        while True:
            params: dict[str, Any] = {"limit": _PAGE_SIZE, "offset": offset}
            if extra_params:
                params.update(extra_params)
            data = await self._api_get(path, params=params)
            page = data.get(list_key, [])
            results.extend(page)
            total = data.get("total_entries", len(results))
            offset += len(page)
            if offset >= total or not page:
                break
        return results

    def _scope_identifiers(self) -> dict[str, str]:
        return {"base_url": self._base_url}

    # ------------------------------------------------------------------
    # v2 protocol — lifecycle
    # ------------------------------------------------------------------

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate connectivity by calling ``GET /api/v1/health``."""
        try:
            data = await self._api_get("health")
        except Exception as exc:
            return ConnectionTestResult(success=False, message=str(exc))
        if data.get("metadatabase", {}).get("status") == "healthy":
            return ConnectionTestResult(success=True, message="Airflow is healthy")
        return ConnectionTestResult(success=False, message=str(data))

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe capability availability via ``GET /api/v1/health`` then ``/api/v1/dags``."""
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities

        # Step 1: health check
        available = True
        message: str | None = None
        try:
            health = await self._api_get("health")
            if health.get("metadatabase", {}).get("status") != "healthy":
                available = False
                message = f"Airflow metadatabase unhealthy: {health}"
        except Exception as exc:
            available = False
            message = str(exc)

        # Step 2: auth validation — attempt a minimal DAGs list call
        if available:
            try:
                await self._api_get("dags", params={"limit": 1, "offset": 0})
            except Exception as exc:
                available = False
                message = str(exc)

        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"base_url": self._base_url},
        )
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
        """DISCOVER: list all DAGs → one DiscoveredContainer per DAG."""
        t0 = time.monotonic()
        dags = await self._api_get_all("dags", "dags")
        containers = tuple(
            DiscoveredContainer(
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
            )
            for dag in dags
        )
        duration_ms = (time.monotonic() - t0) * 1000
        return DiscoverySnapshot(
            meta=self._make_meta(
                adapter, AdapterCapability.DISCOVER, len(containers), duration_ms
            ),
            containers=containers,
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

        For each DAG, fetches all DAG runs (optionally filtered by *since*), then
        fetches task instances for every run and converts them to
        ``ObservedQueryEvent`` records.
        """
        t0 = time.monotonic()
        dags = await self._api_get_all("dags", "dags")

        events: list[ObservedQueryEvent] = []
        for dag in dags:
            dag_id = dag["dag_id"]
            extra: dict[str, Any] = {}
            if since is not None:
                extra["execution_date_gte"] = since.isoformat()

            runs = await self._api_get_all(
                f"dags/{dag_id}/dagRuns", "dag_runs", extra_params=extra
            )
            for run in runs:
                run_id = run["dag_run_id"]
                task_instances = await self._api_get_all(
                    f"dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                    "task_instances",
                )
                for ti in task_instances:
                    start_date = ti.get("start_date")
                    if start_date is None:
                        continue
                    sql = (ti.get("rendered_fields") or {}).get("sql") or ""
                    # ObservedQueryEvent requires non-empty sql
                    if not sql:
                        sql = f"-- task: {ti.get('task_id', '')} operator: {ti.get('operator', '')}"
                    events.append(
                        ObservedQueryEvent(
                            captured_at=datetime.fromisoformat(start_date),
                            sql=sql,
                            source_name=f"airflow/{dag_id}",
                            query_type=ti.get("operator") or "unknown",
                            event_id=f"{dag_id}/{run_id}/{ti['task_id']}",
                            database_name=dag_id,
                            database_user=ti.get("operator") or None,
                            duration_ms=float((ti.get("duration") or 0) * 1000),
                            metadata={
                                "state": ti.get("state") or "",
                                "dag_run_id": run_id,
                                "task_id": ti.get("task_id") or "",
                            },
                        )
                    )

        duration_ms = (time.monotonic() - t0) * 1000
        return TrafficExtractionResult(
            meta=self._make_meta(
                adapter, AdapterCapability.TRAFFIC, len(events), duration_ms
            ),
            events=tuple(events),
        )

    # ------------------------------------------------------------------
    # v2 protocol — LINEAGE
    # ------------------------------------------------------------------

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """LINEAGE: infer cross-DAG data flow from ExternalTaskSensor configs."""
        t0 = time.monotonic()
        dags = await self._api_get_all("dags", "dags")

        edges: list[LineageEdge] = []
        for dag in dags:
            dag_id = dag["dag_id"]
            tasks_data = await self._api_get(f"dags/{dag_id}/tasks")
            for task in tasks_data.get("tasks", []):
                class_name = (task.get("class_ref") or {}).get("class_name", "")
                if class_name == "ExternalTaskSensor":
                    upstream_dag = (task.get("template_fields") or {}).get(
                        "external_dag_id"
                    )
                    if upstream_dag:
                        edges.append(
                            LineageEdge(
                                source_object=f"airflow://{upstream_dag}",
                                target_object=f"airflow://{dag_id}",
                                edge_kind=LineageEdgeKind.CONNECTOR_API,
                                confidence=0.9,
                                metadata={"task_id": task.get("task_id", "")},
                            )
                        )

        duration_ms = (time.monotonic() - t0) * 1000
        return LineageSnapshot(
            meta=self._make_meta(
                adapter, AdapterCapability.LINEAGE, len(edges), duration_ms
            ),
            edges=tuple(edges),
        )

    # ------------------------------------------------------------------
    # v2 protocol — ORCHESTRATION
    # ------------------------------------------------------------------

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """ORCHESTRATION: full task graph for every DAG → OrchestrationSnapshot."""
        t0 = time.monotonic()
        dags = await self._api_get_all("dags", "dags")

        units: list[OrchestrationUnit] = []
        for dag in dags:
            dag_id = dag["dag_id"]

            tasks_data = await self._api_get(f"dags/{dag_id}/tasks")
            tasks = tasks_data.get("tasks", [])

            # Build upstream map by inverting downstream_task_ids
            upstream_map: dict[str, list[str]] = {t["task_id"]: [] for t in tasks}
            for task in tasks:
                for downstream_id in task.get("downstream_task_ids", []):
                    if downstream_id in upstream_map:
                        upstream_map[downstream_id].append(task["task_id"])

            orch_tasks = tuple(
                OrchestrationTask(
                    task_id=task["task_id"],
                    task_type=task.get("operator") or "unknown",
                    upstream_task_ids=tuple(upstream_map.get(task["task_id"], [])),
                    metadata={"trigger_rule": task.get("trigger_rule") or ""},
                )
                for task in tasks
            )

            # Fetch last run for status
            last_run_at: datetime | None = None
            last_run_status: str | None = None
            try:
                runs_data = await self._api_get(
                    f"dags/{dag_id}/dagRuns",
                    params={"limit": 1, "offset": 0, "order_by": "-execution_date"},
                )
                last_runs = runs_data.get("dag_runs", [])
                if last_runs:
                    run = last_runs[0]
                    exec_date = run.get("execution_date")
                    if exec_date:
                        last_run_at = datetime.fromisoformat(exec_date)
                    last_run_status = run.get("state")
            except Exception:
                logger.debug("Could not fetch last run for DAG %s", dag_id)

            schedule_raw = dag.get("schedule_interval")
            schedule: str | None = None
            if schedule_raw:
                if isinstance(schedule_raw, dict):
                    schedule = schedule_raw.get("value") or str(schedule_raw)
                else:
                    schedule = str(schedule_raw)

            units.append(
                OrchestrationUnit(
                    unit_id=f"airflow://{self._base_url}/{dag_id}",
                    unit_type="dag",
                    display_name=dag_id,
                    schedule=schedule,
                    tasks=orch_tasks,
                    last_run_at=last_run_at,
                    last_run_status=last_run_status,
                )
            )

        duration_ms = (time.monotonic() - t0) * 1000
        return OrchestrationSnapshot(
            meta=self._make_meta(
                adapter, AdapterCapability.ORCHESTRATION, len(units), duration_ms
            ),
            units=tuple(units),
        )

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling the Airflow adapter."""
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
