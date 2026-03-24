"""Tests for AirflowAdapter."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from alma_connectors import (
    ExternalSecretRef,
    PersistedSourceAdapter,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.adapters.airflow import AirflowAdapter
from alma_connectors.source_adapter import BigQueryAdapterConfig
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    LineageEdgeKind,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_ADAPTER_ID = str(uuid4())
_BASE_URL = "https://airflow.example.com"


def _make_adapter() -> PersistedSourceAdapter:
    """Return a minimal PersistedSourceAdapter for test use."""
    return PersistedSourceAdapter(
        id=_ADAPTER_ID,
        key="airflow-prod",
        display_name="Airflow Prod",
        kind=SourceAdapterKind.BIGQUERY,  # v1 kind — only adapter.key is used
        target_id="airflow-prod",
        status=SourceAdapterStatus.READY,
        config=BigQueryAdapterConfig(
            service_account_secret=ExternalSecretRef(provider="env", reference="BQ_SA"),
            project_id="unused",
        ),
    )


def _make_adapter_token() -> AirflowAdapter:
    return AirflowAdapter(base_url=_BASE_URL, auth_token="tok-abc123")


def _make_adapter_basic() -> AirflowAdapter:
    return AirflowAdapter(base_url=_BASE_URL, username="admin", password="secret")


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_rejects_empty_base_url() -> None:
    with pytest.raises(ValueError, match="base_url"):
        AirflowAdapter(base_url="", auth_token="tok")


def test_constructor_rejects_missing_creds() -> None:
    with pytest.raises(ValueError, match="auth_token or both"):
        AirflowAdapter(base_url=_BASE_URL)


def test_constructor_rejects_partial_basic_auth() -> None:
    with pytest.raises(ValueError, match="auth_token or both"):
        AirflowAdapter(base_url=_BASE_URL, username="admin")


# ---------------------------------------------------------------------------
# Auth headers
# ---------------------------------------------------------------------------


def test_bearer_auth_header() -> None:
    adapter = _make_adapter_token()
    headers = adapter._auth_headers()
    assert headers["Authorization"] == "Bearer tok-abc123"


def test_basic_auth_header() -> None:
    adapter = _make_adapter_basic()
    headers = adapter._auth_headers()
    expected = base64.b64encode(b"admin:secret").decode()
    assert headers["Authorization"] == f"Basic {expected}"


def test_trailing_slash_stripped() -> None:
    adapter = AirflowAdapter(base_url="https://airflow.example.com/", auth_token="tok")
    assert not adapter._base_url.endswith("/")


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_api_get_all_paginates() -> None:
    """_api_get_all should iterate until total_entries is exhausted."""
    adapter = _make_adapter_token()

    pages = [
        {"dags": [{"dag_id": "dag_a"}, {"dag_id": "dag_b"}], "total_entries": 3},
        {"dags": [{"dag_id": "dag_c"}], "total_entries": 3},
    ]
    call_count = 0

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        nonlocal call_count
        result = pages[call_count]
        call_count += 1
        return result

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        results = await adapter._api_get_all("dags", "dags")

    assert [r["dag_id"] for r in results] == ["dag_a", "dag_b", "dag_c"]
    assert call_count == 2


@pytest.mark.asyncio
async def test_api_get_all_single_page() -> None:
    adapter = _make_adapter_token()

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        return {"dags": [{"dag_id": "only_dag"}], "total_entries": 1}

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        results = await adapter._api_get_all("dags", "dags")

    assert len(results) == 1


# ---------------------------------------------------------------------------
# probe()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_healthy() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        if path == "health":
            return {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
        return {"dags": [], "total_entries": 0}

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        results = await adapter.probe(persisted)

    assert all(r.available for r in results)
    assert {r.capability for r in results} == adapter.declared_capabilities


@pytest.mark.asyncio
async def test_probe_unhealthy_metadatabase() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        if path == "health":
            return {"metadatabase": {"status": "unhealthy"}}
        return {}

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        results = await adapter.probe(persisted)

    assert all(not r.available for r in results)


@pytest.mark.asyncio
async def test_probe_connection_error() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        raise ConnectionError("refused")

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        results = await adapter.probe(persisted)

    assert all(not r.available for r in results)
    assert all("refused" in (r.message or "") for r in results)


@pytest.mark.asyncio
async def test_probe_specific_capabilities() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()
    caps = frozenset({AdapterCapability.DISCOVER, AdapterCapability.TRAFFIC})

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        if path == "health":
            return {"metadatabase": {"status": "healthy"}}
        return {"dags": [], "total_entries": 0}

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        results = await adapter.probe(persisted, capabilities=caps)

    assert {r.capability for r in results} == caps


# ---------------------------------------------------------------------------
# test_connection()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_success() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        return {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        result = await adapter.test_connection(persisted)

    assert result.success
    assert "healthy" in result.message


@pytest.mark.asyncio
async def test_connection_failure_unhealthy() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        return {"metadatabase": {"status": "unhealthy"}}

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        result = await adapter.test_connection(persisted)

    assert not result.success


@pytest.mark.asyncio
async def test_connection_failure_exception() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        raise RuntimeError("network down")

    with patch.object(adapter, "_api_get", side_effect=fake_api_get):
        result = await adapter.test_connection(persisted)

    assert not result.success
    assert "network down" in result.message


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

_SAMPLE_DAGS = [
    {
        "dag_id": "etl_pipeline",
        "description": "Loads raw events",
        "is_active": True,
        "is_paused": False,
        "schedule_interval": {"__type": "CronExpression", "value": "0 * * * *"},
        "tags": [{"name": "etl"}],
        "owners": ["data-eng"],
    },
    {
        "dag_id": "ml_training",
        "description": None,
        "is_active": True,
        "is_paused": True,
        "schedule_interval": None,
        "tags": [],
        "owners": [],
    },
]


@pytest.mark.asyncio
async def test_discover_returns_one_container_per_dag() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    with patch.object(
        adapter,
        "_api_get_all",
        new=AsyncMock(return_value=_SAMPLE_DAGS),
    ):
        snapshot = await adapter.discover(persisted)

    assert len(snapshot.containers) == 2
    ids = {c.container_id for c in snapshot.containers}
    assert f"airflow://{_BASE_URL}/etl_pipeline" in ids
    assert f"airflow://{_BASE_URL}/ml_training" in ids


@pytest.mark.asyncio
async def test_discover_container_metadata() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    with patch.object(
        adapter,
        "_api_get_all",
        new=AsyncMock(return_value=_SAMPLE_DAGS),
    ):
        snapshot = await adapter.discover(persisted)

    etl = next(c for c in snapshot.containers if "etl_pipeline" in c.container_id)
    assert etl.container_type == "dag"
    assert etl.metadata["tags"] == ["etl"]
    assert etl.metadata["owners"] == ["data-eng"]
    assert etl.metadata["is_paused"] is False


@pytest.mark.asyncio
async def test_discover_meta_fields() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    with patch.object(
        adapter,
        "_api_get_all",
        new=AsyncMock(return_value=_SAMPLE_DAGS),
    ):
        snapshot = await adapter.discover(persisted)

    assert snapshot.meta.capability == AdapterCapability.DISCOVER
    assert snapshot.meta.row_count == 2
    assert snapshot.meta.duration_ms >= 0


@pytest.mark.asyncio
async def test_discover_empty() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    with patch.object(adapter, "_api_get_all", new=AsyncMock(return_value=[])):
        snapshot = await adapter.discover(persisted)

    assert snapshot.containers == ()
    assert snapshot.meta.row_count == 0


# ---------------------------------------------------------------------------
# extract_traffic()
# ---------------------------------------------------------------------------

_SAMPLE_RUNS = [
    {"dag_run_id": "scheduled__2024-01-01T00:00:00+00:00", "execution_date": "2024-01-01T00:00:00+00:00"},
]

_SAMPLE_TASK_INSTANCES = [
    {
        "task_id": "load_users",
        "dag_id": "etl_pipeline",
        "dag_run_id": "scheduled__2024-01-01T00:00:00+00:00",
        "start_date": "2024-01-01T00:01:03+00:00",
        "end_date": "2024-01-01T00:04:17+00:00",
        "duration": 194.531,
        "state": "success",
        "operator": "BigQueryOperator",
        "rendered_fields": {"sql": "SELECT * FROM users"},
    },
    {
        "task_id": "notify",
        "dag_id": "etl_pipeline",
        "dag_run_id": "scheduled__2024-01-01T00:00:00+00:00",
        "start_date": "2024-01-01T00:04:18+00:00",
        "end_date": None,
        "duration": None,
        "state": "success",
        "operator": "SlackOperator",
        "rendered_fields": {},
    },
]


@pytest.mark.asyncio
async def test_extract_traffic_creates_events_per_task_instance() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        if "dagRuns" not in path and list_key == "dags":
            return [{"dag_id": "etl_pipeline"}]
        if "dagRuns" in path and "taskInstances" not in path:
            return _SAMPLE_RUNS
        if "taskInstances" in path:
            return _SAMPLE_TASK_INSTANCES
        return []

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all):
        result = await adapter.extract_traffic(persisted)

    assert len(result.events) == 2
    event_ids = {e.event_id for e in result.events}
    assert "etl_pipeline/scheduled__2024-01-01T00:00:00+00:00/load_users" in event_ids


@pytest.mark.asyncio
async def test_extract_traffic_sql_from_rendered_fields() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        if list_key == "dags":
            return [{"dag_id": "etl_pipeline"}]
        if list_key == "dag_runs":
            return _SAMPLE_RUNS
        if list_key == "task_instances":
            return [_SAMPLE_TASK_INSTANCES[0]]
        return []

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all):
        result = await adapter.extract_traffic(persisted)

    sql_event = next(e for e in result.events if e.event_id and "load_users" in e.event_id)
    assert sql_event.sql == "SELECT * FROM users"
    assert sql_event.duration_ms == pytest.approx(194531.0, rel=1e-3)


@pytest.mark.asyncio
async def test_extract_traffic_fallback_sql_when_no_rendered_fields() -> None:
    """Task with no rendered_fields.sql gets a fallback comment."""
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        if list_key == "dags":
            return [{"dag_id": "etl_pipeline"}]
        if list_key == "dag_runs":
            return _SAMPLE_RUNS
        if list_key == "task_instances":
            return [_SAMPLE_TASK_INSTANCES[1]]  # SlackOperator — no sql
        return []

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all):
        result = await adapter.extract_traffic(persisted)

    assert len(result.events) == 1
    assert result.events[0].sql.startswith("-- task:")


@pytest.mark.asyncio
async def test_extract_traffic_skips_tasks_without_start_date() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()
    no_start = {**_SAMPLE_TASK_INSTANCES[0], "start_date": None}

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        if list_key == "dags":
            return [{"dag_id": "etl_pipeline"}]
        if list_key == "dag_runs":
            return _SAMPLE_RUNS
        if list_key == "task_instances":
            return [no_start]
        return []

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all):
        result = await adapter.extract_traffic(persisted)

    assert result.events == ()


@pytest.mark.asyncio
async def test_extract_traffic_since_passed_as_param() -> None:
    """since should be forwarded as execution_date_gte."""
    adapter = _make_adapter_token()
    persisted = _make_adapter()
    since = datetime(2024, 1, 1, tzinfo=UTC)
    captured_extra_params: list[dict] = []

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        if list_key == "dags":
            return [{"dag_id": "etl_pipeline"}]
        if list_key == "dag_runs":
            captured_extra_params.append(extra_params or {})
            return []
        return []

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all):
        await adapter.extract_traffic(persisted, since=since)

    assert any("execution_date_gte" in p for p in captured_extra_params)


@pytest.mark.asyncio
async def test_extract_traffic_meta() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        if list_key == "dags":
            return [{"dag_id": "etl_pipeline"}]
        if list_key == "dag_runs":
            return _SAMPLE_RUNS
        if list_key == "task_instances":
            return [_SAMPLE_TASK_INSTANCES[0]]
        return []

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all):
        result = await adapter.extract_traffic(persisted)

    assert result.meta.capability == AdapterCapability.TRAFFIC
    assert result.meta.row_count == 1


# ---------------------------------------------------------------------------
# extract_lineage()
# ---------------------------------------------------------------------------

_TASKS_WITH_SENSOR = {
    "tasks": [
        {
            "task_id": "wait_for_upstream",
            "class_ref": {"class_name": "ExternalTaskSensor"},
            "template_fields": {"external_dag_id": "upstream_pipeline"},
        },
        {
            "task_id": "do_work",
            "class_ref": {"class_name": "PythonOperator"},
            "template_fields": {},
        },
    ]
}


@pytest.mark.asyncio
async def test_extract_lineage_detects_external_task_sensor() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        return [{"dag_id": "downstream_dag"}]

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        return _TASKS_WITH_SENSOR

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all), \
         patch.object(adapter, "_api_get", side_effect=fake_api_get):
        snapshot = await adapter.extract_lineage(persisted)

    assert len(snapshot.edges) == 1
    edge = snapshot.edges[0]
    assert edge.source_object == "airflow://upstream_pipeline"
    assert edge.target_object == "airflow://downstream_dag"
    assert edge.edge_kind == LineageEdgeKind.CONNECTOR_API
    assert edge.confidence == pytest.approx(0.9)


@pytest.mark.asyncio
async def test_extract_lineage_no_sensors() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        return [{"dag_id": "standalone_dag"}]

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        return {"tasks": [{"task_id": "t1", "class_ref": {"class_name": "PythonOperator"}, "template_fields": {}}]}

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all), \
         patch.object(adapter, "_api_get", side_effect=fake_api_get):
        snapshot = await adapter.extract_lineage(persisted)

    assert snapshot.edges == ()


@pytest.mark.asyncio
async def test_extract_lineage_meta() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    with patch.object(adapter, "_api_get_all", new=AsyncMock(return_value=[])):
        snapshot = await adapter.extract_lineage(persisted)

    assert snapshot.meta.capability == AdapterCapability.LINEAGE


# ---------------------------------------------------------------------------
# extract_orchestration()
# ---------------------------------------------------------------------------

_SAMPLE_TASKS_ORCH = [
    {
        "task_id": "extract_users",
        "downstream_task_ids": ["transform_users"],
        "operator": "PythonOperator",
        "trigger_rule": "all_success",
    },
    {
        "task_id": "transform_users",
        "downstream_task_ids": ["load_users"],
        "operator": "PythonOperator",
        "trigger_rule": "all_success",
    },
    {
        "task_id": "load_users",
        "downstream_task_ids": [],
        "operator": "BigQueryOperator",
        "trigger_rule": "all_success",
    },
]


@pytest.mark.asyncio
async def test_extract_orchestration_builds_units() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        return [{"dag_id": "etl_pipeline", "schedule_interval": {"value": "0 * * * *"}}]

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        if "tasks" in path:
            return {"tasks": _SAMPLE_TASKS_ORCH}
        # last run
        return {
            "dag_runs": [{"execution_date": "2024-01-02T00:00:00+00:00", "state": "success"}]
        }

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all), \
         patch.object(adapter, "_api_get", side_effect=fake_api_get):
        snapshot = await adapter.extract_orchestration(persisted)

    assert len(snapshot.units) == 1
    unit = snapshot.units[0]
    assert unit.unit_id == f"airflow://{_BASE_URL}/etl_pipeline"
    assert unit.unit_type == "dag"
    assert unit.schedule == "0 * * * *"
    assert unit.last_run_status == "success"
    assert unit.last_run_at == datetime(2024, 1, 2, tzinfo=UTC)


@pytest.mark.asyncio
async def test_extract_orchestration_upstream_derivation() -> None:
    """upstream_task_ids are correctly derived from downstream_task_ids inversion."""
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        return [{"dag_id": "etl_pipeline", "schedule_interval": None}]

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        if "tasks" in path:
            return {"tasks": _SAMPLE_TASKS_ORCH}
        return {"dag_runs": []}

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all), \
         patch.object(adapter, "_api_get", side_effect=fake_api_get):
        snapshot = await adapter.extract_orchestration(persisted)

    unit = snapshot.units[0]
    by_id = {t.task_id: t for t in unit.tasks}

    assert by_id["extract_users"].upstream_task_ids == ()
    assert by_id["transform_users"].upstream_task_ids == ("extract_users",)
    assert by_id["load_users"].upstream_task_ids == ("transform_users",)


@pytest.mark.asyncio
async def test_extract_orchestration_tolerates_last_run_error() -> None:
    """If the last-run API call fails, the unit is still returned (no last_run_at)."""
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        return [{"dag_id": "etl_pipeline", "schedule_interval": None}]

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        if "tasks" in path:
            return {"tasks": _SAMPLE_TASKS_ORCH}
        raise RuntimeError("dag runs unavailable")

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all), \
         patch.object(adapter, "_api_get", side_effect=fake_api_get):
        snapshot = await adapter.extract_orchestration(persisted)

    assert len(snapshot.units) == 1
    assert snapshot.units[0].last_run_at is None


@pytest.mark.asyncio
async def test_extract_orchestration_meta() -> None:
    adapter = _make_adapter_token()
    persisted = _make_adapter()

    async def fake_api_get_all(path: str, list_key: str, *, extra_params: dict | None = None) -> list:
        return [
            {"dag_id": "dag1", "schedule_interval": None},
            {"dag_id": "dag2", "schedule_interval": None},
        ]

    async def fake_api_get(path: str, *, params: dict | None = None) -> dict:
        if "tasks" in path:
            return {"tasks": []}
        return {"dag_runs": []}

    with patch.object(adapter, "_api_get_all", side_effect=fake_api_get_all), \
         patch.object(adapter, "_api_get", side_effect=fake_api_get):
        snapshot = await adapter.extract_orchestration(persisted)

    assert snapshot.meta.capability == AdapterCapability.ORCHESTRATION
    assert snapshot.meta.row_count == 2


# ---------------------------------------------------------------------------
# Not-implemented stubs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_schema_not_implemented() -> None:
    adapter = _make_adapter_token()
    with pytest.raises(NotImplementedError):
        await adapter.extract_schema(_make_adapter())


@pytest.mark.asyncio
async def test_extract_definitions_not_implemented() -> None:
    adapter = _make_adapter_token()
    with pytest.raises(NotImplementedError):
        await adapter.extract_definitions(_make_adapter())


@pytest.mark.asyncio
async def test_execute_query_not_implemented() -> None:
    adapter = _make_adapter_token()
    with pytest.raises(NotImplementedError):
        await adapter.execute_query(_make_adapter(), "SELECT 1")


# ---------------------------------------------------------------------------
# Declared capabilities
# ---------------------------------------------------------------------------


def test_declared_capabilities() -> None:
    adapter = _make_adapter_token()
    assert AdapterCapability.DISCOVER in adapter.declared_capabilities
    assert AdapterCapability.TRAFFIC in adapter.declared_capabilities
    assert AdapterCapability.LINEAGE in adapter.declared_capabilities
    assert AdapterCapability.ORCHESTRATION in adapter.declared_capabilities
    assert AdapterCapability.SCHEMA not in adapter.declared_capabilities
    assert AdapterCapability.DEFINITIONS not in adapter.declared_capabilities
