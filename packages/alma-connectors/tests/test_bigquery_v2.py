"""Tests for BigQueryAdapter v2 protocol (probe, discover, extract_schema, extract_traffic)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from alma_connectors import (
    BigQueryAdapterConfig,
    ExternalSecretRef,
    PersistedSourceAdapter,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.adapters.bigquery import BigQueryAdapter
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    DefinitionSnapshot,
    DiscoverySnapshot,
    ExtractionScope,
    SchemaSnapshotV2,
    TrafficExtractionResult,
)
from alma_connectors.source_adapter_v2 import (
    SchemaObjectKind as V2SchemaObjectKind,
)

# ---------------------------------------------------------------------------
# Fake BQ infrastructure (v2-aware)
# ---------------------------------------------------------------------------


class _FakeDataset:
    """Minimal stand-in for google.cloud.bigquery.DatasetListItem."""

    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id


class _FakeQueryJob:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def result(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeBQClient:
    """Fake BQ client for v2 tests with configurable datasets and error injection."""

    def __init__(
        self,
        *,
        datasets: list[str] | None = None,
        column_rows: list[dict[str, Any]] | None = None,
        job_rows: list[dict[str, Any]] | None = None,
        storage_rows: list[dict[str, Any]] | None = None,
        view_rows: list[dict[str, Any]] | None = None,
        routine_rows: list[dict[str, Any]] | None = None,
        table_ddl_rows: list[dict[str, Any]] | None = None,
        model_rows: list[dict[str, Any]] | None = None,
        options_rows: list[dict[str, Any]] | None = None,
        parameter_rows: list[dict[str, Any]] | None = None,
        raise_on_list_datasets: Exception | None = None,
        raise_on_schema_query: Exception | None = None,
        raise_on_jobs_query: Exception | None = None,
        raise_on_definitions_query: Exception | None = None,
        raise_on_routines_query: Exception | None = None,
        raise_on_models_query: Exception | None = None,
    ) -> None:
        self._datasets = [_FakeDataset(d) for d in (datasets if datasets is not None else ["ds_a", "ds_b"])]
        self._column_rows = column_rows or []
        self._job_rows = job_rows or []
        self._storage_rows = storage_rows or []
        self._view_rows = view_rows or []
        self._routine_rows = routine_rows or []
        self._table_ddl_rows = table_ddl_rows or []
        self._model_rows = model_rows or []
        self._options_rows = options_rows or []
        self._parameter_rows = parameter_rows or []
        self._raise_on_list_datasets = raise_on_list_datasets
        self._raise_on_schema_query = raise_on_schema_query
        self._raise_on_jobs_query = raise_on_jobs_query
        self._raise_on_definitions_query = raise_on_definitions_query
        self._raise_on_routines_query = raise_on_routines_query
        self._raise_on_models_query = raise_on_models_query
        self.queries_issued: list[str] = []

    def list_datasets(self) -> list[_FakeDataset]:
        if self._raise_on_list_datasets is not None:
            raise self._raise_on_list_datasets
        return list(self._datasets)

    def query(self, sql: str, job_config: Any = None) -> Any:
        self.queries_issued.append(sql)
        if "TABLE_STORAGE" in sql:
            return _FakeQueryJob(self._storage_rows)
        if "JOBS_BY_PROJECT" in sql:
            if self._raise_on_jobs_query is not None:
                raise self._raise_on_jobs_query
            return _FakeQueryJob(self._job_rows)
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            if self._raise_on_schema_query is not None:
                raise self._raise_on_schema_query
            return _FakeQueryJob(self._column_rows)
        if "INFORMATION_SCHEMA.VIEWS" in sql:
            if self._raise_on_definitions_query is not None:
                raise self._raise_on_definitions_query
            return _FakeQueryJob(self._view_rows)
        if "INFORMATION_SCHEMA.PARAMETERS" in sql:
            return _FakeQueryJob(self._parameter_rows)
        if "INFORMATION_SCHEMA.ROUTINES" in sql:
            if self._raise_on_routines_query is not None:
                raise self._raise_on_routines_query
            if self._raise_on_definitions_query is not None:
                raise self._raise_on_definitions_query
            return _FakeQueryJob(self._routine_rows)
        if "INFORMATION_SCHEMA.TABLE_OPTIONS" in sql:
            return _FakeQueryJob(self._options_rows)
        if "INFORMATION_SCHEMA.MODELS" in sql:
            if self._raise_on_models_query is not None:
                raise self._raise_on_models_query
            return _FakeQueryJob(self._model_rows)
        if "INFORMATION_SCHEMA.TABLES" in sql:
            return _FakeQueryJob(self._table_ddl_rows)
        # probe SELECT 1, test_connection SELECT 1, etc.
        return _FakeQueryJob([])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_adapter(
    *,
    project_id: str = "acme-project",
    location: str = "us",
    observation_cursor: dict[str, object] | None = None,
) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id="00000000-0000-0000-0000-000000000002",
        key="bq-test",
        display_name="BQ Test",
        kind=SourceAdapterKind.BIGQUERY,
        target_id="prod",
        status=SourceAdapterStatus.READY,
        config=BigQueryAdapterConfig(
            service_account_secret=ExternalSecretRef(provider="env", reference="BQ_SA_JSON"),
            project_id=project_id,
            location=location,
        ),
        observation_cursor=observation_cursor,
    )


def _make_bq_adapter(client: _FakeBQClient) -> BigQueryAdapter:
    def resolve_secret(secret: Any) -> str:
        return '{"type":"service_account"}'

    def client_factory(project_id: str, sa_json: str | None) -> _FakeBQClient:
        return client

    return BigQueryAdapter(resolve_secret=resolve_secret, client_factory=client_factory)


# ---------------------------------------------------------------------------
# declared_capabilities
# ---------------------------------------------------------------------------


def test_declared_capabilities_includes_discover_schema_definitions_traffic() -> None:
    adapter = BigQueryAdapter(resolve_secret=lambda s: "", client_factory=lambda p, j: None)
    caps = adapter.declared_capabilities
    assert AdapterCapability.DISCOVER in caps
    assert AdapterCapability.SCHEMA in caps
    assert AdapterCapability.DEFINITIONS in caps
    assert AdapterCapability.TRAFFIC in caps


def test_declared_capabilities_excludes_lineage_orchestration() -> None:
    adapter = BigQueryAdapter(resolve_secret=lambda s: "", client_factory=lambda p, j: None)
    caps = adapter.declared_capabilities
    assert AdapterCapability.LINEAGE not in caps
    assert AdapterCapability.ORCHESTRATION not in caps


# ---------------------------------------------------------------------------
# probe() — happy path
# ---------------------------------------------------------------------------


def test_probe_returns_all_declared_capabilities() -> None:
    client = _FakeBQClient()
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    caps = {r.capability for r in results}
    assert AdapterCapability.DISCOVER in caps
    assert AdapterCapability.SCHEMA in caps
    assert AdapterCapability.DEFINITIONS in caps
    assert AdapterCapability.TRAFFIC in caps


def test_probe_all_available_when_no_errors() -> None:
    client = _FakeBQClient()
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    for r in results:
        assert r.available is True
        assert r.permissions_missing == ()


def test_probe_scope_is_region() -> None:
    client = _FakeBQClient()
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    for r in results:
        assert r.scope == ExtractionScope.REGION
        assert r.scope_context is not None
        assert r.scope_context.identifiers["project"] == "acme-project"
        assert r.scope_context.identifiers["location"] == "us"


# ---------------------------------------------------------------------------
# probe() — permission failure detection
# ---------------------------------------------------------------------------


def test_probe_discover_unavailable_on_permission_error() -> None:
    client = _FakeBQClient(
        raise_on_list_datasets=PermissionError("403 PermissionDenied: bigquery.datasets.list")
    )
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    discover_result = next(r for r in results if r.capability == AdapterCapability.DISCOVER)
    assert discover_result.available is False
    assert len(discover_result.permissions_missing) > 0
    assert discover_result.message is not None


def test_probe_schema_unavailable_on_permission_error() -> None:
    client = _FakeBQClient(
        raise_on_schema_query=PermissionError("403 PermissionDenied: bigquery.tables.getData")
    )
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    schema_result = next(r for r in results if r.capability == AdapterCapability.SCHEMA)
    assert schema_result.available is False
    assert len(schema_result.permissions_missing) > 0


def test_probe_traffic_unavailable_on_permission_error() -> None:
    client = _FakeBQClient(
        raise_on_jobs_query=PermissionError("403 PermissionDenied: bigquery.jobs.listAll")
    )
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    traffic_result = next(r for r in results if r.capability == AdapterCapability.TRAFFIC)
    assert traffic_result.available is False
    assert "bigquery.jobs.listAll" in traffic_result.permissions_missing


def test_probe_subset_capabilities() -> None:
    """When capabilities arg is provided, only probe those capabilities."""
    client = _FakeBQClient()
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(
        bq.probe(persisted, capabilities=frozenset({AdapterCapability.DISCOVER}))
    )
    assert len(results) == 1
    assert results[0].capability == AdapterCapability.DISCOVER


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------


def test_discover_returns_discovery_snapshot() -> None:
    client = _FakeBQClient(datasets=["analytics", "raw", "staging"])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.discover(persisted))
    assert isinstance(snapshot, DiscoverySnapshot)


def test_discover_returns_datasets_as_containers() -> None:
    client = _FakeBQClient(datasets=["analytics", "raw", "staging"])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.discover(persisted))
    assert len(snapshot.containers) == 3
    container_ids = {c.container_id for c in snapshot.containers}
    assert "acme-project.analytics" in container_ids
    assert "acme-project.raw" in container_ids
    assert "acme-project.staging" in container_ids


def test_discover_container_type_is_dataset() -> None:
    client = _FakeBQClient(datasets=["my_dataset"])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.discover(persisted))
    assert snapshot.containers[0].container_type == "dataset"


def test_discover_container_location_matches_config() -> None:
    client = _FakeBQClient(datasets=["ds"])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter(location="eu")
    snapshot = asyncio.run(bq.discover(persisted))
    assert snapshot.containers[0].location == "eu"


def test_discover_meta_capability_is_discover() -> None:
    client = _FakeBQClient(datasets=["ds"])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.discover(persisted))
    assert snapshot.meta.capability == AdapterCapability.DISCOVER
    assert snapshot.meta.adapter_key == "bq-test"
    assert snapshot.meta.row_count == 1


def test_discover_empty_project() -> None:
    client = _FakeBQClient(datasets=[])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.discover(persisted))
    assert snapshot.containers == ()
    assert snapshot.meta.row_count == 0


# ---------------------------------------------------------------------------
# extract_schema()
# ---------------------------------------------------------------------------


def test_extract_schema_returns_schema_snapshot_v2() -> None:
    column_rows = [
        {
            "table_schema": "analytics",
            "table_name": "events",
            "column_name": "id",
            "data_type": "INT64",
            "is_nullable": "NO",
            "ordinal_position": 1,
            "is_partitioning_column": "NO",
            "clustering_ordinal_position": None,
        },
    ]
    client = _FakeBQClient(column_rows=column_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_schema(persisted))
    assert isinstance(snapshot, SchemaSnapshotV2)
    assert snapshot.meta.capability == AdapterCapability.SCHEMA


def test_extract_schema_converts_columns() -> None:
    column_rows = [
        {
            "table_schema": "ds",
            "table_name": "users",
            "column_name": "email",
            "data_type": "STRING",
            "is_nullable": "YES",
            "ordinal_position": 1,
            "is_partitioning_column": "NO",
            "clustering_ordinal_position": None,
        },
    ]
    client = _FakeBQClient(column_rows=column_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_schema(persisted))
    assert len(snapshot.objects) == 1
    obj = snapshot.objects[0]
    assert obj.schema_name == "ds"
    assert obj.object_name == "users"
    assert len(obj.columns) == 1
    assert obj.columns[0].name == "email"
    assert obj.columns[0].data_type == "STRING"
    assert obj.columns[0].is_nullable is True


def test_extract_schema_includes_udfs() -> None:
    routine_rows = [
        {
            "routine_schema": "analytics",
            "routine_name": "add_one",
            "routine_type": "FUNCTION",
            "data_type": "INT64",
            "routine_definition": "x + 1",
            "routine_body": "SQL",
            "external_language": "",
        },
    ]
    parameter_rows = [
        {
            "specific_schema": "analytics",
            "specific_name": "add_one",
            "parameter_name": "x",
            "data_type": "INT64",
            "ordinal_position": 1,
        },
    ]
    client = _FakeBQClient(routine_rows=routine_rows, parameter_rows=parameter_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_schema(persisted))
    udf_objs = [o for o in snapshot.objects if o.kind == V2SchemaObjectKind.UDF]
    assert len(udf_objs) == 1
    udf = udf_objs[0]
    assert udf.schema_name == "analytics"
    assert udf.object_name == "add_one"
    assert udf.return_type == "INT64"
    assert udf.definition_body == "x + 1"
    assert udf.language == "SQL"
    assert len(udf.columns) == 1
    assert udf.columns[0].name == "x"
    assert udf.columns[0].data_type == "INT64"


def test_extract_schema_includes_freshness_metadata() -> None:
    column_rows = [
        {
            "table_schema": "ds",
            "table_name": "orders",
            "column_name": "id",
            "data_type": "INT64",
            "is_nullable": "NO",
            "ordinal_position": 1,
            "is_partitioning_column": "NO",
            "clustering_ordinal_position": None,
        },
    ]
    lmt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
    storage_rows = [
        {
            "dataset_id": "ds",
            "table_id": "orders",
            "row_count": 1000,
            "total_logical_bytes": 512000,
            "last_modified_time": lmt,
        },
    ]
    client = _FakeBQClient(column_rows=column_rows, storage_rows=storage_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_schema(persisted))
    assert len(snapshot.objects) == 1
    obj = snapshot.objects[0]
    assert obj.row_count == 1000
    assert obj.size_bytes == 512000
    assert obj.last_modified == lmt


def test_extract_schema_routines_access_denied_falls_back_gracefully() -> None:
    column_rows = [
        {
            "table_schema": "ds",
            "table_name": "orders",
            "column_name": "id",
            "data_type": "INT64",
            "is_nullable": "NO",
            "ordinal_position": 1,
            "is_partitioning_column": "NO",
            "clustering_ordinal_position": None,
        },
    ]
    client = _FakeBQClient(
        column_rows=column_rows,
        raise_on_routines_query=PermissionError("403 PermissionDenied: bigquery.routines.get"),
    )
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_schema(persisted))
    assert isinstance(snapshot, SchemaSnapshotV2)
    table_objs = [o for o in snapshot.objects if o.kind == V2SchemaObjectKind.TABLE]
    assert len(table_objs) == 1
    routine_objs = [
        o for o in snapshot.objects
        if o.kind in (V2SchemaObjectKind.UDF, V2SchemaObjectKind.PROCEDURE)
    ]
    assert len(routine_objs) == 0


def test_extract_schema_includes_ml_models() -> None:
    lmt = datetime(2024, 3, 1, 0, 0, 0, tzinfo=UTC)
    model_rows = [
        {
            "model_schema": "ml_datasets",
            "model_name": "churn_predictor",
            "model_type": "LOGISTIC_REG",
            "last_modified_time": lmt,
        },
    ]
    client = _FakeBQClient(model_rows=model_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_schema(persisted))
    model_objs = [o for o in snapshot.objects if o.kind == V2SchemaObjectKind.ML_MODEL]
    assert len(model_objs) == 1
    model = model_objs[0]
    assert model.schema_name == "ml_datasets"
    assert model.object_name == "churn_predictor"
    assert model.model_type == "LOGISTIC_REG"
    assert model.last_modified == lmt


# ---------------------------------------------------------------------------
# extract_traffic()
# ---------------------------------------------------------------------------


def test_extract_traffic_returns_traffic_extraction_result() -> None:
    now = datetime.now(tz=UTC)
    job_rows = [
        {
            "job_id": "job-1",
            "creation_time": now,
            "end_time": now,
            "user_email": "alice@example.com",
            "labels": {},
            "query": "SELECT * FROM ds.users",
            "referenced_tables": [],
        }
    ]
    client = _FakeBQClient(job_rows=job_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    result = asyncio.run(bq.extract_traffic(persisted))
    assert isinstance(result, TrafficExtractionResult)
    assert result.meta.capability == AdapterCapability.TRAFFIC
    assert len(result.events) == 1


def test_extract_traffic_passes_observation_cursor() -> None:
    """observation_cursor from v1 is forwarded to v2 TrafficExtractionResult."""
    client = _FakeBQClient(job_rows=[])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    result = asyncio.run(bq.extract_traffic(persisted))
    # No events → no cursor
    assert result.observation_cursor is None


# ---------------------------------------------------------------------------
# probe() — DEFINITIONS
# ---------------------------------------------------------------------------


def test_probe_definitions_available_when_no_errors() -> None:
    client = _FakeBQClient()
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    defs_result = next(r for r in results if r.capability == AdapterCapability.DEFINITIONS)
    assert defs_result.available is True
    assert defs_result.permissions_missing == ()


def test_probe_definitions_unavailable_on_permission_error() -> None:
    client = _FakeBQClient(
        raise_on_definitions_query=PermissionError("403 PermissionDenied: bigquery.routines.list")
    )
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    results = asyncio.run(bq.probe(persisted))
    defs_result = next(r for r in results if r.capability == AdapterCapability.DEFINITIONS)
    assert defs_result.available is False
    assert len(defs_result.permissions_missing) > 0
    assert defs_result.message is not None


# ---------------------------------------------------------------------------
# extract_definitions()
# ---------------------------------------------------------------------------


def test_extract_definitions_returns_definition_snapshot() -> None:
    view_rows = [
        {"table_schema": "analytics", "table_name": "v_users", "view_definition": "SELECT * FROM analytics.users"},
    ]
    client = _FakeBQClient(view_rows=view_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    assert isinstance(snapshot, DefinitionSnapshot)
    assert snapshot.meta.capability == AdapterCapability.DEFINITIONS


def test_extract_definitions_returns_view_sql() -> None:
    view_rows = [
        {"table_schema": "ds", "table_name": "active_users", "view_definition": "SELECT id FROM ds.users WHERE active"},
    ]
    client = _FakeBQClient(view_rows=view_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    views = [d for d in snapshot.definitions if d.object_kind.value == "view"]
    assert len(views) == 1
    assert views[0].schema_name == "ds"
    assert views[0].object_name == "active_users"
    assert views[0].definition_text == "SELECT id FROM ds.users WHERE active"
    assert views[0].definition_language == "sql"


def test_extract_definitions_returns_routine_ddl() -> None:
    routine_rows = [
        {
            "routine_schema": "ds",
            "routine_name": "my_func",
            "routine_type": "FUNCTION",
            "routine_definition": "SELECT x + 1",
            "external_language": None,
        },
        {
            "routine_schema": "ds",
            "routine_name": "my_proc",
            "routine_type": "PROCEDURE",
            "routine_definition": "BEGIN SELECT 1; END",
            "external_language": None,
        },
    ]
    client = _FakeBQClient(routine_rows=routine_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    routines = {d.object_name: d for d in snapshot.definitions}
    assert "my_func" in routines
    assert routines["my_func"].object_kind.value == "udf"
    assert routines["my_func"].definition_language == "sql"
    assert "my_proc" in routines
    assert routines["my_proc"].object_kind.value == "procedure"


def test_extract_definitions_returns_table_ddl() -> None:
    table_ddl_rows = [
        {"table_schema": "ds", "table_name": "orders", "ddl": "CREATE TABLE ds.orders (id INT64)"},
    ]
    client = _FakeBQClient(table_ddl_rows=table_ddl_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    tables = [d for d in snapshot.definitions if d.object_kind.value == "table"]
    assert len(tables) == 1
    assert tables[0].object_name == "orders"
    assert tables[0].definition_language == "ddl"
    assert "CREATE TABLE" in tables[0].definition_text


def test_extract_definitions_handles_null_view_definition() -> None:
    view_rows = [
        {"table_schema": "ds", "table_name": "restricted_view", "view_definition": None},
        {"table_schema": "ds", "table_name": "ok_view", "view_definition": "SELECT 1"},
    ]
    client = _FakeBQClient(view_rows=view_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    view_names = {d.object_name for d in snapshot.definitions}
    assert "restricted_view" not in view_names
    assert "ok_view" in view_names


def test_extract_definitions_handles_null_routine_definition() -> None:
    routine_rows = [
        {
            "routine_schema": "ds",
            "routine_name": "restricted_fn",
            "routine_type": "FUNCTION",
            "routine_definition": None,
            "external_language": None,
        },
        {
            "routine_schema": "ds",
            "routine_name": "visible_fn",
            "routine_type": "FUNCTION",
            "routine_definition": "SELECT x",
            "external_language": None,
        },
    ]
    client = _FakeBQClient(routine_rows=routine_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    routine_names = {d.object_name for d in snapshot.definitions}
    assert "restricted_fn" not in routine_names
    assert "visible_fn" in routine_names


def test_extract_definitions_meta_row_count() -> None:
    view_rows = [
        {"table_schema": "ds", "table_name": "v1", "view_definition": "SELECT 1"},
        {"table_schema": "ds", "table_name": "v2", "view_definition": "SELECT 2"},
    ]
    client = _FakeBQClient(view_rows=view_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    assert snapshot.meta.row_count == 2
    assert snapshot.meta.adapter_key == "bq-test"


def test_extract_definitions_empty_when_no_objects() -> None:
    client = _FakeBQClient()
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.extract_definitions(persisted))
    assert snapshot.definitions == ()
    assert snapshot.meta.row_count == 0


# ---------------------------------------------------------------------------
# NotImplementedError methods
# ---------------------------------------------------------------------------


def test_extract_lineage_raises() -> None:
    adapter = BigQueryAdapter(resolve_secret=lambda s: "", client_factory=lambda p, j: None)
    persisted = _make_adapter()
    with pytest.raises(NotImplementedError, match="LINEAGE"):
        asyncio.run(adapter.extract_lineage(persisted))


def test_extract_orchestration_raises() -> None:
    adapter = BigQueryAdapter(resolve_secret=lambda s: "", client_factory=lambda p, j: None)
    persisted = _make_adapter()
    with pytest.raises(NotImplementedError, match="ORCHESTRATION"):
        asyncio.run(adapter.extract_orchestration(persisted))


# ---------------------------------------------------------------------------
# v1 backward compatibility
# ---------------------------------------------------------------------------


def test_v1_test_connection_still_works() -> None:
    """v1 test_connection must continue to work unchanged."""
    client = _FakeBQClient(datasets=["ds1", "ds2"])
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    result = asyncio.run(bq.test_connection(persisted))
    assert result.success is True
    assert "acme-project" in result.message


def test_v1_introspect_schema_still_works() -> None:
    """v1 introspect_schema must return a v1 SchemaSnapshot."""
    from alma_connectors.source_adapter import SchemaSnapshot

    column_rows = [
        {
            "table_schema": "raw",
            "table_name": "orders",
            "column_name": "order_id",
            "data_type": "INT64",
            "is_nullable": "NO",
            "ordinal_position": 1,
            "is_partitioning_column": "NO",
            "clustering_ordinal_position": None,
        },
    ]
    client = _FakeBQClient(column_rows=column_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    snapshot = asyncio.run(bq.introspect_schema(persisted))
    assert isinstance(snapshot, SchemaSnapshot)
    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].object_name == "orders"


def test_v1_observe_traffic_still_works() -> None:
    """v1 observe_traffic must return a v1 TrafficObservationResult."""
    from alma_connectors.source_adapter import TrafficObservationResult

    now = datetime.now(tz=UTC)
    job_rows = [
        {
            "job_id": "job-99",
            "creation_time": now,
            "end_time": now,
            "user_email": "bob@example.com",
            "labels": {},
            "query": "SELECT 1",
            "referenced_tables": [],
        }
    ]
    client = _FakeBQClient(job_rows=job_rows)
    bq = _make_bq_adapter(client)
    persisted = _make_adapter()
    result = asyncio.run(bq.observe_traffic(persisted))
    assert isinstance(result, TrafficObservationResult)
    assert len(result.events) == 1
