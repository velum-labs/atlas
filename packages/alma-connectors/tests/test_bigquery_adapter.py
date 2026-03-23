"""Tests for BigQueryAdapter."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from alma_connectors import (
    BigQueryAdapterConfig,
    ExternalSecretRef,
    ManagedSecret,
    PersistedSourceAdapter,
    PostgresAdapterConfig,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.adapters.bigquery import (
    BigQueryAdapter,
    _build_consumer_identity,
    _effective_since,
    _extract_referenced_tables,
    _extract_tables_from_sql,
    _normalize_labels,
)

# ---------------------------------------------------------------------------
# Fake BQ infrastructure
# ---------------------------------------------------------------------------


class _FakeQueryJob:
    """Minimal fake for google.cloud.bigquery.QueryJob."""

    def __init__(self, rows: list[dict[str, Any]], *, total_bytes_processed: int = 0) -> None:
        self._rows = rows
        self.total_bytes_processed = total_bytes_processed

    def result(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeQueryJobDryRun:
    """Dry-run job — result() is never called."""

    def __init__(self, *, total_bytes_processed: int = 1024) -> None:
        self.total_bytes_processed = total_bytes_processed


class _FakeBigQueryModule:
    """Minimal stand-in for google.cloud.bigquery."""

    class QueryJobConfig:
        def __init__(self, *, dry_run: bool = False, query_parameters: list[Any] | None = None):
            self.dry_run = dry_run
            self.query_parameters = query_parameters or []

    class ScalarQueryParameter:
        def __init__(self, name: str, type_: str, value: Any) -> None:
            self.name = name
            self.type_ = type_
            self.value = value


class _FakeBigQueryClient:
    """Captures calls and returns preset rows for each known query pattern."""

    def __init__(
        self,
        *,
        job_rows: list[dict[str, Any]] | None = None,
        column_rows: list[dict[str, Any]] | None = None,
        storage_rows: list[dict[str, Any]] | None = None,
        query_rows: list[dict[str, Any]] | None = None,
        dataset_count: int = 3,
        raise_on_query: Exception | None = None,
        raise_on_storage_query: Exception | None = None,
        raise_on_list_datasets: Exception | None = None,
    ) -> None:
        self._job_rows = job_rows or []
        self._column_rows = column_rows or []
        self._storage_rows = storage_rows or []
        self._query_rows = query_rows or []
        self._dataset_count = dataset_count
        self._raise_on_query = raise_on_query
        self._raise_on_storage_query = raise_on_storage_query
        self._raise_on_list_datasets = raise_on_list_datasets
        self.queries_issued: list[str] = []
        self.job_configs_issued: list[Any] = []

    def query(self, sql: str, job_config: Any = None) -> Any:
        if "TABLE_STORAGE" in sql:
            if self._raise_on_storage_query is not None:
                raise self._raise_on_storage_query
            self.queries_issued.append(sql)
            self.job_configs_issued.append(job_config)
            return _FakeQueryJob(self._storage_rows)
        if self._raise_on_query is not None:
            raise self._raise_on_query
        self.queries_issued.append(sql)
        self.job_configs_issued.append(job_config)
        if getattr(job_config, "dry_run", False):
            return _FakeQueryJobDryRun()
        if "JOBS_BY_PROJECT" in sql:
            return _FakeQueryJob(self._job_rows)
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return _FakeQueryJob(self._column_rows)
        # Generic query (execute_query, test_connection probe)
        return _FakeQueryJob(self._query_rows)

    def list_datasets(self) -> list[MagicMock]:
        if self._raise_on_list_datasets is not None:
            raise self._raise_on_list_datasets
        return [MagicMock() for _ in range(self._dataset_count)]


def _make_adapter(
    *,
    adapter_id: str = "00000000-0000-0000-0000-000000000001",
    project_id: str = "acme-project",
    location: str = "us",
    lookback_hours: int = 24,
    max_job_rows: int = 1000,
    max_column_rows: int = 5000,
    observation_cursor: dict[str, object] | None = None,
) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=adapter_id,
        key="bq-warehouse",
        display_name="BQ Warehouse",
        kind=SourceAdapterKind.BIGQUERY,
        target_id="prod-warehouse",
        status=SourceAdapterStatus.READY,
        config=BigQueryAdapterConfig(
            service_account_secret=ExternalSecretRef(provider="env", reference="BQ_SA_JSON"),
            project_id=project_id,
            location=location,
            lookback_hours=lookback_hours,
            max_job_rows=max_job_rows,
            max_column_rows=max_column_rows,
        ),
        observation_cursor=observation_cursor,
    )


def _make_bq_adapter(
    client: _FakeBigQueryClient,
    *,
    sa_json: str = '{"type":"service_account"}',
) -> BigQueryAdapter:
    """Build a BigQueryAdapter that uses a fake BQ client and fake secret."""

    def resolve_secret(secret: ManagedSecret | ExternalSecretRef) -> str:
        return sa_json

    def client_factory(project_id: str, service_account_json: str | None) -> _FakeBigQueryClient:
        return client

    return BigQueryAdapter(resolve_secret=resolve_secret, client_factory=client_factory)


# ---------------------------------------------------------------------------
# Unit tests: pure helper functions
# ---------------------------------------------------------------------------


def test_normalize_labels_dict() -> None:
    assert _normalize_labels({"key": "value", "num": 42}) == {"key": "value", "num": "42"}


def test_normalize_labels_list_of_kv() -> None:
    raw = [{"key": "dag_id", "value": "my_dag"}, {"key": "task_id", "value": "run"}]
    assert _normalize_labels(raw) == {"dag_id": "my_dag", "task_id": "run"}


def test_normalize_labels_none() -> None:
    assert _normalize_labels(None) == {}


def test_normalize_labels_invalid_type() -> None:
    assert _normalize_labels("not-a-label") == {}


def test_extract_referenced_tables_dict_rows() -> None:
    raw = [
        {"project_id": "proj", "dataset_id": "ds", "table_id": "tbl"},
        {"project_id": "", "dataset_id": "", "table_id": ""},  # skipped — empty ids
    ]
    result = _extract_referenced_tables(raw)
    assert len(result) == 1
    assert result[0] == {"project_id": "proj", "dataset_id": "ds", "table_id": "tbl"}


def test_extract_referenced_tables_object_rows() -> None:
    class FakeRef:
        project_id = "p"
        dataset_id = "d"
        table_id = "t"

    result = _extract_referenced_tables([FakeRef()])
    assert result == [{"project_id": "p", "dataset_id": "d", "table_id": "t"}]


def test_extract_referenced_tables_none() -> None:
    assert _extract_referenced_tables(None) == []


def test_build_consumer_identity_airflow_dag_and_task() -> None:
    identity = _build_consumer_identity(
        user_email="alice@example.com",
        labels={"dag_id": "etl_pipeline", "task_id": "load_step"},
        fallback_job_id="job-999",
    )
    assert identity["consumer_key"] == "airflow:etl_pipeline:load_step"
    assert identity["source_type"] == "airflow"
    assert identity["confidence"] == 0.95


def test_build_consumer_identity_airflow_dag_only() -> None:
    identity = _build_consumer_identity(
        user_email=None,
        labels={"dag_id": "daily_export"},
        fallback_job_id="job-1",
    )
    assert identity["consumer_key"] == "airflow:daily_export"
    assert identity["task_id"] is None


def test_build_consumer_identity_user_email() -> None:
    identity = _build_consumer_identity(
        user_email="bob@example.com",
        labels={},
        fallback_job_id="job-2",
    )
    assert identity["consumer_key"] == "user:bob@example.com"
    assert identity["source_type"] == "user"
    assert identity["confidence"] == 0.7


def test_build_consumer_identity_fallback_job_id() -> None:
    identity = _build_consumer_identity(
        user_email=None,
        labels={},
        fallback_job_id="job-xyz",
    )
    assert identity["consumer_key"] == "job:job-xyz"
    assert identity["source_type"] == "unknown"


def test_effective_since_uses_lookback_when_none() -> None:
    result = _effective_since(None, lookback_hours=48)
    expected = datetime.now(tz=UTC) - timedelta(hours=48)
    # Allow 5 seconds of slack
    assert abs((result - expected).total_seconds()) < 5


def test_effective_since_clamps_to_retention_boundary() -> None:
    ancient = datetime.now(tz=UTC) - timedelta(days=200)
    result = _effective_since(ancient, lookback_hours=24)
    boundary = datetime.now(tz=UTC) - timedelta(days=180)
    assert abs((result - boundary).total_seconds()) < 5


def test_effective_since_preserves_recent_watermark() -> None:
    recent = datetime.now(tz=UTC) - timedelta(hours=1)
    result = _effective_since(recent, lookback_hours=24)
    assert abs((result - recent).total_seconds()) < 1


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def test_test_connection_success() -> None:
    client = _FakeBigQueryClient(dataset_count=5)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.test_connection(adapter))

    assert result.success is True
    assert result.resource_count == 5
    assert result.resource_label == "datasets"
    assert "acme-project" in result.message


def test_test_connection_no_datasets() -> None:
    client = _FakeBigQueryClient(dataset_count=0)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.test_connection(adapter))

    assert result.success is True
    assert result.resource_count == 0
    assert "0 dataset" in result.message


def test_test_connection_permission_denied() -> None:
    err = Exception("403 Forbidden: PermissionDenied bigquery.jobs.create")
    client = _FakeBigQueryClient(raise_on_query=err)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.test_connection(adapter))

    assert result.success is False
    assert "Permission denied" in result.message


def test_test_connection_project_not_found() -> None:
    err = Exception("404 Not Found: project acme-project not found")
    client = _FakeBigQueryClient(raise_on_query=err)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.test_connection(adapter))

    assert result.success is False
    assert "not found" in result.message.lower()


def test_test_connection_unknown_error() -> None:
    err = Exception("network timeout")
    client = _FakeBigQueryClient(raise_on_query=err)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.test_connection(adapter))

    assert result.success is False
    assert "Connection failed" in result.message


def test_test_connection_wrong_config_raises() -> None:
    client = _FakeBigQueryClient()
    bq = _make_bq_adapter(client)
    pg_adapter = PersistedSourceAdapter(
        id="00000000-0000-0000-0000-000000000002",
        key="pg-db",
        display_name="PG DB",
        kind=SourceAdapterKind.POSTGRES,
        target_id="prod",
        status=SourceAdapterStatus.READY,
        config=PostgresAdapterConfig(
            database_secret=ExternalSecretRef(provider="env", reference="PG_DSN"),
        ),
    )

    with pytest.raises(ValueError, match="not configured as bigquery"):
        asyncio.run(bq.test_connection(pg_adapter))


# ---------------------------------------------------------------------------
# introspect_schema
# ---------------------------------------------------------------------------

_SAMPLE_COLUMN_ROWS = [
    {
        "table_schema": "analytics",
        "table_name": "events",
        "column_name": "event_id",
        "data_type": "STRING",
        "is_nullable": "NO",
        "ordinal_position": 1,
        "is_partitioning_column": "NO",
        "clustering_ordinal_position": None,
    },
    {
        "table_schema": "analytics",
        "table_name": "events",
        "column_name": "created_at",
        "data_type": "TIMESTAMP",
        "is_nullable": "YES",
        "ordinal_position": 2,
        "is_partitioning_column": "YES",
        "clustering_ordinal_position": None,
    },
    {
        "table_schema": "staging",
        "table_name": "raw_events",
        "column_name": "payload",
        "data_type": "JSON",
        "is_nullable": "YES",
        "ordinal_position": 1,
        "is_partitioning_column": "NO",
        "clustering_ordinal_position": None,
    },
]

_SAMPLE_STORAGE_ROWS = [
    {
        "dataset_id": "analytics",
        "table_id": "events",
        "row_count": 1_000_000,
        "total_logical_bytes": 512_000_000,
    },
    {
        "dataset_id": "staging",
        "table_id": "raw_events",
        "row_count": 50_000,
        "total_logical_bytes": 8_000_000,
    },
]


def test_introspect_schema_returns_snapshot() -> None:
    client = _FakeBigQueryClient(column_rows=_SAMPLE_COLUMN_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    assert len(snapshot.objects) == 2
    names = {(obj.schema_name, obj.object_name) for obj in snapshot.objects}
    assert ("analytics", "events") in names
    assert ("staging", "raw_events") in names


def test_introspect_schema_columns_attached_correctly() -> None:
    client = _FakeBigQueryClient(column_rows=_SAMPLE_COLUMN_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    events_obj = next(o for o in snapshot.objects if o.object_name == "events")
    assert len(events_obj.columns) == 2
    col_names = [c.name for c in events_obj.columns]
    assert "event_id" in col_names
    assert "created_at" in col_names


def test_introspect_schema_is_nullable_parsed() -> None:
    client = _FakeBigQueryClient(column_rows=_SAMPLE_COLUMN_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    events_obj = next(o for o in snapshot.objects if o.object_name == "events")
    event_id_col = next(c for c in events_obj.columns if c.name == "event_id")
    created_at_col = next(c for c in events_obj.columns if c.name == "created_at")
    assert event_id_col.is_nullable is False
    assert created_at_col.is_nullable is True


def test_introspect_schema_empty_project() -> None:
    client = _FakeBigQueryClient(column_rows=[])
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    assert snapshot.objects == ()


def test_introspect_schema_query_uses_correct_region() -> None:
    client = _FakeBigQueryClient(column_rows=[])
    bq = _make_bq_adapter(client)
    adapter = _make_adapter(location="eu")

    asyncio.run(bq.introspect_schema(adapter))

    columns_query = next(q for q in client.queries_issued if "COLUMNS" in q)
    assert "region-eu" in columns_query


def test_introspect_schema_skips_rows_with_empty_names() -> None:
    rows = [
        {
            "table_schema": "",
            "table_name": "events",
            "column_name": "id",
            "data_type": "INT64",
            "is_nullable": "NO",
            "ordinal_position": 1,
        },
        {
            "table_schema": "ds",
            "table_name": "tbl",
            "column_name": "col",
            "data_type": "STRING",
            "is_nullable": "YES",
            "ordinal_position": 1,
        },
    ]
    client = _FakeBigQueryClient(column_rows=rows)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].object_name == "tbl"


# ---------------------------------------------------------------------------
# observe_traffic
# ---------------------------------------------------------------------------

_SAMPLE_JOB_ROWS = [
    {
        "job_id": "bq-job-001",
        "creation_time": datetime(2026, 3, 10, 12, 0, tzinfo=UTC),
        "end_time": datetime(2026, 3, 10, 12, 0, 5, tzinfo=UTC),
        "user_email": "alice@example.com",
        "labels": {"dag_id": "etl", "task_id": "load"},
        "query": "SELECT * FROM analytics.events",
        "referenced_tables": [
            {"project_id": "acme-project", "dataset_id": "analytics", "table_id": "events"}
        ],
    },
    {
        "job_id": "bq-job-002",
        "creation_time": datetime(2026, 3, 10, 13, 0, tzinfo=UTC),
        "end_time": datetime(2026, 3, 10, 13, 0, 2, tzinfo=UTC),
        "user_email": "bob@example.com",
        "labels": {},
        "query": "SELECT count(*) FROM staging.raw",
        "referenced_tables": [],
    },
]


def test_observe_traffic_returns_events() -> None:
    client = _FakeBigQueryClient(job_rows=_SAMPLE_JOB_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    assert result.scanned_records == 2
    assert len(result.events) == 2


def test_observe_traffic_event_fields() -> None:
    client = _FakeBigQueryClient(job_rows=_SAMPLE_JOB_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    # First event should have airflow identity from labels
    airflow_event = next(e for e in result.events if "etl" in e.source_name)
    assert airflow_event.query_type == "query_job"
    assert airflow_event.sql == "SELECT * FROM analytics.events"
    assert airflow_event.database_name == "acme-project"
    assert airflow_event.duration_ms is not None
    assert airflow_event.duration_ms >= 0


def test_observe_traffic_event_id_is_stable_uuid() -> None:
    client = _FakeBigQueryClient(job_rows=_SAMPLE_JOB_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result1 = asyncio.run(bq.observe_traffic(adapter))
    result2 = asyncio.run(bq.observe_traffic(adapter))

    ids1 = {e.event_id for e in result1.events}
    ids2 = {e.event_id for e in result2.events}
    assert ids1 == ids2
    # All IDs must be valid UUIDs
    for event_id in ids1:
        assert event_id is not None
        UUID(event_id)  # raises if invalid


def test_observe_traffic_skips_empty_sql() -> None:
    rows = [
        {
            "job_id": "bq-job-empty",
            "creation_time": datetime(2026, 3, 10, tzinfo=UTC),
            "end_time": datetime(2026, 3, 10, tzinfo=UTC),
            "user_email": None,
            "labels": {},
            "query": "   ",  # blank — must be skipped
            "referenced_tables": [],
        }
    ]
    client = _FakeBigQueryClient(job_rows=rows)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    assert len(result.events) == 0
    assert result.scanned_records == 1


def test_observe_traffic_metadata_contains_referenced_tables() -> None:
    client = _FakeBigQueryClient(job_rows=_SAMPLE_JOB_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    event = next(e for e in result.events if e.statement_id == "bq-job-001")
    assert "referenced_tables" in event.metadata
    assert len(event.metadata["referenced_tables"]) == 1


def test_observe_traffic_uses_since_parameter() -> None:
    client = _FakeBigQueryClient(job_rows=_SAMPLE_JOB_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    since = datetime(2026, 3, 1, tzinfo=UTC)
    asyncio.run(bq.observe_traffic(adapter, since=since))

    jobs_query = next(q for q in client.queries_issued if "JOBS_BY_PROJECT" in q)
    assert "@since" in jobs_query


def test_observe_traffic_uses_cursor_parameter_when_available() -> None:
    client = _FakeBigQueryClient(job_rows=_SAMPLE_JOB_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter(
        observation_cursor={"bq_creation_time": "2026-03-10T11:30:00+00:00"}
    )

    asyncio.run(bq.observe_traffic(adapter, since=datetime(2026, 3, 1, tzinfo=UTC)))

    jobs_query = next(q for q in client.queries_issued if "JOBS_BY_PROJECT" in q)
    assert "creation_time > @cursor" in jobs_query
    assert "@since" not in jobs_query

    jobs_config = next(
        c for c in client.job_configs_issued if hasattr(c, "query_parameters")
    )
    cursor_param = next(p for p in jobs_config.query_parameters if p.name == "cursor")
    assert cursor_param.value == datetime(2026, 3, 10, 11, 30, tzinfo=UTC)


def test_observe_traffic_returns_cursor_for_latest_creation_time() -> None:
    client = _FakeBigQueryClient(job_rows=_SAMPLE_JOB_ROWS)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    assert result.observation_cursor == {
        "bq_creation_time": "2026-03-10T13:00:00+00:00"
    }


def test_observe_traffic_empty_result_does_not_advance_cursor() -> None:
    client = _FakeBigQueryClient(job_rows=[])
    bq = _make_bq_adapter(client)
    adapter = _make_adapter(
        observation_cursor={"bq_creation_time": "2026-03-10T11:30:00+00:00"}
    )

    result = asyncio.run(bq.observe_traffic(adapter))

    assert result.observation_cursor is None


def test_observe_traffic_since_clamped_to_180_days() -> None:
    """A since older than 180 days is clamped to the retention boundary."""
    ancient = datetime.now(tz=UTC) - timedelta(days=200)
    boundary = datetime.now(tz=UTC) - timedelta(days=180)

    client = _FakeBigQueryClient(job_rows=[])
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    asyncio.run(bq.observe_traffic(adapter, since=ancient))

    # The TIMESTAMP parameter passed to BQ should be near the boundary
    jobs_config = next(
        c for c in client.job_configs_issued if hasattr(c, "query_parameters")
        and any(getattr(p, "name", None) == "since" for p in c.query_parameters)
    )
    since_param = next(p for p in jobs_config.query_parameters if p.name == "since")
    delta = abs((since_param.value - boundary).total_seconds())
    assert delta < 10  # within 10 seconds of the boundary


def test_observe_traffic_empty_result() -> None:
    client = _FakeBigQueryClient(job_rows=[])
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    assert result.scanned_records == 0
    assert result.events == ()


# ---------------------------------------------------------------------------
# execute_query
# ---------------------------------------------------------------------------


def test_execute_query_returns_rows() -> None:
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    client = _FakeBigQueryClient(query_rows=rows)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.execute_query(adapter, "SELECT id, name FROM users"))

    assert result.success is True
    assert result.row_count == 2
    assert result.duration_ms >= 0


def test_execute_query_respects_max_rows() -> None:
    rows = [{"id": i} for i in range(20)]
    client = _FakeBigQueryClient(query_rows=rows)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.execute_query(adapter, "SELECT id FROM t", max_rows=5))

    assert result.row_count == 5
    assert result.truncated is True


def test_execute_query_dry_run() -> None:
    client = _FakeBigQueryClient()
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.execute_query(adapter, "SELECT * FROM big_table", dry_run=True))

    assert result.success is True
    assert result.row_count == 0
    # Verify dry_run=True was passed to job config
    dry_run_config = next(
        c for c in client.job_configs_issued if getattr(c, "dry_run", False)
    )
    assert dry_run_config.dry_run is True


def test_execute_query_returns_error_on_exception() -> None:
    err = Exception("syntax error at or near 'SELCT'")
    client = _FakeBigQueryClient(raise_on_query=err)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.execute_query(adapter, "SELCT 1"))

    assert result.success is False
    assert result.row_count == 0
    assert result.error_message is not None
    assert "syntax error" in result.error_message


# ---------------------------------------------------------------------------
# get_setup_instructions
# ---------------------------------------------------------------------------


def test_get_setup_instructions_returns_instructions() -> None:
    bq = BigQueryAdapter(resolve_secret=lambda s: "")
    instructions = bq.get_setup_instructions()

    assert instructions.title
    assert instructions.summary
    assert len(instructions.steps) >= 3
    assert instructions.docs_url is not None
    # Should mention IAM / service account
    combined = " ".join(instructions.steps).lower()
    assert "service account" in combined
    assert "bigquery" in combined.lower() or "bq" in combined.lower()


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


def test_bigquery_adapter_satisfies_source_adapter_protocol() -> None:
    from alma_connectors.source_adapter import SourceAdapter

    bq = BigQueryAdapter(resolve_secret=lambda s: "")
    assert isinstance(bq, SourceAdapter)
    assert bq.kind == SourceAdapterKind.BIGQUERY


# ---------------------------------------------------------------------------
# introspect_schema — TABLE_STORAGE + partition/clustering
# ---------------------------------------------------------------------------


def test_introspect_schema_populates_row_count_and_size() -> None:
    client = _FakeBigQueryClient(
        column_rows=_SAMPLE_COLUMN_ROWS,
        storage_rows=_SAMPLE_STORAGE_ROWS,
    )
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    events_obj = next(o for o in snapshot.objects if o.object_name == "events")
    assert events_obj.row_count == 1_000_000
    assert events_obj.size_bytes == 512_000_000


def test_introspect_schema_populates_partition_column() -> None:
    client = _FakeBigQueryClient(
        column_rows=_SAMPLE_COLUMN_ROWS,
        storage_rows=_SAMPLE_STORAGE_ROWS,
    )
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    # _SAMPLE_COLUMN_ROWS marks 'created_at' as the partition column
    events_obj = next(o for o in snapshot.objects if o.object_name == "events")
    assert events_obj.partition_column == "created_at"
    # staging.raw_events has no partition column
    raw_obj = next(o for o in snapshot.objects if o.object_name == "raw_events")
    assert raw_obj.partition_column is None


def test_introspect_schema_populates_clustering_columns() -> None:
    column_rows = [
        {
            "table_schema": "ds",
            "table_name": "clustered_tbl",
            "column_name": "region",
            "data_type": "STRING",
            "is_nullable": "NO",
            "ordinal_position": 1,
            "is_partitioning_column": "NO",
            "clustering_ordinal_position": 2,
        },
        {
            "table_schema": "ds",
            "table_name": "clustered_tbl",
            "column_name": "country",
            "data_type": "STRING",
            "is_nullable": "NO",
            "ordinal_position": 2,
            "is_partitioning_column": "NO",
            "clustering_ordinal_position": 1,
        },
    ]
    client = _FakeBigQueryClient(column_rows=column_rows)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    obj = snapshot.objects[0]
    # Clustering columns should be sorted by ordinal position: country (1) before region (2)
    assert obj.clustering_columns == ("country", "region")


def test_introspect_schema_table_storage_failure_is_non_fatal() -> None:
    """If TABLE_STORAGE query fails, introspect_schema still returns objects."""
    client = _FakeBigQueryClient(
        column_rows=_SAMPLE_COLUMN_ROWS,
        raise_on_storage_query=Exception("403 permission denied on TABLE_STORAGE"),
    )
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    snapshot = asyncio.run(bq.introspect_schema(adapter))

    # Objects are still returned; row_count/size_bytes are absent
    assert len(snapshot.objects) == 2
    for obj in snapshot.objects:
        assert obj.row_count is None
        assert obj.size_bytes is None


def test_introspect_schema_columns_query_includes_partition_fields() -> None:
    client = _FakeBigQueryClient(column_rows=[])
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    asyncio.run(bq.introspect_schema(adapter))

    columns_query = next(q for q in client.queries_issued if "INFORMATION_SCHEMA.COLUMNS" in q)
    assert "is_partitioning_column" in columns_query
    assert "clustering_ordinal_position" in columns_query


# ---------------------------------------------------------------------------
# _extract_tables_from_sql
# ---------------------------------------------------------------------------


def test_extract_tables_from_sql_backtick_three_part() -> None:
    sql = "SELECT * FROM `my-project.my_dataset.my_table`"
    tables = _extract_tables_from_sql(sql)
    assert len(tables) == 1
    assert tables[0] == {
        "project_id": "my-project",
        "dataset_id": "my_dataset",
        "table_id": "my_table",
    }


def test_extract_tables_from_sql_backtick_two_part() -> None:
    sql = "SELECT * FROM `analytics.events`"
    tables = _extract_tables_from_sql(sql)
    assert len(tables) == 1
    assert tables[0]["dataset_id"] == "analytics"
    assert tables[0]["table_id"] == "events"


def test_extract_tables_from_sql_from_join_unquoted() -> None:
    sql = "SELECT a.id FROM dataset_a.table_a a JOIN dataset_b.table_b b ON a.id = b.id"
    tables = _extract_tables_from_sql(sql)
    names = {(t["dataset_id"], t["table_id"]) for t in tables}
    assert ("dataset_a", "table_a") in names
    assert ("dataset_b", "table_b") in names


def test_extract_tables_from_sql_deduplicates() -> None:
    sql = "SELECT * FROM `proj.ds.tbl` WHERE id IN (SELECT id FROM `proj.ds.tbl`)"
    tables = _extract_tables_from_sql(sql)
    assert len(tables) == 1


def test_extract_tables_from_sql_skips_information_schema() -> None:
    sql = "SELECT * FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`"
    tables = _extract_tables_from_sql(sql)
    assert tables == []


def test_extract_tables_from_sql_empty_sql() -> None:
    assert _extract_tables_from_sql("") == []


def test_extract_tables_from_sql_no_tables() -> None:
    assert _extract_tables_from_sql("SELECT 1") == []


# ---------------------------------------------------------------------------
# observe_traffic — SQL fallback when referenced_tables is empty
# ---------------------------------------------------------------------------


def test_observe_traffic_sql_fallback_when_referenced_tables_empty() -> None:
    """When BQ returns empty referenced_tables, fall back to SQL parsing."""
    rows = [
        {
            "job_id": "bq-job-fallback",
            "creation_time": datetime(2026, 3, 10, tzinfo=UTC),
            "end_time": datetime(2026, 3, 10, tzinfo=UTC),
            "user_email": "alice@example.com",
            "labels": {},
            "query": "SELECT * FROM `acme-project.analytics.events`",
            "referenced_tables": [],  # empty — should trigger SQL fallback
        }
    ]
    client = _FakeBigQueryClient(job_rows=rows)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    assert len(result.events) == 1
    event = result.events[0]
    refs = event.metadata["referenced_tables"]
    assert len(refs) == 1
    assert refs[0]["dataset_id"] == "analytics"
    assert refs[0]["table_id"] == "events"


def test_observe_traffic_no_fallback_when_referenced_tables_present() -> None:
    """When BQ populates referenced_tables, SQL fallback is not used."""
    rows = [
        {
            "job_id": "bq-job-003",
            "creation_time": datetime(2026, 3, 10, tzinfo=UTC),
            "end_time": datetime(2026, 3, 10, tzinfo=UTC),
            "user_email": "bob@example.com",
            "labels": {},
            "query": "SELECT * FROM analytics.events",
            "referenced_tables": [
                {"project_id": "acme-project", "dataset_id": "analytics", "table_id": "events"}
            ],
        }
    ]
    client = _FakeBigQueryClient(job_rows=rows)
    bq = _make_bq_adapter(client)
    adapter = _make_adapter()

    result = asyncio.run(bq.observe_traffic(adapter))

    event = result.events[0]
    refs = event.metadata["referenced_tables"]
    # BQ-provided list should be used (1 entry), not the SQL-parsed result
    assert len(refs) == 1
    assert refs[0]["project_id"] == "acme-project"


# ---------------------------------------------------------------------------
# SourceTableSchema — new field validation
# ---------------------------------------------------------------------------


def test_source_table_schema_row_count_must_be_non_negative() -> None:
    from alma_connectors import SchemaObjectKind, SourceTableSchema

    with pytest.raises(ValueError, match="row_count must be >= 0"):
        SourceTableSchema(
            schema_name="ds",
            object_name="tbl",
            object_kind=SchemaObjectKind.TABLE,
            row_count=-1,
        )


def test_source_table_schema_size_bytes_must_be_non_negative() -> None:
    from alma_connectors import SchemaObjectKind, SourceTableSchema

    with pytest.raises(ValueError, match="size_bytes must be >= 0"):
        SourceTableSchema(
            schema_name="ds",
            object_name="tbl",
            object_kind=SchemaObjectKind.TABLE,
            size_bytes=-1,
        )


def test_source_table_schema_optional_fields_default() -> None:
    from alma_connectors import SchemaObjectKind, SourceTableSchema

    obj = SourceTableSchema(
        schema_name="ds", object_name="tbl", object_kind=SchemaObjectKind.TABLE
    )
    assert obj.row_count is None
    assert obj.size_bytes is None
    assert obj.partition_column is None
    assert obj.clustering_columns == ()
