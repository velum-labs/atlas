"""Tests for PostgresAdapter enhancements: logging check and pg_stat_statements."""

from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from alma_connectors import (
    ExternalSecretRef,
    PersistedSourceAdapter,
    PostgresAdapterConfig,
    PostgresLogCaptureConfig,
    SourceAdapterKind,
    SourceAdapterService,
    SourceAdapterStatus,
)

_PSYCOPG_CONNECT = "alma_connectors.adapters.postgres.psycopg.connect"


def _make_service() -> SourceAdapterService:
    return SourceAdapterService(encryption_key=Fernet.generate_key().decode("utf-8"))


def _make_adapter_with_logs(*, adapter_id: str, log_path: Path) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=adapter_id,
        key="app-db",
        display_name="App DB",
        kind=SourceAdapterKind.POSTGRES,
        target_id="local-dev",
        status=SourceAdapterStatus.READY,
        config=PostgresAdapterConfig(
            database_secret=ExternalSecretRef(provider="env", reference="APP_DB_DSN"),
            log_capture=PostgresLogCaptureConfig(
                log_path=str(log_path),
                default_source="fallback-source",
                default_database_name="observatory",
                default_database_user="postgres",
            ),
        ),
    )


def _make_adapter_no_logs(*, adapter_id: str) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=adapter_id,
        key="app-db",
        display_name="App DB",
        kind=SourceAdapterKind.POSTGRES,
        target_id="local-dev",
        status=SourceAdapterStatus.READY,
        config=PostgresAdapterConfig(
            database_secret=ExternalSecretRef(provider="env", reference="APP_DB_DSN"),
        ),
    )


# ---------------------------------------------------------------------------
# test_connection — log_min_duration_statement check
# ---------------------------------------------------------------------------


def _mock_conn_for_test_connection(*, log_setting: str | None, table_count: int = 3) -> MagicMock:
    """Build a mock psycopg connection that returns preset values for test_connection."""
    table_row = {"cnt": table_count}
    log_row = {"setting": log_setting} if log_setting is not None else None

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    # execute() returns a cursor; fetchone() returns the row.
    # First call → table count, second call → log setting.
    cursor_table = MagicMock()
    cursor_table.fetchone.return_value = table_row
    cursor_log = MagicMock()
    cursor_log.fetchone.return_value = log_row

    conn.execute.side_effect = [cursor_table, cursor_log]
    return conn


def test_test_connection_reports_log_setting_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _mock_conn_for_test_connection(log_setting="0", table_count=5)
    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.test_connection(adapter))

    assert result.success is True
    assert result.resource_count == 5
    assert "log_min_duration_statement=0ms" in result.message


def test_test_connection_warns_when_logging_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _mock_conn_for_test_connection(log_setting="-1", table_count=2)
    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000002")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.test_connection(adapter))

    assert result.success is True
    assert "not configured" in result.message
    assert "pg_stat_statements" in result.message


def test_test_connection_warns_when_pg_settings_row_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _mock_conn_for_test_connection(log_setting=None, table_count=1)
    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000003")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.test_connection(adapter))

    assert result.success is True
    assert "not configured" in result.message


# ---------------------------------------------------------------------------
# observe_traffic — dispatches to log vs pg_stat_statements
# ---------------------------------------------------------------------------


def test_observe_traffic_uses_log_file_when_log_capture_set(tmp_path: Path) -> None:
    log_path = tmp_path / "postgres.log"
    log_path.write_text(
        "2026-01-01 00:00:00 UTC [1] db=obs,user=pg,app=api,client=127.0.0.1"
        " LOG:  duration: 5.0 ms statement: SELECT 1\n",
        encoding="utf-8",
    )
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000010",
        log_path=log_path,
    )
    result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 1
    assert len(result.events) == 1
    assert result.events[0].query_type == "duration_statement"


def test_observe_traffic_uses_pg_stat_statements_when_no_log_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stat_rows = [
        {
            "queryid": "111",
            "query": "SELECT * FROM users",
            "calls": 10,
            "total_exec_time": 100.0,
            "mean_exec_time": 10.0,
            "username": "app_user",
            "dbname": "mydb",
        }
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor = MagicMock()
    cursor.fetchall.return_value = stat_rows
    conn.execute.return_value = cursor

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000020")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 1
    assert len(result.events) == 1
    event = result.events[0]
    assert event.query_type == "pg_stat_statements"
    assert event.sql == "SELECT * FROM users"
    assert event.database_name == "mydb"
    assert event.database_user == "app_user"
    assert event.duration_ms == 10.0
    assert event.metadata["calls"] == 10
    assert event.metadata["adapter"] == "pg_stat_statements"
    assert event.event_id is not None


def test_observe_traffic_pg_stat_statements_since_emits_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    conn.execute.return_value = cursor

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000021")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(
            service.observe_traffic(adapter, since=datetime(2026, 1, 1, tzinfo=UTC))
        )

    assert len(result.errors) == 1
    assert "pg_stat_statements does not support time-based filtering" in result.errors[0]


def test_observe_traffic_pg_stat_statements_extension_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    err = psycopg.ProgrammingError("relation does not exist")
    err.pgcode = "42P01"  # type: ignore[attr-defined]
    conn.execute.side_effect = err

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000022")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 0
    assert len(result.events) == 0
    assert len(result.errors) == 1
    assert "pg_stat_statements extension is not installed" in result.errors[0]


def test_observe_traffic_pg_stat_statements_insufficient_privilege(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    err = psycopg.ProgrammingError("permission denied")
    err.pgcode = "42501"  # type: ignore[attr-defined]
    conn.execute.side_effect = err

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000023")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 0
    assert len(result.errors) == 1
    assert "insufficient privilege" in result.errors[0]


def test_observe_traffic_pg_stat_statements_skips_empty_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _t = {"calls": 1, "total_exec_time": 1.0, "mean_exec_time": 1.0, "username": "u", "dbname": "d"}
    stat_rows = [
        {"queryid": "1", "query": "", **_t},
        {"queryid": "2", "query": "SELECT 1", **_t},
        {
            "queryid": None,
            "query": None,
            "calls": 0,
            "total_exec_time": 0.0,
            "mean_exec_time": 0.0,
            "username": None,
            "dbname": None,
        },
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor = MagicMock()
    cursor.fetchall.return_value = stat_rows
    conn.execute.return_value = cursor

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000024")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 3
    assert len(result.events) == 1
    assert result.events[0].sql == "SELECT 1"


# ---------------------------------------------------------------------------
# _observe_from_logs — file error and since= filter
# ---------------------------------------------------------------------------


def test_observe_traffic_logs_missing_file_returns_error(tmp_path: Path) -> None:
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000040",
        log_path=tmp_path / "nonexistent.log",
    )
    result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 0
    assert len(result.events) == 0
    assert len(result.errors) == 1
    assert "nonexistent.log" in result.errors[0]


def test_observe_traffic_logs_permission_error_returns_error(tmp_path: Path) -> None:
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000041",
        log_path=tmp_path / "postgres.log",
    )
    with patch(
        "alma_connectors.adapters.postgres.Path.open",
        side_effect=PermissionError("Permission denied"),
    ):
        result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 0
    assert len(result.events) == 0
    assert len(result.errors) == 1
    assert "Permission denied" in result.errors[0]


def test_observe_traffic_logs_since_filter_excludes_old_lines(tmp_path: Path) -> None:
    from datetime import UTC, datetime

    log_path = tmp_path / "postgres.log"
    log_path.write_text(
        # old line — before the cutoff
        "2026-01-01 00:00:00 UTC [1] db=obs,user=pg,app=api,client=127.0.0.1"
        " LOG:  duration: 1.0 ms statement: SELECT old\n"
        # new line — after the cutoff
        "2026-02-01 00:00:00 UTC [2] db=obs,user=pg,app=api,client=127.0.0.1"
        " LOG:  duration: 2.0 ms statement: SELECT new\n",
        encoding="utf-8",
    )
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000041",
        log_path=log_path,
    )
    result = asyncio.run(service.observe_traffic(adapter, since=datetime(2026, 1, 15, tzinfo=UTC)))

    assert result.scanned_records == 2
    assert len(result.events) == 1
    assert "SELECT new" in result.events[0].sql


# ---------------------------------------------------------------------------
# _observe_from_logs — observation_cursor (inode + offset seek)
# ---------------------------------------------------------------------------


def test_observe_traffic_logs_cursor_returned_in_result(tmp_path: Path) -> None:
    """Result includes observation_cursor with inode and offset after a successful read."""
    log_path = tmp_path / "postgres.log"
    log_path.write_text(
        "2026-01-01 00:00:00 UTC [1] db=obs,user=pg,app=api,client=127.0.0.1"
        " LOG:  duration: 5.0 ms statement: SELECT 1\n",
        encoding="utf-8",
    )
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000060",
        log_path=log_path,
    )
    result = asyncio.run(service.observe_traffic(adapter))

    assert result.observation_cursor is not None
    assert "inode" in result.observation_cursor
    assert "offset" in result.observation_cursor
    assert result.observation_cursor["inode"] == log_path.stat().st_ino
    assert isinstance(result.observation_cursor["offset"], int)
    assert result.observation_cursor["offset"] > 0


def test_observe_traffic_logs_cursor_skips_lines_before_offset(tmp_path: Path) -> None:
    """Cursor with matching inode seeks to stored offset; only lines after it are emitted."""
    old_line = (
        "2026-01-01 00:00:00 UTC [1] db=obs,user=pg,app=api,client=127.0.0.1"
        " LOG:  duration: 1.0 ms statement: SELECT old\n"
    )
    new_line = (
        "2026-02-01 00:00:00 UTC [2] db=obs,user=pg,app=api,client=127.0.0.1"
        " LOG:  duration: 2.0 ms statement: SELECT new\n"
    )
    log_path = tmp_path / "postgres.log"
    log_path.write_text(old_line + new_line, encoding="utf-8")

    # Compute the exact tell() position after reading the first line in text mode.
    with log_path.open("r", encoding="utf-8") as fh:
        fh.readline()
        cursor_offset = fh.tell()

    file_inode = log_path.stat().st_ino
    service = _make_service()
    base = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000061",
        log_path=log_path,
    )
    adapter = dataclasses.replace(
        base,
        observation_cursor={"inode": file_inode, "offset": cursor_offset},
    )
    result = asyncio.run(service.observe_traffic(adapter))

    assert result.scanned_records == 1
    assert len(result.events) == 1
    assert "SELECT new" in result.events[0].sql


def test_observe_traffic_logs_cursor_restarts_on_inode_change(tmp_path: Path) -> None:
    """Cursor with a different inode (log rotation) causes a full re-read from offset 0."""
    log_path = tmp_path / "postgres.log"
    log_path.write_text(
        "2026-02-01 00:00:00 UTC [1] db=obs,user=pg,app=api,client=127.0.0.1"
        " LOG:  duration: 2.0 ms statement: SELECT after_rotation\n",
        encoding="utf-8",
    )
    service = _make_service()
    base = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000062",
        log_path=log_path,
    )
    # Use a bogus inode that does not match the actual file.
    adapter = dataclasses.replace(
        base,
        observation_cursor={"inode": 0, "offset": 9999},
    )
    result = asyncio.run(service.observe_traffic(adapter))

    # All lines in the rotated file should be read (offset ignored).
    assert result.scanned_records == 1
    assert len(result.events) == 1
    assert "SELECT after_rotation" in result.events[0].sql


# ---------------------------------------------------------------------------
# get_capabilities — always reports can_observe_traffic=True for Postgres
# ---------------------------------------------------------------------------


def test_get_capabilities_reports_traffic_true_without_log_capture() -> None:
    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000030")
    caps = service.get_capabilities(adapter)
    assert caps.can_observe_traffic is True


def test_get_capabilities_reports_traffic_true_with_log_capture(tmp_path: Path) -> None:
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000031",
        log_path=tmp_path / "pg.log",
    )
    caps = service.get_capabilities(adapter)
    assert caps.can_observe_traffic is True


# ---------------------------------------------------------------------------
# get_setup_instructions — content assertions
# ---------------------------------------------------------------------------


def test_get_setup_instructions_mentions_log_min_duration_statement() -> None:
    service = _make_service()
    instructions = service.get_setup_instructions(SourceAdapterKind.POSTGRES)
    full_text = " ".join(instructions.steps)
    assert "log_min_duration_statement" in full_text


def test_get_setup_instructions_mentions_pg_stat_statements() -> None:
    service = _make_service()
    instructions = service.get_setup_instructions(SourceAdapterKind.POSTGRES)
    full_text = " ".join(instructions.steps)
    assert "pg_stat_statements" in full_text


def test_get_setup_instructions_mentions_log_line_prefix() -> None:
    service = _make_service()
    instructions = service.get_setup_instructions(SourceAdapterKind.POSTGRES)
    full_text = " ".join(instructions.steps)
    assert "log_line_prefix" in full_text


# ---------------------------------------------------------------------------
# execute_query — basic success path
# ---------------------------------------------------------------------------


def test_execute_query_returns_rows_and_row_count(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor = MagicMock()
    cursor.fetchmany.return_value = rows
    conn.execute.return_value = cursor

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000050")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.execute_query(adapter, "SELECT id, name FROM users"))

    assert result.success is True
    assert result.row_count == 2
    assert result.rows == ({"id": 1, "name": "alice"}, {"id": 2, "name": "bob"})
    assert result.truncated is False


def test_execute_query_truncates_when_over_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    # fetchmany returns max_rows+1 rows to signal truncation
    rows = [{"n": i} for i in range(101)]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor = MagicMock()
    cursor.fetchmany.return_value = rows
    conn.execute.return_value = cursor

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000051")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        result = asyncio.run(service.execute_query(adapter, "SELECT n FROM big_table"))

    assert result.truncated is True
    assert result.row_count == 100
    assert len(result.rows) == 100


# ---------------------------------------------------------------------------
# introspect_schema — basic success path
# ---------------------------------------------------------------------------


def test_introspect_schema_builds_snapshot_from_column_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema_rows = [
        {
            "schema_name": "public",
            "table_name": "users",
            "column_name": "id",
            "data_type": "integer",
            "is_nullable": "NO",
            "table_type": "BASE TABLE",
        },
        {
            "schema_name": "public",
            "table_name": "users",
            "column_name": "email",
            "data_type": "text",
            "is_nullable": "YES",
            "table_type": "BASE TABLE",
        },
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor_schema = MagicMock()
    cursor_schema.fetchall.return_value = schema_rows
    cursor_dep = MagicMock()
    cursor_dep.fetchall.return_value = []
    cursor_stats = MagicMock()
    cursor_stats.fetchall.return_value = []
    conn.execute.side_effect = [cursor_schema, cursor_dep, cursor_stats]

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000060")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(service.introspect_schema(adapter))

    assert len(snapshot.objects) == 1
    obj = snapshot.objects[0]
    assert obj.schema_name == "public"
    assert obj.object_name == "users"
    assert len(obj.columns) == 2
    col_names = {c.name for c in obj.columns}
    assert col_names == {"id", "email"}
    assert len(snapshot.dependencies) == 0


def test_introspect_schema_maps_view_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    schema_rows = [
        {
            "schema_name": "public",
            "table_name": "active_users",
            "column_name": "id",
            "data_type": "integer",
            "is_nullable": "NO",
            "table_type": "VIEW",
        },
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor_schema = MagicMock()
    cursor_schema.fetchall.return_value = schema_rows
    cursor_dep = MagicMock()
    cursor_dep.fetchall.return_value = []
    cursor_stats = MagicMock()
    cursor_stats.fetchall.return_value = []
    conn.execute.side_effect = [cursor_schema, cursor_dep, cursor_stats]

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000061")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(service.introspect_schema(adapter))

    from alma_connectors import SchemaObjectKind

    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].object_kind == SchemaObjectKind.VIEW


def test_introspect_schema_maps_materialized_view_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    schema_rows = [
        {
            "schema_name": "analytics",
            "table_name": "mv_daily_stats",
            "column_name": "day",
            "data_type": "date",
            "is_nullable": "NO",
            "table_type": "MATERIALIZED VIEW",
        },
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor_schema = MagicMock()
    cursor_schema.fetchall.return_value = schema_rows
    cursor_dep = MagicMock()
    cursor_dep.fetchall.return_value = []
    cursor_stats = MagicMock()
    cursor_stats.fetchall.return_value = []
    conn.execute.side_effect = [cursor_schema, cursor_dep, cursor_stats]

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000062")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(service.introspect_schema(adapter))

    from alma_connectors import SchemaObjectKind

    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].object_kind == SchemaObjectKind.MATERIALIZED_VIEW


def test_introspect_schema_surfaces_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    dep_rows = [
        {
            "source_schema": "public",
            "source_object": "active_users",
            "target_schema": "public",
            "target_object": "users",
        }
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor_schema = MagicMock()
    cursor_schema.fetchall.return_value = []
    cursor_dep = MagicMock()
    cursor_dep.fetchall.return_value = dep_rows
    cursor_stats = MagicMock()
    cursor_stats.fetchall.return_value = []
    conn.execute.side_effect = [cursor_schema, cursor_dep, cursor_stats]

    service = _make_service()
    adapter = _make_adapter_no_logs(adapter_id="00000000-0000-0000-0000-000000000063")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(service.introspect_schema(adapter))

    assert len(snapshot.dependencies) == 1
    dep = snapshot.dependencies[0]
    assert dep.source_schema == "public"
    assert dep.source_object == "active_users"
    assert dep.target_schema == "public"
    assert dep.target_object == "users"


# ---------------------------------------------------------------------------
# _observe_from_logs — STATEMENT and ERROR+STATEMENT pairing
# ---------------------------------------------------------------------------


def test_observe_traffic_logs_statement_type(tmp_path: Path) -> None:
    log_path = tmp_path / "postgres.log"
    log_path.write_text(
        "2026-03-01 12:00:00 UTC [42] db=mydb,user=alice,app=backend,client=10.0.0.1"
        " STATEMENT:  SELECT count(*) FROM orders\n",
        encoding="utf-8",
    )
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000070",
        log_path=log_path,
    )
    result = asyncio.run(service.observe_traffic(adapter))

    assert len(result.events) == 1
    event = result.events[0]
    assert event.query_type == "statement"
    assert "SELECT count(*)" in event.sql


def test_observe_traffic_logs_error_statement_pair(tmp_path: Path) -> None:
    """ERROR followed by STATEMENT should produce a single error_statement event."""
    log_path = tmp_path / "postgres.log"
    log_path.write_text(
        "2026-03-01 12:00:00 UTC [99] db=mydb,user=bob,app=api,client=10.0.0.2"
        " ERROR:  division by zero\n"
        "2026-03-01 12:00:00 UTC [99] db=mydb,user=bob,app=api,client=10.0.0.2"
        " STATEMENT:  SELECT 1/0\n",
        encoding="utf-8",
    )
    service = _make_service()
    adapter = _make_adapter_with_logs(
        adapter_id="00000000-0000-0000-0000-000000000071",
        log_path=log_path,
    )
    result = asyncio.run(service.observe_traffic(adapter))

    assert len(result.events) == 1
    event = result.events[0]
    assert event.query_type == "error_statement"
    assert event.error_message == "division by zero"
    assert "SELECT 1/0" in event.sql


# ---------------------------------------------------------------------------
# _parse_postgres_log_timestamp — timezone formats
# ---------------------------------------------------------------------------


def test_parse_postgres_log_timestamp_utc_abbreviation() -> None:
    from datetime import UTC, datetime

    from alma_connectors.adapters.postgres import _parse_postgres_log_timestamp

    result = _parse_postgres_log_timestamp("2026-03-01 10:00:00 UTC")
    assert result == datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)


def test_parse_postgres_log_timestamp_numeric_offset() -> None:
    from datetime import UTC, datetime

    from alma_connectors.adapters.postgres import _parse_postgres_log_timestamp

    result = _parse_postgres_log_timestamp("2026-03-01 10:00:00 +05:00")
    expected = datetime(2026, 3, 1, 5, 0, 0, tzinfo=UTC)
    assert result == expected


def test_parse_postgres_log_timestamp_no_timezone_defaults_utc() -> None:
    from datetime import UTC, datetime

    from alma_connectors.adapters.postgres import _parse_postgres_log_timestamp

    result = _parse_postgres_log_timestamp("2026-03-01 10:00:00")
    assert result == datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)


def test_parse_postgres_log_timestamp_pst_abbreviation() -> None:
    from datetime import UTC, datetime

    from alma_connectors.adapters.postgres import _parse_postgres_log_timestamp

    result = _parse_postgres_log_timestamp("2026-03-01 10:00:00 PST")
    expected = datetime(2026, 3, 1, 18, 0, 0, tzinfo=UTC)
    assert result == expected


def test_parse_postgres_log_timestamp_four_digit_offset() -> None:
    from datetime import UTC, datetime

    from alma_connectors.adapters.postgres import _parse_postgres_log_timestamp

    # Postgres can emit e.g. "+0530" (IST) without a colon
    result = _parse_postgres_log_timestamp("2026-03-01 10:00:00 +0530")
    expected = datetime(2026, 3, 1, 4, 30, 0, tzinfo=UTC)
    assert result == expected


def test_parse_postgres_log_timestamp_unknown_abbreviation_raises() -> None:
    from alma_connectors.adapters.postgres import _parse_postgres_log_timestamp

    with pytest.raises(ValueError, match="unsupported postgres log timezone abbreviation"):
        _parse_postgres_log_timestamp("2026-03-01 10:00:00 IST")
