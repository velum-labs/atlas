"""Tests for PostgresAdapter v2 capabilities: probe(), discover(), extract_definitions(), etc."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest

from alma_connectors import (
    ExternalSecretRef,
    PersistedSourceAdapter,
    PostgresAdapterConfig,
    PostgresLogCaptureConfig,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.adapters.postgres import PostgresAdapter
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    ExtractionScope,
    SchemaObjectKind as V2Kind,
)

_PSYCOPG_CONNECT = "alma_connectors.adapters.postgres.psycopg.connect"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pg_adapter(dsn: str = "postgresql://localhost/test") -> PostgresAdapter:
    return PostgresAdapter(resolve_secret=lambda _secret: dsn)


def _make_persisted(
    *,
    adapter_id: str = "00000000-0000-0000-0000-000000000001",
    log_path: str | None = None,
) -> PersistedSourceAdapter:
    log_capture = (
        PostgresLogCaptureConfig(
            log_path=log_path,
            default_source="fallback-source",
            default_database_name="observatory",
            default_database_user="postgres",
        )
        if log_path is not None
        else None
    )
    return PersistedSourceAdapter(
        id=adapter_id,
        key="app-db",
        display_name="App DB",
        kind=SourceAdapterKind.POSTGRES,
        target_id="local-dev",
        status=SourceAdapterStatus.READY,
        config=PostgresAdapterConfig(
            database_secret=ExternalSecretRef(provider="env", reference="APP_DB_DSN"),
            log_capture=log_capture,
        ),
    )


def _probe_conn(
    *,
    info_schema_ok: bool = True,
    pg_proc_ok: bool = True,
    pg_stat_ok: bool = True,
    pg_stat_pgcode: str | None = None,
) -> MagicMock:
    """Build a mock psycopg connection for probe() (autocommit)."""
    import psycopg

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    def _execute(sql: str, *args, **kwargs):  # noqa: ANN001
        lower = sql.strip().lower()
        cur = MagicMock()
        cur.fetchall.return_value = []

        if "information_schema.schemata" in lower:
            if not info_schema_ok:
                exc = psycopg.errors.InsufficientPrivilege("permission denied")
                exc.pgcode = "42501"  # type: ignore[attr-defined]
                raise exc

        elif "pg_catalog.pg_proc" in lower:
            if not pg_proc_ok:
                exc = psycopg.errors.InsufficientPrivilege("permission denied")
                exc.pgcode = "42501"  # type: ignore[attr-defined]
                raise exc

        elif "pg_stat_statements" in lower:
            if not pg_stat_ok:
                if pg_stat_pgcode == "42P01":
                    exc = psycopg.errors.UndefinedTable("relation does not exist")
                    exc.pgcode = "42P01"  # type: ignore[attr-defined]
                    raise exc
                if pg_stat_pgcode == "42501":
                    exc = psycopg.errors.InsufficientPrivilege("permission denied")
                    exc.pgcode = "42501"  # type: ignore[attr-defined]
                    raise exc

        return cur

    conn.execute.side_effect = _execute
    return conn


# ---------------------------------------------------------------------------
# declared_capabilities
# ---------------------------------------------------------------------------


def test_declared_capabilities() -> None:
    pg = _make_pg_adapter()
    caps = pg.declared_capabilities
    assert AdapterCapability.DISCOVER in caps
    assert AdapterCapability.SCHEMA in caps
    assert AdapterCapability.DEFINITIONS in caps
    assert AdapterCapability.TRAFFIC in caps
    assert AdapterCapability.LINEAGE not in caps
    assert AdapterCapability.ORCHESTRATION not in caps


# ---------------------------------------------------------------------------
# probe() — pg_stat_statements presence / absence
# ---------------------------------------------------------------------------


def test_probe_all_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    conn = _probe_conn()

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        results = asyncio.run(pg.probe(adapter))

    by_cap = {r.capability: r for r in results}
    assert by_cap[AdapterCapability.DISCOVER].available is True
    assert by_cap[AdapterCapability.SCHEMA].available is True
    assert by_cap[AdapterCapability.DEFINITIONS].available is True
    assert by_cap[AdapterCapability.TRAFFIC].available is True
    assert by_cap[AdapterCapability.TRAFFIC].fallback_used is False


def test_probe_pg_stat_statements_not_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    conn = _probe_conn(pg_stat_ok=False, pg_stat_pgcode="42P01")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        results = asyncio.run(pg.probe(adapter))

    traffic = next(r for r in results if r.capability == AdapterCapability.TRAFFIC)
    assert traffic.available is False
    assert traffic.message is not None
    assert "pg_stat_statements" in traffic.message


def test_probe_pg_stat_statements_absent_with_log_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    log_file = str(tmp_path / "pg.log")
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted(log_path=log_file)
    conn = _probe_conn(pg_stat_ok=False, pg_stat_pgcode="42P01")

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        results = asyncio.run(pg.probe(adapter))

    traffic = next(r for r in results if r.capability == AdapterCapability.TRAFFIC)
    assert traffic.available is True
    assert traffic.fallback_used is True


def test_probe_handles_permission_errors_gracefully(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    conn = _probe_conn(info_schema_ok=False, pg_proc_ok=False)

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        results = asyncio.run(pg.probe(adapter))

    by_cap = {r.capability: r for r in results}
    assert by_cap[AdapterCapability.DISCOVER].available is False
    assert "SELECT ON information_schema.schemata" in by_cap[AdapterCapability.DISCOVER].permissions_missing
    assert by_cap[AdapterCapability.DEFINITIONS].available is False
    assert "SELECT ON pg_catalog.pg_proc" in by_cap[AdapterCapability.DEFINITIONS].permissions_missing


def test_probe_partial_capabilities(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    conn = _probe_conn()

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        results = asyncio.run(
            pg.probe(adapter, capabilities=frozenset({AdapterCapability.DISCOVER}))
        )

    assert len(results) == 1
    assert results[0].capability == AdapterCapability.DISCOVER
    assert results[0].scope == ExtractionScope.DATABASE


# ---------------------------------------------------------------------------
# discover() — schemas as containers
# ---------------------------------------------------------------------------


def _discover_conn(schema_names: list[str]) -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cur = MagicMock()
    cur.fetchall.return_value = [{"nspname": n} for n in schema_names]
    conn.execute.return_value = cur
    return conn


def test_discover_returns_schemas_as_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    conn = _discover_conn(["public", "analytics", "staging"])

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(pg.discover(adapter))

    assert len(snapshot.containers) == 3
    names = {c.display_name for c in snapshot.containers}
    assert names == {"public", "analytics", "staging"}
    for c in snapshot.containers:
        assert c.container_type == "schema"
        assert c.container_id.startswith("app-db/")

    assert snapshot.meta.capability == AdapterCapability.DISCOVER
    assert snapshot.meta.row_count == 3


def test_discover_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()

    with patch(_PSYCOPG_CONNECT, return_value=_discover_conn(["public"])):
        snapshot = asyncio.run(pg.discover(adapter))

    assert snapshot.meta.adapter_kind.value == "postgres"
    assert snapshot.meta.scope_context.scope == ExtractionScope.DATABASE


# ---------------------------------------------------------------------------
# extract_definitions() — view SQL and routines
# ---------------------------------------------------------------------------


def _definitions_conn(*, view_rows: list[dict], routine_rows: list[dict]) -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    view_cur = MagicMock()
    view_cur.fetchall.return_value = view_rows
    routine_cur = MagicMock()
    routine_cur.fetchall.return_value = routine_rows
    conn.execute.side_effect = [view_cur, routine_cur]
    return conn


def test_extract_definitions_returns_view_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    view_rows = [
        {
            "schema_name": "public",
            "object_name": "active_users",
            "kind": "view",
            "definition": "SELECT id, name FROM users WHERE active = true",
        }
    ]
    conn = _definitions_conn(view_rows=view_rows, routine_rows=[])

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(pg.extract_definitions(adapter))

    assert len(snapshot.definitions) == 1
    defn = snapshot.definitions[0]
    assert defn.schema_name == "public"
    assert defn.object_name == "active_users"
    assert defn.definition_language == "sql"
    assert "SELECT" in defn.definition_text
    assert defn.object_kind == V2Kind.VIEW
    assert snapshot.meta.capability == AdapterCapability.DEFINITIONS


def test_extract_definitions_includes_routines(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    routine_rows = [
        {
            "schema_name": "public",
            "object_name": "get_active_count",
            "prokind": "f",
            "language": "plpgsql",
            "definition": "CREATE OR REPLACE FUNCTION public.get_active_count() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;",
        }
    ]
    conn = _definitions_conn(view_rows=[], routine_rows=routine_rows)

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(pg.extract_definitions(adapter))

    assert len(snapshot.definitions) == 1
    defn = snapshot.definitions[0]
    assert defn.object_name == "get_active_count"
    assert defn.definition_language == "plpgsql"
    assert defn.object_kind == V2Kind.UDF


def test_extract_definitions_skips_null_definitions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    view_rows = [
        {"schema_name": "public", "object_name": "empty_view", "kind": "view", "definition": None},
        {"schema_name": "public", "object_name": "real_view", "kind": "view", "definition": "SELECT 1"},
    ]
    conn = _definitions_conn(view_rows=view_rows, routine_rows=[])

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(pg.extract_definitions(adapter))

    assert len(snapshot.definitions) == 1
    assert snapshot.definitions[0].object_name == "real_view"


def test_extract_definitions_materialized_view_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    view_rows = [
        {
            "schema_name": "public",
            "object_name": "mv_sales",
            "kind": "materialized_view",
            "definition": "SELECT * FROM sales",
        }
    ]
    conn = _definitions_conn(view_rows=view_rows, routine_rows=[])

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(pg.extract_definitions(adapter))

    assert snapshot.definitions[0].object_kind == V2Kind.MATERIALIZED_VIEW


# ---------------------------------------------------------------------------
# extract_lineage / extract_orchestration raise NotImplementedError
# ---------------------------------------------------------------------------


def test_extract_lineage_raises() -> None:
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    with pytest.raises(NotImplementedError):
        asyncio.run(pg.extract_lineage(adapter))  # type: ignore[arg-type]


def test_extract_orchestration_raises() -> None:
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    with pytest.raises(NotImplementedError):
        asyncio.run(pg.extract_orchestration(adapter))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# v1 backward compatibility
# ---------------------------------------------------------------------------


def _introspect_conn(rows: list[dict], dep_rows: list[dict], stats_rows: list[dict]) -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    schema_cur = MagicMock()
    schema_cur.fetchall.return_value = rows
    dep_cur = MagicMock()
    dep_cur.fetchall.return_value = dep_rows
    stats_cur = MagicMock()
    stats_cur.fetchall.return_value = stats_rows
    conn.execute.side_effect = [schema_cur, dep_cur, stats_cur]
    return conn


def test_v1_introspect_schema_still_works(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_DSN", "postgresql://localhost/test")
    pg = _make_pg_adapter()
    adapter = _make_persisted()
    rows = [
        {
            "schema_name": "public",
            "table_name": "users",
            "column_name": "id",
            "data_type": "integer",
            "is_nullable": "NO",
            "table_type": "BASE TABLE",
        }
    ]
    conn = _introspect_conn(rows, [], [])

    with patch(_PSYCOPG_CONNECT, return_value=conn):
        snapshot = asyncio.run(pg.introspect_schema(adapter))

    assert len(snapshot.objects) == 1
    obj = snapshot.objects[0]
    assert obj.schema_name == "public"
    assert obj.object_name == "users"
    assert len(obj.columns) == 1
    assert obj.columns[0].name == "id"
