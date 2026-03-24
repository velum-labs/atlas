"""Tests for SnowflakeAdapter v2 capabilities."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from alma_connectors import (
    ExternalSecretRef,
    PersistedSourceAdapter,
    SnowflakeAdapterConfig,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.adapters.snowflake import SnowflakeAdapter
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    LineageEdgeKind,
    SchemaObjectKind as V2SchemaObjectKind,
)

# ---------------------------------------------------------------------------
# Helpers shared with v1 tests
# ---------------------------------------------------------------------------

_ADAPTER_ID = "12345678-1234-5678-1234-567812345678"
_SNOWFLAKE_MODULE = "alma_connectors.adapters.snowflake._get_snowflake_module"


def _make_adapter(
    *,
    account: str = "xy12345.us-east-1",
    warehouse: str = "COMPUTE_WH",
    database: str = "MY_DB",
    role: str = "",
    include_schemas: tuple[str, ...] = (),
    exclude_schemas: tuple[str, ...] = ("INFORMATION_SCHEMA",),
    lookback_hours: int = 168,
    max_query_rows: int = 10_000,
) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=_ADAPTER_ID,
        key="snowflake-prod",
        display_name="Snowflake Prod",
        kind=SourceAdapterKind.SNOWFLAKE,
        target_id="snowflake-prod",
        status=SourceAdapterStatus.READY,
        config=SnowflakeAdapterConfig(
            account_secret=ExternalSecretRef(provider="env", reference="SNOWFLAKE_CONNECTION_JSON"),
            account=account,
            warehouse=warehouse,
            database=database,
            role=role,
            include_schemas=include_schemas,
            exclude_schemas=exclude_schemas,
            lookback_hours=lookback_hours,
            max_query_rows=max_query_rows,
        ),
    )


def _make_snowflake_adapter(secret_value: str = "") -> SnowflakeAdapter:
    return SnowflakeAdapter(resolve_secret=lambda _secret: secret_value)


_DEFAULT_SECRET = '{"account": "xy12345.us-east-1", "user": "atlas", "password": "pass"}'


def _make_probe_cursor(failing_sql_patterns: set[str] | None = None) -> MagicMock:
    """Cursor that raises for any SQL containing a pattern in failing_sql_patterns."""
    failing = {p.upper() for p in (failing_sql_patterns or set())}
    cur = MagicMock()
    cur.description = [("RESULT",)]
    cur.fetchall.return_value = [(1,)]
    cur.fetchone.return_value = (1,)

    def on_execute(sql: str) -> None:
        sql_upper = sql.upper()
        for pattern in failing:
            if pattern in sql_upper:
                raise Exception(f"SQL compilation error: Object '{pattern}' does not exist or not authorized.")

    cur.execute.side_effect = on_execute
    return cur


def _make_dispatch_cursor(
    dispatch: dict[str, tuple[list[str], list[tuple[Any, ...]]]]
) -> MagicMock:
    """Cursor whose response is dispatched by matching SQL substrings (upper-case keys)."""
    cur = MagicMock()
    cur.description = [("RESULT",)]
    cur.fetchall.return_value = []

    def on_execute(sql: str) -> None:
        sql_upper = sql.upper().strip()
        for key, (cols, rows) in dispatch.items():
            if key.upper() in sql_upper:
                cur.description = [(c,) for c in cols]
                cur.fetchall.return_value = rows
                return
        cur.description = [("RESULT",)]
        cur.fetchall.return_value = []

    cur.execute.side_effect = on_execute
    return cur


def _mock_conn(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _mock_module(cursor: MagicMock) -> MagicMock:
    mock_module = MagicMock()
    mock_module.connect.return_value = _mock_conn(cursor)
    return mock_module


# ---------------------------------------------------------------------------
# declared_capabilities
# ---------------------------------------------------------------------------


def test_declared_capabilities_contains_five_caps() -> None:
    adapter = _make_snowflake_adapter()
    caps = adapter.declared_capabilities
    assert AdapterCapability.DISCOVER in caps
    assert AdapterCapability.SCHEMA in caps
    assert AdapterCapability.DEFINITIONS in caps
    assert AdapterCapability.TRAFFIC in caps
    assert AdapterCapability.LINEAGE in caps
    assert AdapterCapability.ORCHESTRATION not in caps


# ---------------------------------------------------------------------------
# probe — ACCOUNT_USAGE availability (TRAFFIC)
# ---------------------------------------------------------------------------


def test_probe_traffic_available_when_query_history_accessible() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_probe_cursor()  # nothing fails
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        results = asyncio.run(
            sf_adapter.probe(persisted, frozenset({AdapterCapability.TRAFFIC}))
        )

    assert len(results) == 1
    result = results[0]
    assert result.capability == AdapterCapability.TRAFFIC
    assert result.available is True


def test_probe_traffic_unavailable_when_account_usage_missing() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_probe_cursor({"QUERY_HISTORY"})
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        results = asyncio.run(
            sf_adapter.probe(persisted, frozenset({AdapterCapability.TRAFFIC}))
        )

    assert len(results) == 1
    result = results[0]
    assert result.capability == AdapterCapability.TRAFFIC
    assert result.available is False
    assert "IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE" in result.permissions_missing


# ---------------------------------------------------------------------------
# probe — ACCESS_HISTORY (Enterprise edition)
# ---------------------------------------------------------------------------


def test_probe_lineage_available_when_access_history_accessible() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_probe_cursor()  # nothing fails
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        results = asyncio.run(
            sf_adapter.probe(persisted, frozenset({AdapterCapability.LINEAGE}))
        )

    assert len(results) == 1
    result = results[0]
    assert result.capability == AdapterCapability.LINEAGE
    assert result.available is True
    assert result.message is not None
    assert "Enterprise" in result.message


def test_probe_lineage_unavailable_when_access_history_missing() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_probe_cursor({"ACCESS_HISTORY"})
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        results = asyncio.run(
            sf_adapter.probe(persisted, frozenset({AdapterCapability.LINEAGE}))
        )

    assert len(results) == 1
    result = results[0]
    assert result.capability == AdapterCapability.LINEAGE
    assert result.available is False
    assert "IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE" in result.permissions_missing


def test_probe_all_capabilities_returns_five_results() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_probe_cursor()  # nothing fails
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        results = asyncio.run(sf_adapter.probe(persisted))

    capability_set = {r.capability for r in results}
    assert AdapterCapability.DISCOVER in capability_set
    assert AdapterCapability.SCHEMA in capability_set
    assert AdapterCapability.DEFINITIONS in capability_set
    assert AdapterCapability.TRAFFIC in capability_set
    assert AdapterCapability.LINEAGE in capability_set
    assert all(r.available for r in results)


# ---------------------------------------------------------------------------
# discover — databases + schemas
# ---------------------------------------------------------------------------

_DB_COLS = ["created_on", "name", "is_default", "is_current", "origin", "owner", "comment",
            "options", "retention_time", "kind", "budget"]
_SCHEMA_COLS = ["created_on", "name", "is_default", "is_current", "database_name", "owner",
                "comment", "options", "retention_time", "kind", "budget"]


def _db_row(name: str, owner: str = "ATLAS") -> tuple[Any, ...]:
    return ("2024-01-01", name, "N", "Y", "", owner, "", "", "1", "STANDARD", None)


def _schema_row(name: str, db: str, owner: str = "ATLAS") -> tuple[Any, ...]:
    return ("2024-01-01", name, "N", "Y", db, owner, "", "", "1", "STANDARD", None)


def test_discover_returns_databases_and_schemas() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_dispatch_cursor({
        "SHOW DATABASES": (
            _DB_COLS,
            [_db_row("MY_DB"), _db_row("ANALYTICS_DB")],
        ),
        "SHOW SCHEMAS": (
            _SCHEMA_COLS,
            [
                _schema_row("PUBLIC", "MY_DB"),
                _schema_row("ANALYTICS", "MY_DB"),
                _schema_row("INFORMATION_SCHEMA", "MY_DB"),  # should be filtered
            ],
        ),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.discover(persisted))

    assert snapshot.meta.capability == AdapterCapability.DISCOVER
    assert snapshot.meta.adapter_kind.value == "snowflake"

    container_ids = {c.container_id for c in snapshot.containers}
    container_types = {c.container_id: c.container_type for c in snapshot.containers}

    # Databases present
    assert "MY_DB" in container_ids
    assert "ANALYTICS_DB" in container_ids
    assert container_types["MY_DB"] == "database"

    # User schemas present, INFORMATION_SCHEMA filtered
    assert "MY_DB.PUBLIC" in container_ids
    assert "MY_DB.ANALYTICS" in container_ids
    assert not any(c.container_id.endswith(".INFORMATION_SCHEMA") for c in snapshot.containers)

    # Schema container_type
    assert container_types["MY_DB.PUBLIC"] == "schema"


def test_discover_schema_container_has_database_name_metadata() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_dispatch_cursor({
        "SHOW DATABASES": (_DB_COLS, [_db_row("MY_DB")]),
        "SHOW SCHEMAS": (_SCHEMA_COLS, [_schema_row("PUBLIC", "MY_DB")]),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.discover(persisted))

    schema_container = next(c for c in snapshot.containers if c.container_type == "schema")
    assert schema_container.metadata.get("database_name") == "MY_DB"


# ---------------------------------------------------------------------------
# extract_schema — v2 with UDFs, procedures, freshness
# ---------------------------------------------------------------------------

_COLS_COLS = ["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "ORDINAL_POSITION"]
_TABLES_COLS = ["TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ROW_COUNT", "BYTES", "LAST_ALTERED"]
_FUNCS_COLS = ["FUNCTION_SCHEMA", "FUNCTION_NAME", "DATA_TYPE", "ARGUMENT_SIGNATURE",
               "FUNCTION_LANGUAGE", "FUNCTION_DEFINITION", "LAST_ALTERED"]
_PROCS_COLS = ["PROCEDURE_SCHEMA", "PROCEDURE_NAME", "DATA_TYPE", "ARGUMENT_SIGNATURE",
               "PROCEDURE_LANGUAGE", "PROCEDURE_DEFINITION", "LAST_ALTERED"]

_TS = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def test_extract_schema_includes_tables_views_udfs_procedures() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_dispatch_cursor({
        "INFORMATION_SCHEMA.COLUMNS": (
            _COLS_COLS,
            [
                ("PUBLIC", "ORDERS", "ID", "NUMBER", "NO", 1),
                ("PUBLIC", "V_ACTIVE", "ID", "NUMBER", "YES", 1),
            ],
        ),
        "INFORMATION_SCHEMA.TABLES": (
            _TABLES_COLS,
            [
                ("PUBLIC", "ORDERS", "BASE TABLE", 500, 1024, _TS),
                ("PUBLIC", "V_ACTIVE", "VIEW", 0, 0, _TS),
            ],
        ),
        "INFORMATION_SCHEMA.FUNCTIONS": (
            _FUNCS_COLS,
            [("PUBLIC", "MY_FUNC", "NUMBER", "(x NUMBER)", "SQL", "RETURN x * 2", _TS)],
        ),
        "INFORMATION_SCHEMA.PROCEDURES": (
            _PROCS_COLS,
            [("PUBLIC", "MY_PROC", "VARCHAR", "()", "SQL", "BEGIN RETURN 'ok'; END", _TS)],
        ),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_schema(persisted))

    assert snapshot.meta.capability == AdapterCapability.SCHEMA
    kinds = {o.object_name: o.kind for o in snapshot.objects}
    assert kinds["ORDERS"] == V2SchemaObjectKind.TABLE
    assert kinds["V_ACTIVE"] == V2SchemaObjectKind.VIEW
    assert kinds["MY_FUNC"] == V2SchemaObjectKind.UDF
    assert kinds["MY_PROC"] == V2SchemaObjectKind.PROCEDURE


def test_extract_schema_freshness_fields_populated() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_dispatch_cursor({
        "INFORMATION_SCHEMA.COLUMNS": (
            _COLS_COLS,
            [("PUBLIC", "ORDERS", "ID", "NUMBER", "NO", 1)],
        ),
        "INFORMATION_SCHEMA.TABLES": (
            _TABLES_COLS,
            [("PUBLIC", "ORDERS", "BASE TABLE", 1000, 2048, _TS)],
        ),
        "INFORMATION_SCHEMA.FUNCTIONS": (_FUNCS_COLS, []),
        "INFORMATION_SCHEMA.PROCEDURES": (_PROCS_COLS, []),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_schema(persisted))

    orders = next(o for o in snapshot.objects if o.object_name == "ORDERS")
    assert orders.row_count == 1000
    assert orders.size_bytes == 2048
    assert orders.last_modified == _TS


def test_extract_schema_udf_has_language_and_body() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_dispatch_cursor({
        "INFORMATION_SCHEMA.COLUMNS": (_COLS_COLS, []),
        "INFORMATION_SCHEMA.TABLES": (_TABLES_COLS, []),
        "INFORMATION_SCHEMA.FUNCTIONS": (
            _FUNCS_COLS,
            [("PUBLIC", "DOUBLE", "NUMBER", "(x NUMBER)", "SQL", "RETURN x * 2", _TS)],
        ),
        "INFORMATION_SCHEMA.PROCEDURES": (_PROCS_COLS, []),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_schema(persisted))

    udf = next(o for o in snapshot.objects if o.kind == V2SchemaObjectKind.UDF)
    assert udf.language == "SQL"
    assert udf.return_type == "NUMBER"
    assert udf.definition_body == "RETURN x * 2"


# ---------------------------------------------------------------------------
# extract_definitions — view DDL
# ---------------------------------------------------------------------------

_VIEW_COLS = ["TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION"]
_DEF_FUNC_COLS = ["FUNCTION_SCHEMA", "FUNCTION_NAME", "ARGUMENT_SIGNATURE",
                  "FUNCTION_LANGUAGE", "FUNCTION_DEFINITION"]


def test_extract_definitions_returns_view_ddl() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    view_sql = "SELECT * FROM PUBLIC.ORDERS WHERE STATUS = 'ACTIVE'"
    cur = _make_dispatch_cursor({
        "INFORMATION_SCHEMA.VIEWS": (
            _VIEW_COLS,
            [("PUBLIC", "V_ACTIVE_ORDERS", view_sql)],
        ),
        "INFORMATION_SCHEMA.FUNCTIONS": (_DEF_FUNC_COLS, []),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_definitions(persisted))

    assert snapshot.meta.capability == AdapterCapability.DEFINITIONS
    assert len(snapshot.definitions) == 1
    defn = snapshot.definitions[0]
    assert defn.schema_name == "PUBLIC"
    assert defn.object_name == "V_ACTIVE_ORDERS"
    assert defn.object_kind == V2SchemaObjectKind.VIEW
    assert defn.definition_text == view_sql
    assert defn.definition_language == "sql"


def test_extract_definitions_includes_function_body() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_dispatch_cursor({
        "INFORMATION_SCHEMA.VIEWS": (_VIEW_COLS, []),
        "INFORMATION_SCHEMA.FUNCTIONS": (
            _DEF_FUNC_COLS,
            [("PUBLIC", "DOUBLE_IT", "(x NUMBER)", "SQL", "RETURN x * 2")],
        ),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_definitions(persisted))

    assert len(snapshot.definitions) == 1
    defn = snapshot.definitions[0]
    assert defn.object_kind == V2SchemaObjectKind.UDF
    assert defn.definition_text == "RETURN x * 2"
    assert defn.definition_language == "sql"


def test_extract_definitions_skips_empty_view_definition() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = _make_dispatch_cursor({
        "INFORMATION_SCHEMA.VIEWS": (
            _VIEW_COLS,
            [
                ("PUBLIC", "V_EMPTY", ""),           # empty — skip
                ("PUBLIC", "V_REAL", "SELECT 1"),    # non-empty — keep
            ],
        ),
        "INFORMATION_SCHEMA.FUNCTIONS": (_DEF_FUNC_COLS, []),
    })
    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_definitions(persisted))

    assert len(snapshot.definitions) == 1
    assert snapshot.definitions[0].object_name == "V_REAL"


# ---------------------------------------------------------------------------
# extract_traffic — wraps observe_traffic
# ---------------------------------------------------------------------------

_TRAFFIC_COLS = [
    "QUERY_ID", "QUERY_TEXT", "USER_NAME", "DATABASE_NAME", "SCHEMA_NAME",
    "WAREHOUSE_NAME", "EXECUTION_STATUS", "START_TIME", "END_TIME", "TOTAL_ELAPSED_TIME",
]


def _traffic_row(qid: str = "q1", sql: str = "SELECT 1") -> tuple[Any, ...]:
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC)
    return (qid, sql, "ATLAS", "MY_DB", "PUBLIC", "COMPUTE_WH", "SUCCESS", start, end, 100.0)


def test_extract_traffic_wraps_v1_result() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    cur = MagicMock()
    cur.description = [(c,) for c in _TRAFFIC_COLS]
    cur.fetchall.return_value = [_traffic_row("q1", "SELECT * FROM orders")]

    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        result = asyncio.run(sf_adapter.extract_traffic(persisted))

    assert result.meta.capability == AdapterCapability.TRAFFIC
    assert result.meta.adapter_kind.value == "snowflake"
    assert len(result.events) == 1
    assert result.events[0].event_id == "q1"
    assert result.meta.row_count == 1


# ---------------------------------------------------------------------------
# extract_lineage — with Enterprise (ACCESS_HISTORY available)
# ---------------------------------------------------------------------------

_LINEAGE_COLS = [
    "QUERY_ID", "QUERY_START_TIME",
    "DIRECT_OBJECTS_ACCESSED", "BASE_OBJECTS_ACCESSED", "OBJECTS_MODIFIED",
]


def _lineage_row(
    query_id: str,
    base_objects: list[dict[str, Any]],
    modified_objects: list[dict[str, Any]],
) -> tuple[Any, ...]:
    return (
        query_id,
        datetime(2024, 1, 1, tzinfo=UTC),
        [],  # DIRECT_OBJECTS_ACCESSED
        base_objects,
        modified_objects,
    )


def test_extract_lineage_returns_edges_with_enterprise() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    rows = [
        _lineage_row(
            "q1",
            base_objects=[{
                "objectName": "MY_DB.PUBLIC.ORDERS",
                "columns": [{"columnName": "ID"}, {"columnName": "AMOUNT"}],
            }],
            modified_objects=[{
                "objectName": "MY_DB.ANALYTICS.ORDER_SUMMARY",
                "columns": [{"columnName": "ORDER_ID"}, {"columnName": "TOTAL"}],
            }],
        ),
    ]
    cur = MagicMock()
    cur.description = [(c,) for c in _LINEAGE_COLS]
    cur.fetchall.return_value = rows

    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_lineage(persisted))

    assert snapshot.meta.capability == AdapterCapability.LINEAGE
    assert len(snapshot.edges) == 1
    edge = snapshot.edges[0]
    assert edge.source_object == "MY_DB.PUBLIC.ORDERS"
    assert edge.target_object == "MY_DB.ANALYTICS.ORDER_SUMMARY"
    assert edge.edge_kind == LineageEdgeKind.DECLARED
    assert edge.confidence == 1.0
    assert ("ID", "ORDER_ID") in edge.column_mappings
    assert ("AMOUNT", "TOTAL") in edge.column_mappings


def test_extract_lineage_deduplicates_edges() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    # Same source→target pair appears in two rows
    base = [{"objectName": "MY_DB.PUBLIC.ORDERS", "columns": []}]
    modified = [{"objectName": "MY_DB.ANALYTICS.SUMMARY", "columns": []}]
    rows = [_lineage_row("q1", base, modified), _lineage_row("q2", base, modified)]

    cur = MagicMock()
    cur.description = [(c,) for c in _LINEAGE_COLS]
    cur.fetchall.return_value = rows

    with patch(_SNOWFLAKE_MODULE, return_value=_mock_module(cur)):
        snapshot = asyncio.run(sf_adapter.extract_lineage(persisted))

    assert len(snapshot.edges) == 1


# ---------------------------------------------------------------------------
# extract_lineage — without Enterprise (ACCESS_HISTORY unavailable)
# ---------------------------------------------------------------------------


def test_extract_lineage_returns_empty_when_access_history_unavailable() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    mock_module = MagicMock()
    mock_module.connect.return_value.cursor.return_value.execute.side_effect = Exception(
        "Object 'ACCESS_HISTORY' does not exist."
    )
    mock_module.connect.return_value.__enter__ = lambda s: s
    mock_module.connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        snapshot = asyncio.run(sf_adapter.extract_lineage(persisted))

    assert snapshot.meta.capability == AdapterCapability.LINEAGE
    assert snapshot.edges == ()
    assert snapshot.meta.row_count == 0


def test_extract_lineage_without_enterprise_via_connect_exception() -> None:
    """Verify graceful fallback when the entire connection fails for ACCESS_HISTORY."""
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    mock_module = MagicMock()
    mock_module.connect.side_effect = Exception("Insufficient privileges to access ACCESS_HISTORY")

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        snapshot = asyncio.run(sf_adapter.extract_lineage(persisted))

    assert snapshot.edges == ()
    assert snapshot.meta.row_count == 0


# ---------------------------------------------------------------------------
# extract_orchestration — not implemented
# ---------------------------------------------------------------------------


def test_extract_orchestration_raises_not_implemented() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    with pytest.raises(NotImplementedError, match="ORCHESTRATION"):
        asyncio.run(sf_adapter.extract_orchestration(persisted))


# ---------------------------------------------------------------------------
# v1 methods still work
# ---------------------------------------------------------------------------


def test_v1_introspect_schema_still_works() -> None:
    """Ensure existing introspect_schema() is unaffected by v2 additions."""
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    _COL_NAMES = [
        "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE",
        "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "ORDINAL_POSITION", "COMMENT",
    ]
    _RC_NAMES = ["TABLE_SCHEMA", "TABLE_NAME", "ROW_COUNT"]

    col_rows = [("MY_DB", "PUBLIC", "ORDERS", "BASE TABLE", "ID", "NUMBER", "NO", 1, None)]
    rc_rows = [("PUBLIC", "ORDERS", 42)]

    call_idx: list[int] = [0]

    cur = MagicMock()
    descriptions = [
        [(n,) for n in _COL_NAMES],
        [(n,) for n in _RC_NAMES],
    ]
    fetches = [col_rows, rc_rows]

    def on_execute(_sql: str) -> None:
        idx = call_idx[0]
        cur.description = descriptions[idx] if idx < len(descriptions) else descriptions[-1]
        cur.fetchall.return_value = fetches[idx] if idx < len(fetches) else []
        call_idx[0] += 1

    cur.execute.side_effect = on_execute
    mock_module = MagicMock()
    mock_module.connect.return_value = _mock_conn(cur)

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        snapshot = asyncio.run(sf_adapter.introspect_schema(persisted))

    assert len(snapshot.objects) == 1
    orders = snapshot.objects[0]
    assert orders.schema_name == "PUBLIC"
    assert orders.object_name == "ORDERS"
    assert orders.row_count == 42


def test_v1_observe_traffic_still_works() -> None:
    """Ensure existing observe_traffic() is unaffected by v2 additions."""
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(_DEFAULT_SECRET)

    _TRAFFIC_NAMES = [
        "QUERY_ID", "QUERY_TEXT", "USER_NAME", "DATABASE_NAME", "SCHEMA_NAME",
        "WAREHOUSE_NAME", "EXECUTION_STATUS", "START_TIME", "END_TIME", "TOTAL_ELAPSED_TIME",
    ]
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC)
    rows = [("q1", "SELECT 1", "ATLAS", "MY_DB", "PUBLIC", "COMPUTE_WH", "SUCCESS", start, end, 50.0)]

    cur = MagicMock()
    cur.description = [(n,) for n in _TRAFFIC_NAMES]
    cur.fetchall.return_value = rows
    mock_module = MagicMock()
    mock_module.connect.return_value = _mock_conn(cur)

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.observe_traffic(persisted))

    assert result.scanned_records == 1
    assert len(result.events) == 1
    assert result.events[0].event_id == "q1"
