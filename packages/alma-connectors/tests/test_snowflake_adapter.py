"""Tests for SnowflakeAdapter."""

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
from alma_connectors.adapters.snowflake import (
    SnowflakeAdapter,
    _map_table_type,
    _parse_secret,
)
from alma_connectors.source_adapter import SchemaObjectKind

# ---------------------------------------------------------------------------
# Helpers
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


def _make_mock_cursor(
    *,
    rows: list[tuple[Any, ...]],
    col_names: list[str],
) -> MagicMock:
    cur = MagicMock()
    cur.description = [(name,) for name in col_names]
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    return cur


def _make_mock_conn(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# _parse_secret
# ---------------------------------------------------------------------------


def test_parse_secret_json() -> None:
    raw = '{"account": "xy12345", "user": "atlas", "password": "secret"}'
    result = _parse_secret(raw)
    assert result["account"] == "xy12345"
    assert result["user"] == "atlas"
    assert result["password"] == "secret"


def test_parse_secret_semicolon_kv() -> None:
    raw = "account=xy12345;user=atlas;password=secret"
    result = _parse_secret(raw)
    assert result["account"] == "xy12345"
    assert result["user"] == "atlas"
    assert result["password"] == "secret"


def test_parse_secret_invalid_json_raises() -> None:
    with pytest.raises(ValueError, match="JSON"):
        _parse_secret("{invalid json}")


def test_parse_secret_empty_string_raises() -> None:
    with pytest.raises(ValueError, match="must be"):
        _parse_secret("   ")


# ---------------------------------------------------------------------------
# _map_table_type
# ---------------------------------------------------------------------------


def test_map_table_type_view() -> None:
    assert _map_table_type("VIEW") == SchemaObjectKind.VIEW


def test_map_table_type_materialized_view() -> None:
    assert _map_table_type("MATERIALIZED VIEW") == SchemaObjectKind.MATERIALIZED_VIEW


def test_map_table_type_base_table() -> None:
    assert _map_table_type("BASE TABLE") == SchemaObjectKind.TABLE


def test_map_table_type_unknown() -> None:
    assert _map_table_type("EXTERNAL TABLE") == SchemaObjectKind.TABLE


# ---------------------------------------------------------------------------
# SnowflakeAdapterConfig validation
# ---------------------------------------------------------------------------


def test_config_defaults() -> None:
    config = SnowflakeAdapterConfig(
        account_secret=ExternalSecretRef(provider="env", reference="SF_SECRET"),
        account="xy12345.us-east-1",
    )
    assert config.warehouse == "COMPUTE_WH"
    assert config.database == ""
    assert config.role == ""
    assert config.lookback_hours == 168
    assert config.max_query_rows == 10_000
    assert config.exclude_schemas == ("INFORMATION_SCHEMA",)
    assert config.include_schemas == ()


def test_config_frozen() -> None:
    config = SnowflakeAdapterConfig(
        account_secret=ExternalSecretRef(provider="env", reference="SF_SECRET"),
        account="xy12345.us-east-1",
    )
    with pytest.raises(Exception):  # noqa: B017
        config.account = "something-else"  # type: ignore[misc]


def test_config_invalid_lookback_hours() -> None:
    with pytest.raises(ValueError, match="lookback_hours"):
        SnowflakeAdapterConfig(
            account_secret=ExternalSecretRef(provider="env", reference="SF_SECRET"),
            account="xy12345",
            lookback_hours=0,
        )


def test_config_invalid_max_query_rows() -> None:
    with pytest.raises(ValueError, match="max_query_rows"):
        SnowflakeAdapterConfig(
            account_secret=ExternalSecretRef(provider="env", reference="SF_SECRET"),
            account="xy12345",
            max_query_rows=0,
        )


def test_config_requires_non_empty_account() -> None:
    with pytest.raises(ValueError, match="account"):
        SnowflakeAdapterConfig(
            account_secret=ExternalSecretRef(provider="env", reference="SF_SECRET"),
            account="   ",
        )


# ---------------------------------------------------------------------------
# capabilities
# ---------------------------------------------------------------------------


def test_capabilities_all_true() -> None:
    adapter = _make_snowflake_adapter()
    caps = adapter.capabilities
    assert caps.can_test_connection is True
    assert caps.can_introspect_schema is True
    assert caps.can_observe_traffic is True
    assert caps.can_execute_query is True


def test_kind_is_snowflake() -> None:
    adapter = _make_snowflake_adapter()
    assert adapter.kind == SourceAdapterKind.SNOWFLAKE


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def test_test_connection_success() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345.us-east-1", "user": "atlas", "password": "pass"}'
    )

    mock_cursor = _make_mock_cursor(rows=[(5,)], col_names=["COUNT(*)"])
    mock_conn = _make_mock_conn(mock_cursor)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.test_connection(persisted))

    assert result.success is True
    assert "xy12345.us-east-1" in result.message
    assert result.resource_count == 5
    assert result.resource_label == "tables"


def test_test_connection_failure() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "bad"}'
    )
    mock_module = MagicMock()
    mock_module.connect.side_effect = Exception("Authentication failed")

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.test_connection(persisted))

    assert result.success is False
    assert "Authentication failed" in result.message


def test_test_connection_missing_module() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter()

    with patch(_SNOWFLAKE_MODULE, side_effect=RuntimeError("snowflake-connector-python is required")), pytest.raises(RuntimeError, match="snowflake-connector-python"):
        asyncio.run(sf_adapter.test_connection(persisted))


# ---------------------------------------------------------------------------
# introspect_schema
# ---------------------------------------------------------------------------


def _col_row(
    schema: str,
    table: str,
    table_type: str,
    col_name: str,
    data_type: str,
    is_nullable: str = "YES",
    ordinal: int = 1,
) -> tuple[Any, ...]:
    return ("MY_DB", schema, table, table_type, col_name, data_type, is_nullable, ordinal, None)


_COL_NAMES = [
    "TABLE_CATALOG",
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "TABLE_TYPE",
    "COLUMN_NAME",
    "DATA_TYPE",
    "IS_NULLABLE",
    "ORDINAL_POSITION",
    "COMMENT",
]
_RC_NAMES = ["TABLE_SCHEMA", "TABLE_NAME", "ROW_COUNT"]


def _make_dual_cursor(
    col_rows: list[tuple[Any, ...]],
    rc_rows: list[tuple[Any, ...]],
) -> MagicMock:
    """Create a cursor that returns col_rows on first fetchall, rc_rows on second."""
    cur = MagicMock()
    descriptions = [
        [(name,) for name in _COL_NAMES],
        [(name,) for name in _RC_NAMES],
    ]
    fetch_results = [col_rows, rc_rows]
    call_idx = [0]

    def on_execute(_sql: str) -> None:
        idx = call_idx[0]
        cur.description = descriptions[idx] if idx < len(descriptions) else descriptions[-1]
        cur.fetchall.return_value = fetch_results[idx] if idx < len(fetch_results) else []
        call_idx[0] += 1

    cur.execute.side_effect = on_execute
    return cur


def test_introspect_schema_basic() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    col_rows = [
        _col_row("PUBLIC", "ORDERS", "BASE TABLE", "ORDER_ID", "NUMBER", "NO"),
        _col_row("PUBLIC", "ORDERS", "BASE TABLE", "CUSTOMER_ID", "NUMBER"),
        _col_row("PUBLIC", "CUSTOMERS", "BASE TABLE", "ID", "NUMBER", "NO"),
    ]
    rc_rows = [("PUBLIC", "ORDERS", 100), ("PUBLIC", "CUSTOMERS", 50)]

    cur = _make_dual_cursor(col_rows, rc_rows)
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        snapshot = asyncio.run(sf_adapter.introspect_schema(persisted))

    table_names = {(o.schema_name, o.object_name) for o in snapshot.objects}
    assert ("PUBLIC", "ORDERS") in table_names
    assert ("PUBLIC", "CUSTOMERS") in table_names
    assert len(snapshot.objects) == 2

    orders = next(o for o in snapshot.objects if o.object_name == "ORDERS")
    assert orders.row_count == 100
    assert len(orders.columns) == 2
    assert orders.object_kind == SchemaObjectKind.TABLE


def test_introspect_schema_excludes_information_schema() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    col_rows = [
        _col_row("INFORMATION_SCHEMA", "TABLES", "VIEW", "TABLE_NAME", "TEXT"),
        _col_row("PUBLIC", "USERS", "BASE TABLE", "ID", "NUMBER", "NO"),
    ]
    cur = _make_dual_cursor(col_rows, [])
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        snapshot = asyncio.run(sf_adapter.introspect_schema(persisted))

    names = [o.object_name for o in snapshot.objects]
    assert "TABLES" not in names
    assert "USERS" in names


def test_introspect_schema_include_filter() -> None:
    persisted = _make_adapter(include_schemas=("ANALYTICS",))
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    col_rows = [
        _col_row("PUBLIC", "ORDERS", "BASE TABLE", "ID", "NUMBER"),
        _col_row("ANALYTICS", "REVENUE", "BASE TABLE", "AMOUNT", "FLOAT"),
    ]
    cur = _make_dual_cursor(col_rows, [])
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        snapshot = asyncio.run(sf_adapter.introspect_schema(persisted))

    names = [o.object_name for o in snapshot.objects]
    assert "REVENUE" in names
    assert "ORDERS" not in names


def test_introspect_schema_view_kind() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    col_rows = [_col_row("PUBLIC", "V_ORDERS", "VIEW", "ID", "NUMBER")]
    cur = _make_dual_cursor(col_rows, [])
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        snapshot = asyncio.run(sf_adapter.introspect_schema(persisted))

    assert snapshot.objects[0].object_kind == SchemaObjectKind.VIEW


# ---------------------------------------------------------------------------
# observe_traffic
# ---------------------------------------------------------------------------

_TRAFFIC_NAMES = [
    "QUERY_ID",
    "QUERY_TEXT",
    "USER_NAME",
    "DATABASE_NAME",
    "SCHEMA_NAME",
    "WAREHOUSE_NAME",
    "EXECUTION_STATUS",
    "START_TIME",
    "END_TIME",
    "TOTAL_ELAPSED_TIME",
]


def _traffic_row(
    query_id: str = "q1",
    sql: str = "SELECT 1",
    user: str = "ATLAS",
    database: str = "MY_DB",
    schema: str = "PUBLIC",
    warehouse: str = "COMPUTE_WH",
    status: str = "SUCCESS",
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    elapsed_ms: float = 123.4,
) -> tuple[Any, ...]:
    start = start_time or datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = end_time or datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC)
    return (query_id, sql, user, database, schema, warehouse, status, start, end, elapsed_ms)


def test_observe_traffic_basic() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    traffic_rows = [_traffic_row("q1", "SELECT * FROM orders"), _traffic_row("q2", "SELECT * FROM customers")]
    cur = MagicMock()
    cur.description = [(name,) for name in _TRAFFIC_NAMES]
    cur.fetchall.return_value = traffic_rows
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.observe_traffic(persisted))

    assert result.scanned_records == 2
    assert len(result.events) == 2
    assert result.events[0].event_id == "q1"
    assert result.events[0].database_user == "ATLAS"
    assert result.events[0].database_name == "MY_DB"
    assert result.events[0].duration_ms == pytest.approx(123.4)
    assert result.errors == ()


def test_observe_traffic_sql_in_query() -> None:
    persisted = _make_adapter(lookback_hours=24)
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    cur = MagicMock()
    cur.description = [(name,) for name in _TRAFFIC_NAMES]
    cur.fetchall.return_value = []
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        asyncio.run(sf_adapter.observe_traffic(persisted))

    issued_sql = cur.execute.call_args[0][0]
    assert "DATEADD(hour, -24," in issued_sql
    assert "QUERY_HISTORY" in issued_sql


def test_observe_traffic_skips_empty_sql() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    traffic_rows = [_traffic_row("q1", "   ")]
    cur = MagicMock()
    cur.description = [(name,) for name in _TRAFFIC_NAMES]
    cur.fetchall.return_value = traffic_rows
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.observe_traffic(persisted))

    assert result.scanned_records == 1
    assert len(result.events) == 0


# ---------------------------------------------------------------------------
# execute_query
# ---------------------------------------------------------------------------


def test_execute_query_success() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    cur = MagicMock()
    cur.description = [("ID",), ("NAME",)]
    cur.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.execute_query(persisted, "SELECT id, name FROM users"))

    assert result.success is True
    assert result.row_count == 2
    assert result.rows[0] == {"ID": 1, "NAME": "Alice"}
    assert result.truncated is False


def test_execute_query_truncation() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    cur = MagicMock()
    cur.description = [("N",)]
    cur.fetchall.return_value = [(i,) for i in range(10)]
    mock_conn = _make_mock_conn(cur)
    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.execute_query(persisted, "SELECT n FROM t", max_rows=3))

    assert result.success is True
    assert result.row_count == 3
    assert result.truncated is True


def test_execute_query_failure() -> None:
    persisted = _make_adapter()
    sf_adapter = _make_snowflake_adapter(
        secret_value='{"account": "xy12345", "user": "atlas", "password": "pass"}'
    )

    mock_module = MagicMock()
    mock_module.connect.side_effect = Exception("Query execution error")

    with patch(_SNOWFLAKE_MODULE, return_value=mock_module):
        result = asyncio.run(sf_adapter.execute_query(persisted, "SELECT 1"))

    assert result.success is False
    assert "Query execution error" in (result.error_message or "")
    assert result.row_count == 0


# ---------------------------------------------------------------------------
# get_setup_instructions
# ---------------------------------------------------------------------------


def test_get_setup_instructions_returns_valid_object() -> None:
    adapter = _make_snowflake_adapter()
    instructions = adapter.get_setup_instructions()

    assert instructions.title
    assert "Snowflake" in instructions.title
    assert instructions.summary
    assert len(instructions.steps) >= 4
    # All steps must be non-empty strings
    for step in instructions.steps:
        assert step.strip()


def test_get_setup_instructions_mentions_account_usage() -> None:
    adapter = _make_snowflake_adapter()
    instructions = adapter.get_setup_instructions()
    all_text = " ".join(instructions.steps)
    assert "ACCOUNT_USAGE" in all_text or "account_usage" in all_text.lower()


# ---------------------------------------------------------------------------
# Wrong config type guard
# ---------------------------------------------------------------------------


def test_get_config_wrong_type_raises() -> None:
    from alma_connectors import BigQueryAdapterConfig

    persisted = PersistedSourceAdapter(
        id=_ADAPTER_ID,
        key="bq-prod",
        display_name="BQ Prod",
        kind=SourceAdapterKind.BIGQUERY,
        target_id="bq-prod",
        status=SourceAdapterStatus.READY,
        config=BigQueryAdapterConfig(
            service_account_secret=ExternalSecretRef(provider="env", reference="BQ_SA"),
            project_id="my-project",
        ),
    )
    sf_adapter = _make_snowflake_adapter()

    with pytest.raises(ValueError, match="not configured as snowflake"):
        asyncio.run(sf_adapter.test_connection(persisted))
