"""Snowflake source adapter implementation."""

from __future__ import annotations

import json
import logging
import time
import warnings
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from alma_connectors.source_adapter import (
    ConnectionTestResult,
    ExternalSecretRef,
    ManagedSecret,
    ObservedQueryEvent,
    PersistedSourceAdapter,
    QueryResult,
    SchemaObjectKind,
    SchemaSnapshot,
    SetupInstructions,
    SnowflakeAdapterConfig,
    SourceAdapterCapabilities,
    SourceAdapterKind,
    SourceColumnSchema,
    SourceTableSchema,
    TrafficObservationResult,
)

logger = logging.getLogger(__name__)

_SNOWFLAKE_SYSTEM_SCHEMAS = frozenset({"INFORMATION_SCHEMA"})


def _get_snowflake_module() -> Any:
    try:
        import snowflake.connector  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "snowflake-connector-python is required for the Snowflake source adapter. "
            "Install it with: pip install snowflake-connector-python"
        ) from exc
    return snowflake.connector


def _parse_secret(raw: str) -> dict[str, str]:
    """Parse a secret value that may be a JSON blob or a plain DSN string.

    Accepts either:
    - A JSON object with keys: account, user, password, warehouse, database, role
    - A Snowflake connection string: account=...;user=...;password=...
    """
    raw = raw.strip()
    if raw.startswith("{"):
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Snowflake secret looks like JSON but failed to parse: {exc}") from exc
    # Try semicolon-delimited key=value pairs
    result: dict[str, str] = {}
    for part in raw.split(";"):
        part = part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            result[k.strip().lower()] = v.strip()
    if result:
        return result
    raise ValueError(
        "Snowflake secret must be a JSON object or semicolon-delimited key=value string"
    )


def _map_table_type(raw: str) -> SchemaObjectKind:
    upper = raw.upper()
    if upper == "VIEW":
        return SchemaObjectKind.VIEW
    if upper == "MATERIALIZED VIEW":
        return SchemaObjectKind.MATERIALIZED_VIEW
    return SchemaObjectKind.TABLE


class SnowflakeAdapter:
    """Runtime Snowflake source adapter."""

    kind = SourceAdapterKind.SNOWFLAKE
    capabilities = SourceAdapterCapabilities(
        can_test_connection=True,
        can_introspect_schema=True,
        can_observe_traffic=True,
        can_execute_query=True,
    )

    def __init__(self, *, resolve_secret: Callable[[ManagedSecret | ExternalSecretRef], str]) -> None:
        self._resolve_secret = resolve_secret

    def _get_config(self, adapter: PersistedSourceAdapter) -> SnowflakeAdapterConfig:
        if not isinstance(adapter.config, SnowflakeAdapterConfig):
            raise ValueError(f"adapter '{adapter.key}' is not configured as snowflake")
        return adapter.config

    def _connect(self, config: SnowflakeAdapterConfig) -> Any:
        """Open a Snowflake connection using config + secret credentials."""
        connector = _get_snowflake_module()
        raw_secret = self._resolve_secret(config.account_secret)
        creds = _parse_secret(raw_secret) if raw_secret.strip() else {}

        connect_kwargs: dict[str, Any] = {
            "account": creds.get("account", config.account),
            "user": creds.get("user", creds.get("username", "")),
            "password": creds.get("password", ""),
        }
        warehouse = creds.get("warehouse", config.warehouse)
        if warehouse:
            connect_kwargs["warehouse"] = warehouse
        database = creds.get("database", config.database)
        if database:
            connect_kwargs["database"] = database
        role = creds.get("role", config.role)
        if role:
            connect_kwargs["role"] = role

        return connector.connect(**connect_kwargs)

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate credentials and connectivity for the Snowflake adapter.

        .. deprecated:: 0.2.0
            Use :meth:`~alma_connectors.source_adapter_v2.SourceAdapterV2.probe` instead.
        """
        warnings.warn(
            "SnowflakeAdapter.test_connection() is deprecated since 0.2.0 and will be removed in 1.0.0. "
            "Use probe() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        config = self._get_config(adapter)
        try:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'")
                row = cur.fetchone()
                table_count = int(row[0]) if row else 0
                cur.close()
            finally:
                conn.close()
        except RuntimeError:
            raise
        except Exception as exc:
            return ConnectionTestResult(
                success=False,
                message=f"Connection failed: {exc}",
            )
        return ConnectionTestResult(
            success=True,
            message=f"Connected to Snowflake account '{config.account}' successfully.",
            resource_count=table_count,
            resource_label="tables",
        )

    async def introspect_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshot:
        """Return a typed snapshot of source objects and dependencies.

        .. deprecated:: 0.2.0
            Use :meth:`~alma_connectors.source_adapter_v2.SourceAdapterV2.extract_schema` instead.
        """
        warnings.warn(
            "SnowflakeAdapter.introspect_schema() is deprecated since 0.2.0 and will be removed in 1.0.0. "
            "Use extract_schema() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        config = self._get_config(adapter)
        database = config.database
        db_prefix = f"{database}." if database else ""

        columns_sql = f"""\
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION,
    COMMENT
FROM {db_prefix}INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
"""

        row_count_sql = f"""\
SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT
FROM {db_prefix}INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
"""

        # Determine excluded schemas (upper-case for Snowflake identifiers)
        exclude_upper = frozenset(s.upper() for s in config.exclude_schemas)
        include_upper = frozenset(s.upper() for s in config.include_schemas) if config.include_schemas else None

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(columns_sql)
            col_rows = cur.fetchall()
            col_names = [desc[0].upper() for desc in cur.description]

            # Fetch row counts (best-effort)
            row_counts: dict[tuple[str, str], int | None] = {}
            try:
                cur.execute(row_count_sql)
                for rc_row in cur.fetchall():
                    rc_desc = [d[0].upper() for d in cur.description]
                    rc = dict(zip(rc_desc, rc_row))
                    schema = str(rc.get("TABLE_SCHEMA", ""))
                    name = str(rc.get("TABLE_NAME", ""))
                    count = rc.get("ROW_COUNT")
                    row_counts[(schema, name)] = int(count) if count is not None else None
            except Exception:
                logger.debug("Could not fetch row counts from INFORMATION_SCHEMA.TABLES", exc_info=True)

            cur.close()
        finally:
            conn.close()

        # Group column rows by (schema, table)
        tables: dict[tuple[str, str], dict[str, Any]] = {}
        columns_by_table: dict[tuple[str, str], list[SourceColumnSchema]] = defaultdict(list)

        for raw_row in col_rows:
            row = dict(zip(col_names, raw_row))
            schema = str(row.get("TABLE_SCHEMA", ""))
            table = str(row.get("TABLE_NAME", ""))
            table_type = str(row.get("TABLE_TYPE", "BASE TABLE"))

            schema_upper = schema.upper()
            if schema_upper in exclude_upper:
                continue
            if include_upper is not None and schema_upper not in include_upper:
                continue

            key = (schema, table)
            if key not in tables:
                tables[key] = {"table_type": table_type}

            is_nullable = str(row.get("IS_NULLABLE", "YES")).upper() != "NO"
            col = SourceColumnSchema(
                name=str(row.get("COLUMN_NAME", "")),
                data_type=str(row.get("DATA_TYPE", "UNKNOWN")),
                is_nullable=is_nullable,
            )
            columns_by_table[key].append(col)

        objects: list[SourceTableSchema] = []
        for (schema, table), meta in tables.items():
            row_count = row_counts.get((schema, table))
            objects.append(
                SourceTableSchema(
                    schema_name=schema,
                    object_name=table,
                    object_kind=_map_table_type(meta["table_type"]),
                    columns=tuple(columns_by_table[(schema, table)]),
                    row_count=row_count,
                )
            )

        return SchemaSnapshot(
            captured_at=datetime.now(UTC),
            objects=tuple(objects),
        )

    async def observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        """Observe traffic and return canonical query events.

        .. deprecated:: 0.2.0
            Use :meth:`~alma_connectors.source_adapter_v2.SourceAdapterV2.extract_traffic` instead.
        """
        warnings.warn(
            "SnowflakeAdapter.observe_traffic() is deprecated since 0.2.0 and will be removed in 1.0.0. "
            "Use extract_traffic() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        config = self._get_config(adapter)
        lookback_hours = config.lookback_hours
        max_rows = config.max_query_rows

        traffic_sql = f"""\
SELECT
    QUERY_ID,
    QUERY_TEXT,
    USER_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    WAREHOUSE_NAME,
    EXECUTION_STATUS,
    START_TIME,
    END_TIME,
    TOTAL_ELAPSED_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
  AND EXECUTION_STATUS = 'SUCCESS'
  AND QUERY_TYPE IN ('SELECT', 'CTAS', 'INSERT', 'MERGE', 'UPDATE', 'DELETE', 'CREATE_TABLE_AS_SELECT')
ORDER BY START_TIME DESC
LIMIT {max_rows}
"""

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(traffic_sql)
            raw_rows = cur.fetchall()
            col_names = [desc[0].upper() for desc in cur.description]
            cur.close()
        finally:
            conn.close()

        events: list[ObservedQueryEvent] = []
        errors: list[str] = []

        for raw_row in raw_rows:
            row = dict(zip(col_names, raw_row))
            sql_text = str(row.get("QUERY_TEXT", "")).strip()
            if not sql_text:
                continue

            start_time = row.get("START_TIME")
            if isinstance(start_time, datetime):
                captured_at = start_time.astimezone(UTC) if start_time.tzinfo else start_time.replace(tzinfo=UTC)
            else:
                captured_at = datetime.now(UTC)

            end_time = row.get("END_TIME")
            elapsed_ms = row.get("TOTAL_ELAPSED_TIME")
            duration_ms: float | None = None
            if elapsed_ms is not None:
                try:
                    duration_ms = float(elapsed_ms)
                except (TypeError, ValueError):
                    pass

            query_id = str(row.get("QUERY_ID", "")) or None
            database_name = str(row.get("DATABASE_NAME", "")) or None
            user_name = str(row.get("USER_NAME", "")) or None

            try:
                event = ObservedQueryEvent(
                    captured_at=captured_at,
                    sql=sql_text,
                    source_name=adapter.target_id,
                    query_type="SELECT",
                    event_id=query_id,
                    database_name=database_name,
                    database_user=user_name,
                    duration_ms=duration_ms,
                    metadata={
                        "schema_name": str(row.get("SCHEMA_NAME", "") or ""),
                        "warehouse_name": str(row.get("WAREHOUSE_NAME", "") or ""),
                    },
                )
                events.append(event)
            except Exception as exc:
                errors.append(f"Skipped query event {query_id}: {exc}")

        return TrafficObservationResult(
            scanned_records=len(raw_rows),
            events=tuple(events),
            errors=tuple(errors),
        )

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
    ) -> QueryResult:
        config = self._get_config(adapter)
        start = time.monotonic()
        try:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                cur.execute(sql)
                col_names = [desc[0] for desc in cur.description] if cur.description else []
                all_rows = cur.fetchall()
                truncated = False
                if max_rows is not None and len(all_rows) > max_rows:
                    all_rows = all_rows[:max_rows]
                    truncated = True
                rows = tuple(dict(zip(col_names, r)) for r in all_rows)
                cur.close()
            finally:
                conn.close()
        except RuntimeError:
            raise
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            return QueryResult(
                success=False,
                row_count=0,
                duration_ms=elapsed_ms,
                error_message=str(exc),
            )
        elapsed_ms = (time.monotonic() - start) * 1000
        return QueryResult(
            success=True,
            row_count=len(rows),
            duration_ms=elapsed_ms,
            rows=rows,
            truncated=truncated,
        )

    def get_setup_instructions(self) -> SetupInstructions:
        return SetupInstructions(
            title="Snowflake Source Adapter Setup",
            summary=(
                "Grant Atlas read access to your Snowflake account so it can introspect schemas "
                "and observe query history via ACCOUNT_USAGE."
            ),
            steps=(
                "Create a dedicated Atlas role and user in Snowflake:\n"
                "  CREATE ROLE ATLAS_ROLE;\n"
                "  CREATE USER ATLAS_USER PASSWORD='...' DEFAULT_ROLE=ATLAS_ROLE;\n"
                "  GRANT ROLE ATLAS_ROLE TO USER ATLAS_USER;",
                "Grant USAGE on the warehouse and database:\n"
                "  GRANT USAGE ON WAREHOUSE <your_warehouse> TO ROLE ATLAS_ROLE;\n"
                "  GRANT USAGE ON DATABASE <your_database> TO ROLE ATLAS_ROLE;\n"
                "  GRANT USAGE ON ALL SCHEMAS IN DATABASE <your_database> TO ROLE ATLAS_ROLE;\n"
                "  GRANT SELECT ON ALL TABLES IN DATABASE <your_database> TO ROLE ATLAS_ROLE;\n"
                "  GRANT SELECT ON ALL VIEWS IN DATABASE <your_database> TO ROLE ATLAS_ROLE;",
                "Grant access to ACCOUNT_USAGE for query history observation:\n"
                "  GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ATLAS_ROLE;",
                "Store credentials as a JSON secret in the SNOWFLAKE_CONNECTION_JSON environment variable:\n"
                '  {"account": "xy12345.us-east-1", "user": "ATLAS_USER", "password": "...", '
                '"warehouse": "COMPUTE_WH", "database": "MY_DB", "role": "ATLAS_ROLE"}',
                "Configure the source in atlas.yml with kind: snowflake and the relevant params.",
            ),
            docs_url=None,
        )
