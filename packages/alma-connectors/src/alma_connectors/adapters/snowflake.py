"""Snowflake source adapter implementation."""

from __future__ import annotations

import contextlib
import json
import logging
import time
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
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DefinitionSnapshot,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionMeta,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    ObjectDefinition,
    SchemaObject,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)
from alma_connectors.source_adapter_v2 import (
    ColumnSchema as V2ColumnSchema,
)
from alma_connectors.source_adapter_v2 import (
    SchemaObjectKind as V2SchemaObjectKind,
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


def _map_v2_table_type(raw: str) -> V2SchemaObjectKind:
    upper = raw.upper()
    if upper == "VIEW":
        return V2SchemaObjectKind.VIEW
    if upper == "MATERIALIZED VIEW":
        return V2SchemaObjectKind.MATERIALIZED_VIEW
    return V2SchemaObjectKind.TABLE


def _parse_variant(val: Any) -> Any:
    """Parse a Snowflake VARIANT value which may come as dict, list, or JSON string."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return None
    return None


def _normalize_sf_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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

    # ------------------------------------------------------------------
    # v1 protocol methods (backward-compatible)
    # ------------------------------------------------------------------

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
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
                    rc = dict(zip(rc_desc, rc_row, strict=False))
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
            row = dict(zip(col_names, raw_row, strict=False))
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
            row = dict(zip(col_names, raw_row, strict=False))
            sql_text = str(row.get("QUERY_TEXT", "")).strip()
            if not sql_text:
                continue

            start_time = row.get("START_TIME")
            if isinstance(start_time, datetime):
                captured_at = start_time.astimezone(UTC) if start_time.tzinfo else start_time.replace(tzinfo=UTC)
            else:
                captured_at = datetime.now(UTC)

            elapsed_ms = row.get("TOTAL_ELAPSED_TIME")
            duration_ms: float | None = None
            if elapsed_ms is not None:
                with contextlib.suppress(TypeError, ValueError):
                    duration_ms = float(elapsed_ms)

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
                rows = tuple(dict(zip(col_names, r, strict=False)) for r in all_rows)
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

    # ------------------------------------------------------------------
    # SourceAdapterV2 protocol
    # ------------------------------------------------------------------

    @property
    def declared_capabilities(self) -> frozenset[AdapterCapability]:
        return frozenset({
            AdapterCapability.DISCOVER,
            AdapterCapability.SCHEMA,
            AdapterCapability.DEFINITIONS,
            AdapterCapability.TRAFFIC,
            AdapterCapability.LINEAGE,
        })

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe which capabilities are actually available.

        - DISCOVER: SHOW DATABASES
        - SCHEMA: SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1
        - DEFINITIONS: probe INFORMATION_SCHEMA.VIEWS and INFORMATION_SCHEMA.FUNCTIONS
        - TRAFFIC: SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY LIMIT 1
          (requires IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE)
        - LINEAGE: SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1
          (requires Enterprise edition + IMPORTED PRIVILEGES)
        """
        caps = capabilities if capabilities is not None else self.declared_capabilities
        config = self._get_config(adapter)
        db_prefix = f"{config.database}." if config.database else ""

        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"account": config.account},
        )
        results: list[CapabilityProbeResult] = []

        conn = self._connect(config)
        try:
            cur = conn.cursor()

            if AdapterCapability.DISCOVER in caps:
                try:
                    cur.execute("SHOW DATABASES")
                    cur.fetchall()
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.DISCOVER,
                        available=True,
                        scope=ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                    ))
                except Exception as exc:
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.DISCOVER,
                        available=False,
                        scope=ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                        message=str(exc),
                    ))

            if AdapterCapability.SCHEMA in caps:
                try:
                    cur.execute(f"SELECT 1 FROM {db_prefix}INFORMATION_SCHEMA.COLUMNS LIMIT 1")
                    cur.fetchall()
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.SCHEMA,
                        available=True,
                        scope=ExtractionScope.DATABASE if config.database else ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                    ))
                except Exception as exc:
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.SCHEMA,
                        available=False,
                        scope=ExtractionScope.DATABASE if config.database else ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                        message=str(exc),
                    ))

            if AdapterCapability.DEFINITIONS in caps:
                probe_errors: list[str] = []
                for probe_sql in (
                    f"SELECT 1 FROM {db_prefix}INFORMATION_SCHEMA.VIEWS LIMIT 1",
                    f"SELECT 1 FROM {db_prefix}INFORMATION_SCHEMA.FUNCTIONS LIMIT 1",
                ):
                    try:
                        cur.execute(probe_sql)
                        cur.fetchall()
                    except Exception as exc:
                        probe_errors.append(str(exc))
                results.append(CapabilityProbeResult(
                    capability=AdapterCapability.DEFINITIONS,
                    available=len(probe_errors) == 0,
                    scope=ExtractionScope.DATABASE if config.database else ExtractionScope.GLOBAL,
                    scope_context=scope_ctx,
                    message="; ".join(probe_errors) if probe_errors else None,
                ))

            if AdapterCapability.TRAFFIC in caps:
                try:
                    cur.execute("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY LIMIT 1")
                    cur.fetchall()
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.TRAFFIC,
                        available=True,
                        scope=ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                    ))
                except Exception as exc:
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.TRAFFIC,
                        available=False,
                        scope=ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                        message=str(exc),
                        permissions_missing=("IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE",),
                    ))

            if AdapterCapability.LINEAGE in caps:
                try:
                    cur.execute("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1")
                    cur.fetchall()
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.LINEAGE,
                        available=True,
                        scope=ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                        message="ACCESS_HISTORY available (Enterprise edition)",
                    ))
                except Exception as exc:
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.LINEAGE,
                        available=False,
                        scope=ExtractionScope.GLOBAL,
                        scope_context=scope_ctx,
                        message=str(exc),
                        permissions_missing=("IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE",),
                    ))

            cur.close()
        finally:
            conn.close()

        return tuple(results)

    async def discover(self, adapter: PersistedSourceAdapter) -> DiscoverySnapshot:
        """DISCOVER: enumerate databases and schemas via SHOW DATABASES / SHOW SCHEMAS."""
        config = self._get_config(adapter)
        started_at = time.monotonic()
        captured_at = datetime.now(UTC)

        conn = self._connect(config)
        try:
            cur = conn.cursor()

            cur.execute("SHOW DATABASES")
            db_rows = cur.fetchall()
            db_col_names = [desc[0].lower() for desc in cur.description]

            cur.execute("SHOW SCHEMAS IN ACCOUNT")
            schema_rows = cur.fetchall()
            schema_col_names = [desc[0].lower() for desc in cur.description]

            cur.close()
        finally:
            conn.close()

        containers: list[DiscoveredContainer] = []

        for raw_row in db_rows:
            row = dict(zip(db_col_names, raw_row, strict=False))
            db_name = str(row.get("name", "")).strip()
            if not db_name:
                continue
            owner = str(row.get("owner", "") or "")
            comment = str(row.get("comment", "") or "")
            containers.append(DiscoveredContainer(
                container_id=db_name,
                container_type="database",
                display_name=db_name,
                metadata={k: v for k, v in {"owner": owner, "comment": comment}.items() if v},
            ))

        for raw_row in schema_rows:
            row = dict(zip(schema_col_names, raw_row, strict=False))
            schema_name = str(row.get("name", "")).strip()
            db_name = str(row.get("database_name", "")).strip()
            if not schema_name or schema_name.upper() == "INFORMATION_SCHEMA":
                continue
            container_id = f"{db_name}.{schema_name}" if db_name else schema_name
            containers.append(DiscoveredContainer(
                container_id=container_id,
                container_type="schema",
                display_name=schema_name,
                metadata={"database_name": db_name} if db_name else {},
            ))

        duration_ms = (time.monotonic() - started_at) * 1000.0
        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"account": config.account},
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.SNOWFLAKE,
            capability=AdapterCapability.DISCOVER,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(containers),
        )
        return DiscoverySnapshot(meta=meta, containers=tuple(containers))

    async def extract_schema(self, adapter: PersistedSourceAdapter) -> SchemaSnapshotV2:
        """SCHEMA: tables/views with freshness, UDFs, and stored procedures.

        Queries (in order, with graceful fallback for optional views):
          1. INFORMATION_SCHEMA.COLUMNS    — columns for all tables/views
          2. INFORMATION_SCHEMA.TABLES     — row_count, bytes, last_altered (freshness)
          3. INFORMATION_SCHEMA.FUNCTIONS  — UDFs (best-effort)
          4. INFORMATION_SCHEMA.PROCEDURES — stored procedures (best-effort)
        """
        config = self._get_config(adapter)
        database = config.database
        db_prefix = f"{database}." if database else ""
        exclude_upper = frozenset(s.upper() for s in config.exclude_schemas)
        include_upper = frozenset(s.upper() for s in config.include_schemas) if config.include_schemas else None

        started_at = time.monotonic()
        captured_at = datetime.now(UTC)

        columns_sql = f"""\
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION
FROM {db_prefix}INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
"""

        tables_sql = f"""\
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    BYTES,
    LAST_ALTERED
FROM {db_prefix}INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
"""

        functions_sql = f"""\
SELECT
    FUNCTION_SCHEMA,
    FUNCTION_NAME,
    DATA_TYPE,
    ARGUMENT_SIGNATURE,
    FUNCTION_LANGUAGE,
    FUNCTION_DEFINITION,
    LAST_ALTERED
FROM {db_prefix}INFORMATION_SCHEMA.FUNCTIONS
WHERE FUNCTION_SCHEMA NOT IN ('INFORMATION_SCHEMA')
  AND IS_BUILTIN = 'N'
"""

        procedures_sql = f"""\
SELECT
    PROCEDURE_SCHEMA,
    PROCEDURE_NAME,
    DATA_TYPE,
    ARGUMENT_SIGNATURE,
    PROCEDURE_LANGUAGE,
    PROCEDURE_DEFINITION,
    LAST_ALTERED
FROM {db_prefix}INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
"""

        conn = self._connect(config)
        try:
            cur = conn.cursor()

            cur.execute(columns_sql)
            col_rows = cur.fetchall()
            col_names = [desc[0].upper() for desc in cur.description]

            table_meta: dict[tuple[str, str], dict[str, Any]] = {}
            try:
                cur.execute(tables_sql)
                tbl_col_names = [d[0].upper() for d in cur.description]
                for row in cur.fetchall():
                    r = dict(zip(tbl_col_names, row, strict=False))
                    tbl_schema = str(r.get("TABLE_SCHEMA", ""))
                    tbl_name = str(r.get("TABLE_NAME", ""))
                    table_meta[(tbl_schema, tbl_name)] = r
            except Exception:
                logger.debug(
                    "SnowflakeAdapter.extract_schema: TABLES metadata query failed. adapter=%s",
                    adapter.key,
                    exc_info=True,
                )

            func_rows: list[dict[str, Any]] = []
            try:
                cur.execute(functions_sql)
                func_col_names = [d[0].upper() for d in cur.description]
                for row in cur.fetchall():
                    func_rows.append(dict(zip(func_col_names, row, strict=False)))
            except Exception:
                logger.debug(
                    "SnowflakeAdapter.extract_schema: FUNCTIONS query failed. adapter=%s",
                    adapter.key,
                    exc_info=True,
                )

            proc_rows: list[dict[str, Any]] = []
            try:
                cur.execute(procedures_sql)
                proc_col_names = [d[0].upper() for d in cur.description]
                for row in cur.fetchall():
                    proc_rows.append(dict(zip(proc_col_names, row, strict=False)))
            except Exception:
                logger.debug(
                    "SnowflakeAdapter.extract_schema: PROCEDURES query failed. adapter=%s",
                    adapter.key,
                    exc_info=True,
                )

            cur.close()
        finally:
            conn.close()

        # Build column groups for tables/views
        seen_tables: dict[tuple[str, str], str] = {}  # (schema, name) -> table_type
        columns_by_table: dict[tuple[str, str], list[V2ColumnSchema]] = defaultdict(list)

        for raw_row in col_rows:
            row = dict(zip(col_names, raw_row, strict=False))
            schema = str(row.get("TABLE_SCHEMA", ""))
            table = str(row.get("TABLE_NAME", ""))

            schema_upper = schema.upper()
            if schema_upper in exclude_upper:
                continue
            if include_upper is not None and schema_upper not in include_upper:
                continue

            key = (schema, table)
            if key not in seen_tables:
                meta_row = table_meta.get(key, {})
                seen_tables[key] = str(meta_row.get("TABLE_TYPE", "BASE TABLE"))

            is_nullable = str(row.get("IS_NULLABLE", "YES")).upper() != "NO"
            columns_by_table[key].append(
                V2ColumnSchema(
                    name=str(row.get("COLUMN_NAME", "")),
                    data_type=str(row.get("DATA_TYPE", "UNKNOWN")),
                    is_nullable=is_nullable,
                )
            )

        objects_list: list[SchemaObject] = []
        for (schema, table), table_type in seen_tables.items():
            meta_row = table_meta.get((schema, table), {})
            row_count_raw = meta_row.get("ROW_COUNT")
            row_count = int(row_count_raw) if row_count_raw is not None else None
            bytes_raw = meta_row.get("BYTES")
            size_bytes = int(bytes_raw) if bytes_raw is not None else None
            last_modified = _normalize_sf_timestamp(meta_row.get("LAST_ALTERED"))
            objects_list.append(SchemaObject(
                schema_name=schema,
                object_name=table,
                kind=_map_v2_table_type(table_type),
                columns=tuple(columns_by_table[(schema, table)]),
                row_count=row_count,
                size_bytes=size_bytes,
                last_modified=last_modified,
            ))

        # UDFs
        for row in func_rows:
            schema = str(row.get("FUNCTION_SCHEMA", "")).strip()
            name = str(row.get("FUNCTION_NAME", "")).strip()
            if not schema or not name:
                continue
            schema_upper = schema.upper()
            if schema_upper in exclude_upper:
                continue
            if include_upper is not None and schema_upper not in include_upper:
                continue
            lang = str(row.get("FUNCTION_LANGUAGE", "") or "").strip() or None
            return_type = str(row.get("DATA_TYPE", "") or "").strip() or None
            definition_body = str(row.get("FUNCTION_DEFINITION", "") or "").strip() or None
            last_modified = _normalize_sf_timestamp(row.get("LAST_ALTERED"))
            objects_list.append(SchemaObject(
                schema_name=schema,
                object_name=name,
                kind=V2SchemaObjectKind.UDF,
                language=lang,
                return_type=return_type,
                definition_body=definition_body,
                last_modified=last_modified,
            ))

        # Stored procedures
        for row in proc_rows:
            schema = str(row.get("PROCEDURE_SCHEMA", "")).strip()
            name = str(row.get("PROCEDURE_NAME", "")).strip()
            if not schema or not name:
                continue
            schema_upper = schema.upper()
            if schema_upper in exclude_upper:
                continue
            if include_upper is not None and schema_upper not in include_upper:
                continue
            lang = str(row.get("PROCEDURE_LANGUAGE", "") or "").strip() or None
            return_type = str(row.get("DATA_TYPE", "") or "").strip() or None
            definition_body = str(row.get("PROCEDURE_DEFINITION", "") or "").strip() or None
            last_modified = _normalize_sf_timestamp(row.get("LAST_ALTERED"))
            objects_list.append(SchemaObject(
                schema_name=schema,
                object_name=name,
                kind=V2SchemaObjectKind.PROCEDURE,
                language=lang,
                return_type=return_type,
                definition_body=definition_body,
                last_modified=last_modified,
            ))

        duration_ms = (time.monotonic() - started_at) * 1000.0
        scope_ctx = ScopeContext(
            scope=ExtractionScope.DATABASE if database else ExtractionScope.GLOBAL,
            identifiers={"account": config.account, "database": database} if database else {"account": config.account},
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.SNOWFLAKE,
            capability=AdapterCapability.SCHEMA,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(objects_list),
        )
        return SchemaSnapshotV2(meta=meta, objects=tuple(objects_list))

    async def extract_definitions(self, adapter: PersistedSourceAdapter) -> DefinitionSnapshot:
        """DEFINITIONS: view SQL from INFORMATION_SCHEMA.VIEWS; function bodies from INFORMATION_SCHEMA.FUNCTIONS."""
        config = self._get_config(adapter)
        database = config.database
        db_prefix = f"{database}." if database else ""

        started_at = time.monotonic()
        captured_at = datetime.now(UTC)

        views_sql = f"""\
SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
FROM {db_prefix}INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
"""

        functions_sql = f"""\
SELECT
    FUNCTION_SCHEMA,
    FUNCTION_NAME,
    ARGUMENT_SIGNATURE,
    FUNCTION_LANGUAGE,
    FUNCTION_DEFINITION
FROM {db_prefix}INFORMATION_SCHEMA.FUNCTIONS
WHERE FUNCTION_SCHEMA NOT IN ('INFORMATION_SCHEMA')
  AND IS_BUILTIN = 'N'
  AND FUNCTION_DEFINITION IS NOT NULL
"""

        definitions: list[ObjectDefinition] = []

        conn = self._connect(config)
        try:
            cur = conn.cursor()

            # Views
            try:
                cur.execute(views_sql)
                view_col_names = [d[0].upper() for d in cur.description]
                for raw_row in cur.fetchall():
                    row = dict(zip(view_col_names, raw_row, strict=False))
                    schema_name = str(row.get("TABLE_SCHEMA", "")).strip()
                    object_name = str(row.get("TABLE_NAME", "")).strip()
                    view_def = row.get("VIEW_DEFINITION")
                    if not schema_name or not object_name or view_def is None:
                        continue
                    definition_text = str(view_def).strip()
                    if not definition_text:
                        continue
                    definitions.append(ObjectDefinition(
                        schema_name=schema_name,
                        object_name=object_name,
                        object_kind=V2SchemaObjectKind.VIEW,
                        definition_text=definition_text,
                        definition_language="sql",
                    ))
            except Exception:
                logger.warning(
                    "SnowflakeAdapter.extract_definitions: VIEWS query failed. adapter=%s",
                    adapter.key,
                    exc_info=True,
                )

            # Functions (UDFs)
            try:
                cur.execute(functions_sql)
                func_col_names = [d[0].upper() for d in cur.description]
                for raw_row in cur.fetchall():
                    row = dict(zip(func_col_names, raw_row, strict=False))
                    schema_name = str(row.get("FUNCTION_SCHEMA", "")).strip()
                    object_name = str(row.get("FUNCTION_NAME", "")).strip()
                    func_def = row.get("FUNCTION_DEFINITION")
                    if not schema_name or not object_name or func_def is None:
                        continue
                    definition_text = str(func_def).strip()
                    if not definition_text:
                        continue
                    lang = str(row.get("FUNCTION_LANGUAGE", "") or "sql").strip().lower() or "sql"
                    definitions.append(ObjectDefinition(
                        schema_name=schema_name,
                        object_name=object_name,
                        object_kind=V2SchemaObjectKind.UDF,
                        definition_text=definition_text,
                        definition_language=lang,
                    ))
            except Exception:
                logger.warning(
                    "SnowflakeAdapter.extract_definitions: FUNCTIONS query failed. adapter=%s",
                    adapter.key,
                    exc_info=True,
                )

            cur.close()
        finally:
            conn.close()

        duration_ms = (time.monotonic() - started_at) * 1000.0
        scope_ctx = ScopeContext(
            scope=ExtractionScope.DATABASE if database else ExtractionScope.GLOBAL,
            identifiers={"account": config.account, "database": database} if database else {"account": config.account},
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.SNOWFLAKE,
            capability=AdapterCapability.DEFINITIONS,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(definitions),
        )
        return DefinitionSnapshot(meta=meta, definitions=tuple(definitions))

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        """TRAFFIC: wrap observe_traffic() output into v2 TrafficExtractionResult."""
        config = self._get_config(adapter)
        started_at = time.monotonic()
        captured_at = datetime.now(UTC)

        v1_result = await self.observe_traffic(adapter, since=since)

        duration_ms = (time.monotonic() - started_at) * 1000.0
        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"account": config.account},
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.SNOWFLAKE,
            capability=AdapterCapability.TRAFFIC,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(v1_result.events),
        )
        return TrafficExtractionResult(
            meta=meta,
            events=v1_result.events,
            observation_cursor=None,
        )

    async def extract_lineage(self, adapter: PersistedSourceAdapter) -> LineageSnapshot:
        """LINEAGE: column-level lineage from SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY.

        Requires Enterprise edition. Returns an empty LineageSnapshot gracefully
        if ACCESS_HISTORY is unavailable (non-Enterprise or missing privileges).
        """
        config = self._get_config(adapter)
        lookback_hours = config.lookback_hours
        max_rows = config.max_query_rows

        started_at = time.monotonic()
        captured_at = datetime.now(UTC)

        lineage_sql = f"""\
SELECT
    QUERY_ID,
    QUERY_START_TIME,
    DIRECT_OBJECTS_ACCESSED,
    BASE_OBJECTS_ACCESSED,
    OBJECTS_MODIFIED
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE QUERY_START_TIME >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
LIMIT {max_rows}
"""

        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers={"account": config.account},
        )

        try:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                cur.execute(lineage_sql)
                raw_rows = cur.fetchall()
                col_names = [desc[0].upper() for desc in cur.description]
                cur.close()
            finally:
                conn.close()
        except Exception as exc:
            logger.info(
                "SnowflakeAdapter.extract_lineage: ACCESS_HISTORY unavailable "
                "(non-Enterprise edition or missing privileges). adapter=%s error=%s",
                adapter.key,
                exc,
            )
            duration_ms = (time.monotonic() - started_at) * 1000.0
            meta = ExtractionMeta(
                adapter_key=adapter.key,
                adapter_kind=SourceAdapterKindV2.SNOWFLAKE,
                capability=AdapterCapability.LINEAGE,
                scope_context=scope_ctx,
                captured_at=captured_at,
                duration_ms=duration_ms,
                row_count=0,
            )
            return LineageSnapshot(meta=meta, edges=())

        edges: list[LineageEdge] = []
        seen_edges: set[tuple[str, str]] = set()

        for raw_row in raw_rows:
            row = dict(zip(col_names, raw_row, strict=False))

            base_objects = _parse_variant(row.get("BASE_OBJECTS_ACCESSED")) or []
            modified_objects = _parse_variant(row.get("OBJECTS_MODIFIED")) or []

            if not isinstance(base_objects, list):
                base_objects = [base_objects] if base_objects else []
            if not isinstance(modified_objects, list):
                modified_objects = [modified_objects] if modified_objects else []

            for target_obj in modified_objects:
                if not isinstance(target_obj, dict):
                    continue
                target_name = str(target_obj.get("objectName", "")).strip()
                if not target_name:
                    continue
                target_cols = [
                    str(c.get("columnName", ""))
                    for c in (target_obj.get("columns") or [])
                    if isinstance(c, dict) and c.get("columnName")
                ]

                for source_obj in base_objects:
                    if not isinstance(source_obj, dict):
                        continue
                    source_name = str(source_obj.get("objectName", "")).strip()
                    if not source_name or source_name == target_name:
                        continue

                    edge_key = (source_name, target_name)
                    if edge_key in seen_edges:
                        continue
                    seen_edges.add(edge_key)

                    source_cols = [
                        str(c.get("columnName", ""))
                        for c in (source_obj.get("columns") or [])
                        if isinstance(c, dict) and c.get("columnName")
                    ]
                    col_mappings: tuple[tuple[str, str], ...] = ()
                    if source_cols and target_cols:
                        col_mappings = tuple(zip(source_cols, target_cols, strict=False))

                    edges.append(LineageEdge(
                        source_object=source_name,
                        target_object=target_name,
                        edge_kind=LineageEdgeKind.DECLARED,
                        confidence=1.0,
                        column_mappings=col_mappings,
                        metadata={"query_id": str(row.get("QUERY_ID", "") or "")},
                    ))

        duration_ms = (time.monotonic() - started_at) * 1000.0
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.SNOWFLAKE,
            capability=AdapterCapability.LINEAGE,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(edges),
        )
        return LineageSnapshot(meta=meta, edges=tuple(edges))

    async def extract_orchestration(self, adapter: PersistedSourceAdapter) -> None:  # type: ignore[override]
        raise NotImplementedError(
            "SnowflakeAdapter does not support ORCHESTRATION extraction. "
            "Snowflake Tasks are not yet implemented; use a dedicated orchestration adapter."
        )
