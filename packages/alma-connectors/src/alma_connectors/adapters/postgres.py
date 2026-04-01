"""PostgreSQL source adapter implementation."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar
from uuid import NAMESPACE_URL, uuid5

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from alma_connectors.source_adapter import (
    ConnectionTestResult,
    ExternalSecretRef,
    ManagedSecret,
    ObservedQueryEvent,
    PersistedSourceAdapter,
    PostgresAdapterConfig,
    PostgresLogCaptureConfig,
    QueryResult,
    SchemaObjectKind,
    SchemaSnapshot,
    SetupInstructions,
    SourceAdapterCapabilities,
    SourceAdapterKind,
    SourceColumnSchema,
    SourceObjectDependency,
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
    ObjectDefinition,
    SchemaObject,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)
from alma_connectors.source_adapter_v2 import (
    ColumnSchema as ColumnSchemaV2,
)
from alma_connectors.source_adapter_v2 import (
    ObjectDependency as ObjectDependencyV2,
)
from alma_connectors.source_adapter_v2 import (
    SchemaObjectKind as SchemaObjectKindV2,
)

logger = logging.getLogger(__name__)
_DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
_DEFAULT_STATEMENT_TIMEOUT_MS = 30_000
_DEFAULT_LOG_SCAN_MAX_LINES = 10_000
_DEFAULT_LOG_SCAN_MAX_BYTES = 5_000_000
_DEFAULT_LOG_SCAN_MAX_EVENTS = 2_000

def _validate_postgres_dsn(dsn: str, field_name: str = "DSN") -> None:
    """Validate that dsn looks like a PostgreSQL connection string."""
    stripped = dsn.strip()
    if not stripped:
        raise ValueError(f"{field_name} must be a non-empty string")
    lower = stripped.lower()
    if lower.startswith("postgresql://") or lower.startswith("postgres://"):
        return
    if "=" in stripped:
        return
    raise ValueError(
        f"{field_name} does not look like a valid PostgreSQL DSN (expected "
        f"'postgresql://...' or key=value conninfo)"
    )


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

_T = TypeVar("_T")


def _is_pg_retryable(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "connection" in msg and any(k in msg for k in ("refused", "reset", "timeout", "pool"))


async def _async_retry_with_backoff(  # noqa: UP047
    fn: Callable[[], Awaitable[_T]],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable: Callable[[Exception], bool],
) -> _T:
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return await fn()
        except Exception as exc:
            if not retryable(exc):
                raise
            last_exc = exc
            if attempt < max_attempts - 1:
                delay = min(base_delay * (2 ** attempt), max_delay)
                logger.warning(
                    "Retryable PG error (attempt %d/%d): %s. Retrying in %.1fs",
                    attempt + 1,
                    max_attempts,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
    raise last_exc  # type: ignore[misc]


def _readonly_options(*, statement_timeout_ms: int | None = _DEFAULT_STATEMENT_TIMEOUT_MS) -> str:
    options = ["-c default_transaction_read_only=on"]
    if statement_timeout_ms is not None:
        options.append(f"-c statement_timeout={statement_timeout_ms}")
    return " ".join(options)


async def _run_blocking[T](fn: Callable[[], T]) -> T:
    return await asyncio.to_thread(fn)


async def _run_pg_call[T](fn: Callable[[], T], *, retry: bool = True) -> T:
    async def _call() -> T:
        return await _run_blocking(fn)

    if not retry:
        return await _call()
    return await _async_retry_with_backoff(_call, retryable=_is_pg_retryable)


_DEFAULT_POSTGRES_INCLUDE_SCHEMAS = ("public",)
_DEFAULT_POSTGRES_EXCLUDE_SCHEMAS = ("pg_catalog", "information_schema")
_PG_STAT_STATEMENTS_SQL = """\
    SELECT
        pss.queryid::text AS queryid,
        pss.query,
        pss.calls,
        pss.total_exec_time,
        pss.mean_exec_time,
        r.rolname AS username,
        d.datname AS dbname
    FROM pg_stat_statements pss
    JOIN pg_roles r ON r.oid = pss.userid
    JOIN pg_database d ON d.oid = pss.dbid
    ORDER BY pss.total_exec_time DESC
    LIMIT 10000
"""
_POSTGRES_LOG_TIMESTAMP_PATTERN = (
    r"\d{4}-\d{2}-\d{2} "
    r"\d{2}:\d{2}:\d{2}(?:\.\d+)?"
    r"(?: [A-Z]+|[+-]\d{2}(?::?\d{2})?)?"
)
_POSTGRES_LOG_PREFIX_PATTERN = re.compile(
    rf"^(?P<timestamp>{_POSTGRES_LOG_TIMESTAMP_PATTERN}) \[(?P<pid>\d+)\] "
    r"db=(?P<database>[^,]*),user=(?P<user>[^,]*),app=(?P<app>[^,]*),client=(?P<client>\S+) "
    r"(?P<level>LOG|ERROR|STATEMENT):\s+(?P<body>.*)$"
)
_POSTGRES_DURATION_BODY_PATTERN = re.compile(r"duration:\s+(?P<duration_ms>[0-9.]+)\s+ms\s+statement:\s+(?P<sql>.+)$")
_POSTGRES_LOG_TIMEZONE_OFFSETS = {
    # UTC / GMT
    "UTC": "+00:00",
    "GMT": "+00:00",
    # North America
    "PST": "-08:00",
    "PDT": "-07:00",
    "MST": "-07:00",
    "MDT": "-06:00",
    "CST": "-06:00",
    "CDT": "-05:00",
    "EST": "-05:00",
    "EDT": "-04:00",
    # South America
    "BRT": "-03:00",
    "ART": "-03:00",
    "CLT": "-04:00",
    "CLST": "-03:00",
    # Europe
    "WET": "+00:00",
    "WEST": "+01:00",
    "CET": "+01:00",
    "CEST": "+02:00",
    "EET": "+02:00",
    "EEST": "+03:00",
    "MSK": "+03:00",
    # Asia
    "IST": "+05:30",
    "PKT": "+05:00",
    "HKT": "+08:00",
    "SGT": "+08:00",
    "JST": "+09:00",
    "KST": "+09:00",
    # Australia / Pacific
    "ACST": "+09:30",
    "AEST": "+10:00",
    "AEDT": "+11:00",
    "NZST": "+12:00",
    "NZDT": "+13:00",
}


def _parse_postgres_log_timestamp(raw_value: str) -> datetime:
    normalized = raw_value.strip()
    timestamp_body = normalized
    timezone_suffix: str | None = None

    if " " in normalized:
        candidate_body, candidate_timezone = normalized.rsplit(" ", 1)
        if candidate_timezone.isalpha() or re.fullmatch(r"[+-]\d{2}(?::?\d{2})?", candidate_timezone):
            timestamp_body = candidate_body
            timezone_suffix = candidate_timezone

    normalized = timestamp_body.replace(" ", "T", 1)
    if timezone_suffix is not None:
        if timezone_suffix.isalpha():
            offset = _POSTGRES_LOG_TIMEZONE_OFFSETS.get(timezone_suffix.upper())
            if offset is None:
                logger.warning(
                    "Unknown postgres log timezone abbreviation %r, assuming UTC",
                    timezone_suffix,
                )
                offset = "+00:00"
            normalized += offset
        elif re.fullmatch(r"[+-]\d{2}", timezone_suffix):
            normalized += f"{timezone_suffix}:00"
        elif re.fullmatch(r"[+-]\d{4}", timezone_suffix):
            normalized += f"{timezone_suffix[:3]}:{timezone_suffix[3:]}"
        else:
            normalized += timezone_suffix
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class PostgresAdapter:
    """Runtime PostgreSQL source adapter."""

    kind = SourceAdapterKind.POSTGRES
    capabilities = SourceAdapterCapabilities(
        can_test_connection=True,
        can_introspect_schema=True,
        can_observe_traffic=True,
        can_execute_query=True,
    )

    @property
    def declared_capabilities(self) -> frozenset[AdapterCapability]:
        return frozenset({
            AdapterCapability.DISCOVER,
            AdapterCapability.SCHEMA,
            AdapterCapability.DEFINITIONS,
            AdapterCapability.TRAFFIC,
        })

    def __init__(self, *, resolve_secret: Callable[[ManagedSecret | ExternalSecretRef], str]) -> None:
        self._resolve_secret = resolve_secret

    def _resolve_secret_value(self, secret: ManagedSecret | ExternalSecretRef) -> str:
        return self._resolve_secret(secret)

    def _get_config(self, adapter: PersistedSourceAdapter) -> PostgresAdapterConfig:
        if not isinstance(adapter.config, PostgresAdapterConfig):
            raise ValueError(f"adapter '{adapter.key}' is not configured as postgres")
        return adapter.config

    # ------------------------------------------------------------------
    # Async context manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> PostgresAdapter:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass  # connections are closed per-request

    def _get_dsn(
        self,
        adapter: PersistedSourceAdapter,
        *,
        probe_target: str | None = None,
    ) -> str:
        if probe_target == "read_replica":
            return self._get_read_replica_dsn(adapter)
        return self._resolve_secret_value(self._get_config(adapter).database_secret)

    def _get_read_replica_dsn(self, adapter: PersistedSourceAdapter) -> str:
        config = self._get_config(adapter)
        replica = config.read_replica
        if replica is None:
            raise ValueError(f"adapter '{adapter.key}' does not define a read_replica configuration")
        base_dsn = (
            self._resolve_secret_value(replica.database_secret)
            if replica.database_secret is not None
            else self._resolve_secret_value(config.database_secret)
        )
        if replica.host is None and replica.port is None:
            return base_dsn
        conninfo = conninfo_to_dict(base_dsn)
        if replica.host is not None:
            conninfo["host"] = replica.host
        if replica.port is not None:
            conninfo["port"] = str(replica.port)
        return make_conninfo(**conninfo)

    async def _validate_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Validate credentials and connectivity for the PostgreSQL adapter."""
        config = self._get_config(adapter)
        include_schemas = list(config.include_schemas)
        exclude_schemas = list(config.exclude_schemas)
        dsn = self._get_dsn(adapter)
        conditions: list[str] = []
        params: list[Any] = []
        if include_schemas:
            conditions.append("table_schema = ANY(%s)")
            params.append(include_schemas)
        if exclude_schemas:
            conditions.append("table_schema <> ALL(%s)")
            params.append(exclude_schemas)
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        
        def _fetch_validation_rows() -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                options=_readonly_options(),
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                table_row = conn.execute(
                    f"""
                    SELECT count(*) AS cnt
                    FROM information_schema.tables
                    WHERE {where_clause}
                    """,
                    params,
                ).fetchone()
                log_row = conn.execute(
                    "SELECT setting FROM pg_settings WHERE name = 'log_min_duration_statement'"
                ).fetchone()
            return table_row, log_row

        table_row, log_row = await _run_pg_call(_fetch_validation_rows)
        table_count = int((table_row or {}).get("cnt", 0))
        log_setting = (log_row or {}).get("setting")
        if log_setting is not None and log_setting != "-1":
            log_note = f" log_min_duration_statement={log_setting}ms."
        else:
            log_note = (
                " log_min_duration_statement is not configured;"
                " enable it or use pg_stat_statements for traffic observation."
            )
        return ConnectionTestResult(
            success=True,
            message=f"Connected successfully. Found {table_count} tables.{log_note}",
            resource_count=table_count,
            resource_label="tables",
        )

    async def _build_schema_snapshot_data(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshot:
        """Build schema object data from PostgreSQL metadata."""
        config = self._get_config(adapter)
        dsn = self._get_dsn(adapter)
        include_schemas = list(config.include_schemas)
        exclude_schemas = list(config.exclude_schemas)
        conditions: list[str] = []
        matview_conditions: list[str] = []
        params: list[Any] = []
        if include_schemas:
            conditions.append("c.table_schema = ANY(%s)")
            matview_conditions.append("n.nspname = ANY(%s)")
            params.append(include_schemas)
        if exclude_schemas:
            conditions.append("c.table_schema <> ALL(%s)")
            matview_conditions.append("n.nspname <> ALL(%s)")
            params.append(exclude_schemas)
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        matview_where_clause = " AND ".join(matview_conditions) if matview_conditions else "TRUE"
        schema_sql = f"""
            SELECT
              c.table_schema AS schema_name,
              c.table_name,
              c.column_name,
              c.data_type,
              c.is_nullable,
              CASE
                WHEN mv.matviewname IS NOT NULL THEN 'MATERIALIZED VIEW'
                ELSE COALESCE(t.table_type, 'BASE TABLE')
              END AS table_type
            FROM information_schema.columns c
            LEFT JOIN information_schema.tables t
              ON t.table_schema = c.table_schema
             AND t.table_name = c.table_name
            LEFT JOIN pg_catalog.pg_matviews mv
              ON mv.schemaname = c.table_schema
             AND mv.matviewname = c.table_name
            WHERE {where_clause}

            UNION ALL

            SELECT
              n.nspname AS schema_name,
              cls.relname AS table_name,
              a.attname AS column_name,
              pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
              CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
              'MATERIALIZED VIEW'::text AS table_type
            FROM pg_catalog.pg_matviews mv2
            JOIN pg_catalog.pg_class cls ON cls.relname = mv2.matviewname
            JOIN pg_catalog.pg_namespace n
              ON n.oid = cls.relnamespace
             AND n.nspname = mv2.schemaname
            JOIN pg_catalog.pg_attribute a
              ON a.attrelid = cls.oid
             AND a.attnum > 0
             AND NOT a.attisdropped
            WHERE {matview_where_clause}

            ORDER BY schema_name, table_name, column_name
        """
        dependency_placeholders = ",".join(["%s"] * len(include_schemas))
        dependency_sql = f"""
            SELECT DISTINCT
                ns.nspname AS source_schema,
                v.relname AS source_object,
                dns.nspname AS target_schema,
                d.relname AS target_object,
                CASE v.relkind
                  WHEN 'v' THEN 'view'
                  WHEN 'm' THEN 'materialized_view'
                  ELSE 'table'
                END AS object_kind
            FROM pg_depend dep
            JOIN pg_rewrite rw ON dep.objid = rw.oid
            JOIN pg_class v ON rw.ev_class = v.oid AND v.relkind IN ('v', 'm')
            JOIN pg_class d ON dep.refobjid = d.oid AND d.relkind IN ('r', 'v', 'm')
            JOIN pg_namespace ns
              ON v.relnamespace = ns.oid AND ns.nspname IN ({dependency_placeholders})
            JOIN pg_namespace dns ON d.relnamespace = dns.oid
            WHERE dep.deptype = 'n' AND v.relname <> d.relname
        """
        stats_conditions: list[str] = []
        stats_params: list[Any] = []
        if include_schemas:
            stats_conditions.append("n.nspname = ANY(%s)")
            stats_params.append(include_schemas)
        if exclude_schemas:
            stats_conditions.append("n.nspname <> ALL(%s)")
            stats_params.append(exclude_schemas)
        stats_where_clause = " AND ".join(stats_conditions) if stats_conditions else "TRUE"
        stats_sql = f"""
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                GREATEST(COALESCE(s.n_live_tup, c.reltuples), 0)::bigint AS row_count
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_stat_user_tables s
              ON s.relid = c.oid
            WHERE c.relkind IN ('r', 'm')
              AND {stats_where_clause}
        """

        def _fetch_schema_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                options=_readonly_options(),
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                rows = list(conn.execute(schema_sql, params + params).fetchall())
                dependency_rows = (
                    list(conn.execute(dependency_sql, include_schemas).fetchall())
                    if include_schemas
                    else []
                )
                stats_rows = list(conn.execute(stats_sql, stats_params).fetchall())
            return rows, dependency_rows, stats_rows

        rows, dependency_rows, stats_rows = await _run_pg_call(_fetch_schema_rows)
        row_count_by_object = {
            (str(row["schema_name"]), str(row["table_name"])): max(0, int(row["row_count"]))
            for row in stats_rows
            if row.get("row_count") is not None
        }

        grouped_columns: dict[tuple[str, str, SchemaObjectKind], list[SourceColumnSchema]] = defaultdict(list)
        for row in rows:
            object_kind = SchemaObjectKind.TABLE
            table_type = str(row["table_type"]).upper()
            if table_type == "VIEW":
                object_kind = SchemaObjectKind.VIEW
            elif table_type == "MATERIALIZED VIEW":
                object_kind = SchemaObjectKind.MATERIALIZED_VIEW
            grouped_columns[(str(row["schema_name"]), str(row["table_name"]), object_kind)].append(
                SourceColumnSchema(
                    name=str(row["column_name"]),
                    data_type=str(row["data_type"]),
                    is_nullable=str(row["is_nullable"]).upper() == "YES",
                )
            )

        objects = tuple(
            SourceTableSchema(
                schema_name=schema_name,
                object_name=object_name,
                object_kind=object_kind,
                columns=tuple(columns),
                row_count=row_count_by_object.get((schema_name, object_name)),
            )
            for (schema_name, object_name, object_kind), columns in grouped_columns.items()
        )
        dependencies = tuple(
            SourceObjectDependency(
                source_schema=str(row["source_schema"]),
                source_object=str(row["source_object"]),
                target_schema=str(row["target_schema"]),
                target_object=str(row["target_object"]),
            )
            for row in dependency_rows
        )
        return SchemaSnapshot(
            captured_at=datetime.now(tz=UTC),
            objects=objects,
            dependencies=dependencies,
        )

    async def _observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        """Observe traffic and return canonical query events."""
        config = self._get_config(adapter)
        if config.log_capture is not None:
            return await asyncio.to_thread(self._observe_from_logs, adapter, config.log_capture, since=since)
        return await self._observe_from_pg_stat_statements(adapter, since=since)

    def _observe_from_logs(
        self,
        adapter: PersistedSourceAdapter,
        log_capture: PostgresLogCaptureConfig,
        *,
        since: datetime | None,
    ) -> TrafficObservationResult:
        pending_errors: dict[str, dict[str, str]] = {}
        scanned_records = 0
        scanned_bytes = 0
        events: list[ObservedQueryEvent] = []
        errors: list[str] = []
        log_path = Path(log_capture.log_path)

        # Determine the starting byte offset from the stored cursor.
        # If the inode changed (log rotation) we restart from the beginning.
        cursor = adapter.observation_cursor or {}
        cursor_inode: int | None = None
        cursor_offset: int | None = None
        raw_inode = cursor.get("inode")
        raw_offset = cursor.get("offset")
        if isinstance(raw_inode, int) and isinstance(raw_offset, int):
            cursor_inode = raw_inode
            cursor_offset = raw_offset

        try:
            fh = log_path.open("r", encoding="utf-8", errors="replace")
        except (FileNotFoundError, PermissionError) as exc:
            return TrafficObservationResult(
                scanned_records=0,
                events=(),
                errors=(f"cannot open log file '{log_capture.log_path}': {exc}",),
            )

        final_inode: int | None = None
        final_offset: int | None = None

        with fh as handle:
            # Resolve the current inode to detect log rotation.
            try:
                current_inode = os.fstat(handle.fileno()).st_ino
            except OSError:
                current_inode = None

            # Seek to the stored offset only when the inode matches (no rotation).
            if cursor_inode is not None and cursor_offset is not None and current_inode == cursor_inode:
                try:
                    handle.seek(cursor_offset)
                except OSError:
                    handle.seek(0)

            for raw_line in handle:
                scanned_records += 1
                scanned_bytes += len(raw_line.encode("utf-8", errors="ignore"))
                if scanned_records > _DEFAULT_LOG_SCAN_MAX_LINES:
                    errors.append(
                        "log scan truncated after max lines;"
                        f" increase the cap in code if {log_capture.log_path} is expected to be much larger."
                    )
                    break
                if scanned_bytes > _DEFAULT_LOG_SCAN_MAX_BYTES:
                    errors.append(
                        "log scan truncated after max bytes;"
                        f" increase the cap in code if {log_capture.log_path} is expected to be much larger."
                    )
                    break
                match = _POSTGRES_LOG_PREFIX_PATTERN.match(raw_line.rstrip("\n"))
                if match is None:
                    continue

                timestamp = _parse_postgres_log_timestamp(match.group("timestamp"))
                if since is not None and timestamp < since:
                    continue

                pid = match.group("pid")
                database = match.group("database")
                user = match.group("user")
                app = match.group("app")
                client = match.group("client")
                level = match.group("level")
                body = match.group("body")
                source_name = app.strip() or log_capture.default_source or adapter.key
                database_name = database.strip() or log_capture.default_database_name
                database_user = user.strip() or log_capture.default_database_user
                client_addr = client.strip() or None
                event_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"{adapter.id}:{scanned_records}:{raw_line.rstrip()}",
                    )
                )
                event_metadata = {
                    "adapter": "postgres_logs",
                    "log_path": log_capture.log_path,
                    "parser_contract": "velum_postgres_log_v1",
                    "pid": pid,
                    "client_addr": client_addr or "",
                }
                raw_payload = {
                    "line_number": scanned_records,
                    "raw_line": raw_line.rstrip("\n"),
                    "client_addr": client_addr,
                }

                if level == "LOG":
                    duration_match = _POSTGRES_DURATION_BODY_PATTERN.search(body)
                    if duration_match is None:
                        continue
                    events.append(
                        ObservedQueryEvent(
                            captured_at=timestamp,
                            sql=duration_match.group("sql"),
                            source_name=source_name,
                            query_type="duration_statement",
                            event_id=event_id,
                            database_name=database_name,
                            database_user=database_user,
                            client_addr=client_addr,
                            statement_id=pid,
                            duration_ms=float(duration_match.group("duration_ms")),
                            metadata=event_metadata,
                            raw_payload=raw_payload,
                        )
                    )
                    if len(events) >= _DEFAULT_LOG_SCAN_MAX_EVENTS:
                        errors.append("log scan truncated after max events were emitted")
                        break
                    continue

                if level == "ERROR":
                    pending_errors[pid] = {
                        "timestamp": match.group("timestamp"),
                        "database": database,
                        "user": user,
                        "app": app,
                        "error_message": body.strip(),
                    }
                    continue

                if level != "STATEMENT":
                    continue

                pending_error = pending_errors.pop(pid, None)
                if pending_error is None:
                    events.append(
                        ObservedQueryEvent(
                            captured_at=timestamp,
                            sql=body,
                            source_name=source_name,
                            query_type="statement",
                            event_id=event_id,
                            database_name=database_name,
                            database_user=database_user,
                            client_addr=client_addr,
                            statement_id=pid,
                            metadata=event_metadata,
                            raw_payload=raw_payload,
                        )
                    )
                    continue

                events.append(
                    ObservedQueryEvent(
                        captured_at=_parse_postgres_log_timestamp(pending_error["timestamp"]),
                        sql=body,
                        source_name=pending_error["app"].strip() or log_capture.default_source or adapter.key,
                        query_type="error_statement",
                        event_id=event_id,
                        database_name=pending_error["database"].strip() or log_capture.default_database_name,
                        database_user=pending_error["user"].strip() or log_capture.default_database_user,
                        client_addr=client_addr,
                        statement_id=pid,
                        error_message=pending_error["error_message"],
                        metadata=event_metadata,
                        raw_payload=raw_payload,
                    )
                )
                if len(events) >= _DEFAULT_LOG_SCAN_MAX_EVENTS:
                    errors.append("log scan truncated after max events were emitted")
                    break

            # Record byte position and inode after consuming the file so the
            # next run can seek past already-processed content.
            try:
                final_offset = handle.tell()
                final_inode = current_inode
            except OSError:
                pass

        new_cursor: dict[str, object] | None = None
        if final_inode is not None and final_offset is not None:
            new_cursor = {"inode": final_inode, "offset": final_offset}

        return TrafficObservationResult(
            scanned_records=scanned_records,
            events=tuple(events),
            errors=tuple(errors),
            observation_cursor=new_cursor,
        )

    async def _observe_from_pg_stat_statements(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None,
    ) -> TrafficObservationResult:
        """Observe traffic via pg_stat_statements when no log file is configured.

        pg_stat_statements records cumulative per-query aggregates reset by
        pg_stat_statements_reset().  It carries no timestamps, so the ``since``
        parameter cannot be honoured — all recorded statements are returned and
        the caller must deduplicate by event_id across runs.
        """
        dsn = self._get_dsn(adapter)
        errors: list[str] = []
        if since is not None:
            errors.append(
                "pg_stat_statements does not support time-based filtering;"
                " all recorded statements are returned regardless of 'since'."
            )
        def _fetch_pg_stat_rows() -> list[dict[str, Any]]:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                options=_readonly_options(),
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                return list(conn.execute(_PG_STAT_STATEMENTS_SQL).fetchall())

        try:
            rows = await _run_pg_call(_fetch_pg_stat_rows)
        except psycopg.Error as exc:
            pgcode = getattr(exc, "pgcode", None)
            if pgcode == "42P01":  # undefined_table — extension not installed
                return TrafficObservationResult(
                    scanned_records=0,
                    events=(),
                    errors=(
                        "pg_stat_statements extension is not installed;"
                        " run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
                    ),
                )
            if pgcode == "42501":  # insufficient_privilege
                return TrafficObservationResult(
                    scanned_records=0,
                    events=(),
                    errors=(
                        "insufficient privilege to query pg_stat_statements;"
                        " grant SELECT on pg_stat_statements to the configured role.",
                    ),
                )
            raise

        now = datetime.now(tz=UTC)
        events: list[ObservedQueryEvent] = []
        for row in rows:
            sql = str(row.get("query") or "").strip()
            if not sql:
                continue
            queryid = str(row.get("queryid") or "")
            event_id = str(uuid5(NAMESPACE_URL, f"{adapter.id}:pg_stat_statements:{queryid}"))
            dbname = str(row.get("dbname") or "").strip() or None
            username = str(row.get("username") or "").strip() or None
            events.append(
                ObservedQueryEvent(
                    captured_at=now,
                    sql=sql,
                    source_name=adapter.key,
                    query_type="pg_stat_statements",
                    event_id=event_id,
                    database_name=dbname,
                    database_user=username,
                    duration_ms=float(row.get("mean_exec_time") or 0.0),
                    metadata={
                        "adapter": "pg_stat_statements",
                        "queryid": queryid,
                        "calls": int(row.get("calls") or 0),
                        "total_exec_time_ms": float(row.get("total_exec_time") or 0.0),
                        "mean_exec_time_ms": float(row.get("mean_exec_time") or 0.0),
                    },
                )
            )

        return TrafficObservationResult(
            scanned_records=len(rows),
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
        dsn = self._get_dsn(adapter, probe_target=probe_target)
        started_at = time.perf_counter()
        row_limit = max_rows if max_rows and max_rows > 0 else 100

        def _fetch_query_rows() -> list[dict[str, Any]]:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                options=_readonly_options(),
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                result = conn.execute(sql)
                return list(result.fetchmany(row_limit + 1))

        rows = await _run_pg_call(_fetch_query_rows)
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        truncated = len(rows) > row_limit
        visible_rows = rows[:row_limit]
        return QueryResult(
            success=True,
            row_count=len(visible_rows),
            duration_ms=duration_ms,
            rows=tuple(dict(row) for row in visible_rows),
            truncated=truncated,
        )

    # -----------------------------------------------------------------------
    # SourceAdapterV2 methods
    # -----------------------------------------------------------------------

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe which v2 capabilities are actually available at runtime."""
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities
        config = self._get_config(adapter)
        dsn = self._get_dsn(adapter)

        info_schema_ok = False
        pg_proc_ok = False
        pg_stat_ok = False
        info_schema_missing: list[str] = []
        pg_proc_missing: list[str] = []
        pg_stat_missing: list[str] = []
        pg_stat_message: str | None = None
        pg_stat_fallback_used = False

        def _probe_runtime_capabilities() -> tuple[bool, bool, bool, list[str], list[str], list[str], str | None, bool]:
            info_schema_ok_local = False
            pg_proc_ok_local = False
            pg_stat_ok_local = False
            info_schema_missing_local: list[str] = []
            pg_proc_missing_local: list[str] = []
            pg_stat_missing_local: list[str] = []
            pg_stat_message_local: str | None = None
            pg_stat_fallback_used_local = False

            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                autocommit=True,
                options=_readonly_options(),
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                try:
                    conn.execute("SELECT 1 FROM information_schema.schemata LIMIT 1")
                    info_schema_ok_local = True
                except psycopg.Error as exc:
                    if getattr(exc, "pgcode", None) == "42501":
                        info_schema_missing_local.append("SELECT ON information_schema.schemata")

                try:
                    conn.execute("SELECT 1 FROM pg_catalog.pg_proc LIMIT 1")
                    pg_proc_ok_local = True
                except psycopg.Error as exc:
                    if getattr(exc, "pgcode", None) == "42501":
                        pg_proc_missing_local.append("SELECT ON pg_catalog.pg_proc")

                try:
                    conn.execute("SELECT 1 FROM pg_stat_statements LIMIT 1")
                    pg_stat_ok_local = True
                except psycopg.Error as exc:
                    pgcode = getattr(exc, "pgcode", None)
                    if pgcode == "42P01":  # undefined_table — extension not loaded
                        pg_stat_message_local = (
                            "pg_stat_statements extension not loaded;"
                            " run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements"
                        )
                        if config.log_capture is not None:
                            pg_stat_fallback_used_local = True
                    elif pgcode == "42501":
                        pg_stat_missing_local.append("SELECT ON pg_stat_statements")

            return (
                info_schema_ok_local,
                pg_proc_ok_local,
                pg_stat_ok_local,
                info_schema_missing_local,
                pg_proc_missing_local,
                pg_stat_missing_local,
                pg_stat_message_local,
                pg_stat_fallback_used_local,
            )

        with contextlib.suppress(psycopg.Error):
            (
                info_schema_ok,
                pg_proc_ok,
                pg_stat_ok,
                info_schema_missing,
                pg_proc_missing,
                pg_stat_missing,
                pg_stat_message,
                pg_stat_fallback_used,
            ) = await _run_pg_call(_probe_runtime_capabilities)

        scope = ExtractionScope.DATABASE
        results: list[CapabilityProbeResult] = []

        for cap in sorted(caps_to_probe, key=lambda c: c.value):
            if cap == AdapterCapability.DISCOVER:
                results.append(CapabilityProbeResult(
                    capability=cap,
                    available=info_schema_ok,
                    scope=scope,
                    permissions_missing=tuple(info_schema_missing),
                    message=None if info_schema_ok else "cannot access information_schema.schemata",
                ))
            elif cap == AdapterCapability.SCHEMA:
                available = info_schema_ok and pg_proc_ok
                missing = info_schema_missing + pg_proc_missing
                results.append(CapabilityProbeResult(
                    capability=cap,
                    available=available,
                    scope=scope,
                    permissions_missing=tuple(missing),
                    message=None if available else "missing access to information_schema or pg_catalog.pg_proc",
                ))
            elif cap == AdapterCapability.DEFINITIONS:
                results.append(CapabilityProbeResult(
                    capability=cap,
                    available=pg_proc_ok,
                    scope=scope,
                    permissions_missing=tuple(pg_proc_missing),
                    message=None if pg_proc_ok else "cannot access pg_catalog.pg_proc",
                ))
            elif cap == AdapterCapability.TRAFFIC:
                if pg_stat_ok:
                    results.append(CapabilityProbeResult(
                        capability=cap,
                        available=True,
                        scope=scope,
                    ))
                elif pg_stat_fallback_used:
                    results.append(CapabilityProbeResult(
                        capability=cap,
                        available=True,
                        scope=scope,
                        fallback_used=True,
                        message="using log-based traffic observation (pg_stat_statements not loaded)",
                    ))
                else:
                    results.append(CapabilityProbeResult(
                        capability=cap,
                        available=False,
                        scope=scope,
                        permissions_missing=tuple(pg_stat_missing),
                        message=pg_stat_message,
                    ))

        return tuple(results)

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: enumerate PostgreSQL schemas as containers."""
        dsn = self._get_dsn(adapter)
        started_at = time.perf_counter()
        logger.debug("PostgreSQL discover started: adapter=%s", adapter.key)

        def _discover_rows() -> list[dict[str, Any]]:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                options=_readonly_options(),
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                return list(conn.execute("""
                    SELECT nspname
                    FROM pg_catalog.pg_namespace
                    WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                      AND nspname NOT LIKE 'pg_toast_%'
                      AND nspname NOT LIKE 'pg_temp_%'
                    ORDER BY nspname
                """).fetchall())

        rows = await _run_pg_call(_discover_rows)

        duration_ms = (time.perf_counter() - started_at) * 1000.0
        now = datetime.now(tz=UTC)

        containers = tuple(
            DiscoveredContainer(
                container_id=f"{adapter.key}/{row['nspname']}",
                container_type="schema",
                display_name=str(row["nspname"]),
            )
            for row in rows
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.DISCOVER,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE),
            captured_at=now,
            duration_ms=duration_ms,
            row_count=len(containers),
        )
        return DiscoverySnapshot(meta=meta, containers=containers)

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """SCHEMA: tables/views from v1 introspect_schema + routines from pg_proc + freshness."""
        started_at = time.perf_counter()
        v1_snapshot = await self._build_schema_snapshot_data(adapter)

        config = self._get_config(adapter)
        dsn = self._get_dsn(adapter)
        include_schemas = list(config.include_schemas)
        exclude_schemas = list(config.exclude_schemas)

        routine_conds: list[str] = []
        freshness_conds: list[str] = []
        routine_params: list[Any] = []
        freshness_params: list[Any] = []

        if include_schemas:
            routine_conds.append("n.nspname = ANY(%s)")
            freshness_conds.append("s.schemaname = ANY(%s)")
            routine_params.append(include_schemas)
            freshness_params.append(include_schemas)
        if exclude_schemas:
            routine_conds.append("n.nspname <> ALL(%s)")
            freshness_conds.append("s.schemaname <> ALL(%s)")
            routine_params.append(exclude_schemas)
            freshness_params.append(exclude_schemas)

        routine_where = " AND ".join(routine_conds) if routine_conds else "TRUE"
        freshness_where = " AND ".join(freshness_conds) if freshness_conds else "TRUE"

        routine_sql = f"""
            SELECT
                n.nspname AS schema_name,
                p.proname AS routine_name,
                p.prokind,
                l.lanname AS language,
                pg_catalog.pg_get_function_result(p.oid) AS return_type
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_catalog.pg_language l ON l.oid = p.prolang
            WHERE p.prokind IN ('f', 'p')
              AND {routine_where}
            ORDER BY n.nspname, p.proname
        """
        freshness_sql = f"""
            SELECT s.schemaname, s.relname, s.last_autovacuum, s.n_live_tup
            FROM pg_catalog.pg_stat_user_tables s
            WHERE {freshness_where}
        """

        def _extract_schema_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                options=_readonly_options(),
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ) as conn:
                routine_rows = list(conn.execute(routine_sql, routine_params).fetchall())
                freshness_rows = list(conn.execute(freshness_sql, freshness_params).fetchall())
            return routine_rows, freshness_rows

        routine_rows, freshness_rows = await _run_pg_call(_extract_schema_rows)

        freshness_by_table: dict[tuple[str, str], dict[str, Any]] = {
            (str(row["schemaname"]), str(row["relname"])): dict(row)
            for row in freshness_rows
        }

        _v1_to_v2_kind: dict[SchemaObjectKind, SchemaObjectKindV2] = {
            SchemaObjectKind.TABLE: SchemaObjectKindV2.TABLE,
            SchemaObjectKind.VIEW: SchemaObjectKindV2.VIEW,
            SchemaObjectKind.MATERIALIZED_VIEW: SchemaObjectKindV2.MATERIALIZED_VIEW,
        }
        # EXTERNAL_TABLE may not exist in all versions of the v1 enum
        _ext = getattr(SchemaObjectKind, "EXTERNAL_TABLE", None)
        _ext_v2 = getattr(SchemaObjectKindV2, "EXTERNAL_TABLE", None)
        if _ext is not None and _ext_v2 is not None:
            _v1_to_v2_kind[_ext] = _ext_v2

        objects: list[SchemaObject] = []

        for obj in v1_snapshot.objects:
            freshness = freshness_by_table.get((obj.schema_name, obj.object_name))
            last_modified: datetime | None = None
            row_count = obj.row_count
            if freshness:
                raw_vac = freshness.get("last_autovacuum")
                if isinstance(raw_vac, datetime):
                    last_modified = raw_vac
                if row_count is None:
                    n_live = freshness.get("n_live_tup")
                    if n_live is not None:
                        row_count = max(0, int(n_live))

            columns_v2 = tuple(
                ColumnSchemaV2(
                    name=col.name,
                    data_type=col.data_type,
                    is_nullable=col.is_nullable,
                )
                for col in obj.columns
            )
            objects.append(SchemaObject(
                schema_name=obj.schema_name,
                object_name=obj.object_name,
                kind=_v1_to_v2_kind.get(obj.object_kind, SchemaObjectKindV2.TABLE),
                columns=columns_v2,
                last_modified=last_modified,
                row_count=row_count,
            ))

        _prokind_to_v2 = {"f": SchemaObjectKindV2.UDF, "p": SchemaObjectKindV2.PROCEDURE}
        for row in routine_rows:
            rt = str(row.get("return_type") or "").strip() or None
            lang = str(row.get("language") or "").strip() or None
            objects.append(SchemaObject(
                schema_name=str(row["schema_name"]),
                object_name=str(row["routine_name"]),
                kind=_prokind_to_v2.get(str(row.get("prokind", "f")), SchemaObjectKindV2.UDF),
                language=lang,
                return_type=rt,
            ))

        dependencies_v2 = tuple(
            ObjectDependencyV2(
                source_schema=dep.source_schema,
                source_object=dep.source_object,
                target_schema=dep.target_schema,
                target_object=dep.target_object,
            )
            for dep in v1_snapshot.dependencies
        )

        duration_ms = (time.perf_counter() - started_at) * 1000.0
        now = datetime.now(tz=UTC)
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.SCHEMA,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE),
            captured_at=now,
            duration_ms=duration_ms,
            row_count=len(objects),
        )
        return SchemaSnapshotV2(meta=meta, objects=tuple(objects), dependencies=dependencies_v2)

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """DEFINITIONS: view SQL via pg_get_viewdef + routine source via pg_get_functiondef."""
        config = self._get_config(adapter)
        dsn = self._get_dsn(adapter)
        include_schemas = list(config.include_schemas)
        exclude_schemas = list(config.exclude_schemas)

        view_conds: list[str] = []
        routine_conds: list[str] = []
        view_params: list[Any] = []
        routine_params: list[Any] = []

        if include_schemas:
            view_conds.append("n.nspname = ANY(%s)")
            routine_conds.append("n.nspname = ANY(%s)")
            view_params.append(include_schemas)
            routine_params.append(include_schemas)
        if exclude_schemas:
            view_conds.append("n.nspname <> ALL(%s)")
            routine_conds.append("n.nspname <> ALL(%s)")
            view_params.append(exclude_schemas)
            routine_params.append(exclude_schemas)

        view_where = " AND ".join(view_conds) if view_conds else "TRUE"
        routine_where = " AND ".join(routine_conds) if routine_conds else "TRUE"

        view_sql = f"""
            SELECT
                n.nspname AS schema_name,
                c.relname AS object_name,
                CASE c.relkind
                    WHEN 'v' THEN 'view'
                    WHEN 'm' THEN 'materialized_view'
                END AS kind,
                pg_catalog.pg_get_viewdef(c.oid, true) AS definition
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('v', 'm')
              AND {view_where}
            ORDER BY n.nspname, c.relname
        """
        routine_sql = f"""
            SELECT
                n.nspname AS schema_name,
                p.proname AS object_name,
                p.prokind,
                l.lanname AS language,
                pg_catalog.pg_get_functiondef(p.oid) AS definition
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_catalog.pg_language l ON l.oid = p.prolang
            WHERE p.prokind IN ('f', 'p')
              AND l.lanname NOT IN ('c', 'internal')
              AND {routine_where}
            ORDER BY n.nspname, p.proname
        """

        started_at = time.perf_counter()
        def _extract_definition_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
                options=_readonly_options(),
            ) as conn:
                view_rows = list(conn.execute(view_sql, view_params).fetchall())
                routine_rows = list(conn.execute(routine_sql, routine_params).fetchall())
            return view_rows, routine_rows

        view_rows, routine_rows = await _run_pg_call(_extract_definition_rows)

        duration_ms = (time.perf_counter() - started_at) * 1000.0
        now = datetime.now(tz=UTC)

        _kind_map: dict[str, SchemaObjectKindV2] = {
            "view": SchemaObjectKindV2.VIEW,
            "materialized_view": SchemaObjectKindV2.MATERIALIZED_VIEW,
        }
        _prokind_map: dict[str, SchemaObjectKindV2] = {
            "f": SchemaObjectKindV2.UDF,
            "p": SchemaObjectKindV2.PROCEDURE,
        }

        definitions: list[ObjectDefinition] = []
        for row in view_rows:
            defn = str(row.get("definition") or "").strip()
            if not defn:
                continue
            definitions.append(ObjectDefinition(
                schema_name=str(row["schema_name"]),
                object_name=str(row["object_name"]),
                object_kind=_kind_map.get(str(row["kind"]), SchemaObjectKindV2.VIEW),
                definition_text=defn,
                definition_language="sql",
            ))

        for row in routine_rows:
            defn = str(row.get("definition") or "").strip()
            if not defn:
                continue
            lang = str(row.get("language") or "sql").strip() or "sql"
            definitions.append(ObjectDefinition(
                schema_name=str(row["schema_name"]),
                object_name=str(row["object_name"]),
                object_kind=_prokind_map.get(str(row.get("prokind", "f")), SchemaObjectKindV2.UDF),
                definition_text=defn,
                definition_language=lang,
            ))

        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.DEFINITIONS,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE),
            captured_at=now,
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
        """TRAFFIC: wraps v1 observe_traffic() with ExtractionMeta."""
        started_at = time.perf_counter()
        v1_result = await self._observe_traffic(adapter, since=since)
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        now = datetime.now(tz=UTC)

        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.TRAFFIC,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE),
            captured_at=now,
            duration_ms=duration_ms,
            row_count=len(v1_result.events),
        )
        return TrafficExtractionResult(
            meta=meta,
            events=v1_result.events,
            observation_cursor=v1_result.observation_cursor,
        )

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> None:  # type: ignore[override]
        raise NotImplementedError("lineage extraction is not supported by PostgresAdapter")

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> None:  # type: ignore[override]
        raise NotImplementedError("orchestration extraction is not supported by PostgresAdapter")

    def get_setup_instructions(self) -> SetupInstructions:
        return SetupInstructions(
            title="PostgreSQL Source Adapter",
            summary=(
                "Register a PostgreSQL source with a read-only DSN."
                " Traffic observation requires either log file access"
                " or the pg_stat_statements extension."
            ),
            steps=(
                "Create a read-only PostgreSQL role for schema introspection and query validation.",
                "Provide the connection DSN as a managed secret or an environment-variable reference.",
                "Configure include_schemas and exclude_schemas to limit introspection scope (default: public).",
                "For log-based traffic: set log_min_duration_statement=0 and"
                " log_line_prefix='%m [%p] db=%d,user=%u,app=%a,client=%h '"
                " in postgresql.conf, then set log_capture.log_path to the active log file.",
                "For extension-based traffic: run CREATE EXTENSION IF NOT EXISTS pg_stat_statements"
                " and ensure pg_stat_statements.track=all is set in postgresql.conf;"
                " omit log_capture to use this path automatically.",
            ),
        )
