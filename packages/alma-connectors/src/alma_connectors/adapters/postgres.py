"""PostgreSQL source adapter implementation."""

from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
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
    SourceAdapter,
    SourceAdapterCapabilities,
    SourceAdapterKind,
    SourceColumnSchema,
    SourceObjectDependency,
    SourceTableSchema,
    TrafficObservationResult,
)

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
    "UTC": "+00:00",
    "GMT": "+00:00",
    "PST": "-08:00",
    "PDT": "-07:00",
    "MST": "-07:00",
    "MDT": "-06:00",
    "CST": "-06:00",
    "CDT": "-05:00",
    "EST": "-05:00",
    "EDT": "-04:00",
    "CET": "+01:00",
    "CEST": "+02:00",
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
                raise ValueError(f"unsupported postgres log timezone abbreviation: {timezone_suffix}")
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


class PostgresAdapter(SourceAdapter):
    """Runtime PostgreSQL source adapter."""

    kind = SourceAdapterKind.POSTGRES
    capabilities = SourceAdapterCapabilities(
        can_test_connection=True,
        can_introspect_schema=True,
        can_observe_traffic=True,
        can_execute_query=True,
    )

    def __init__(self, *, resolve_secret: Callable[[ManagedSecret | ExternalSecretRef], str]) -> None:
        self._resolve_secret = resolve_secret

    def _resolve_secret_value(self, secret: ManagedSecret | ExternalSecretRef) -> str:
        return self._resolve_secret(secret)

    def _get_config(self, adapter: PersistedSourceAdapter) -> PostgresAdapterConfig:
        if not isinstance(adapter.config, PostgresAdapterConfig):
            raise ValueError(f"adapter '{adapter.key}' is not configured as postgres")
        return adapter.config

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

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
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
        with psycopg.connect(
            dsn,
            row_factory=dict_row,
            options="-c default_transaction_read_only=on",
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

    async def introspect_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshot:
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

        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            rows = list(conn.execute(schema_sql, params + params).fetchall())
            dependency_rows = list(conn.execute(dependency_sql, include_schemas).fetchall()) if include_schemas else []
            stats_rows = list(conn.execute(stats_sql, stats_params).fetchall())
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

    async def observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        config = self._get_config(adapter)
        if config.log_capture is not None:
            return self._observe_from_logs(adapter, config.log_capture, since=since)
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
        events: list[ObservedQueryEvent] = []
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
        try:
            with psycopg.connect(
                dsn,
                row_factory=dict_row,
                options="-c default_transaction_read_only=on",
            ) as conn:
                rows = list(conn.execute(_PG_STAT_STATEMENTS_SQL).fetchall())
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
        with psycopg.connect(
            dsn,
            row_factory=dict_row,
            options="-c default_transaction_read_only=on -c statement_timeout=30000",
        ) as conn:
            result = conn.execute(sql)
            rows = list(result.fetchmany(row_limit + 1))
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
