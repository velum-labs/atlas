"""SQLite source adapter implementation."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

from alma_connectors.adapters._base import BaseAdapterV2
from alma_connectors.source_adapter import (
    PersistedSourceAdapter,
    QueryResult,
    SetupInstructions,
    SourceAdapterCapabilities,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionMeta,
    ExtractionScope,
    ScopeContext,
    SourceAdapterKindV2,
)
from alma_connectors.source_adapter_v2 import ColumnSchema as ColumnSchemaV2
from alma_connectors.source_adapter_v2 import ObjectDependency as ObjectDependencyV2
from alma_connectors.source_adapter_v2 import SchemaObject
from alma_connectors.source_adapter_v2 import SchemaObjectKind as SchemaObjectKindV2
from alma_connectors.source_adapter_v2 import SchemaSnapshotV2

_DEFAULT_QUERY_ROW_LIMIT = 100
_SQLITE_SCHEMA_NAME = "_default"
_USER_OBJECTS_SQL = """
SELECT name, type
FROM sqlite_master
WHERE type IN ('table', 'view')
  AND name NOT LIKE 'sqlite_%'
ORDER BY type, name
"""


def _quote_identifier(identifier: str) -> str:
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'


def _row_to_dict(row: sqlite3.Row) -> dict[str, object]:
    return {key: row[key] for key in row.keys()}


class SQLiteAdapter(BaseAdapterV2):
    """Read-only adapter for SQLite database files."""

    kind = SourceAdapterKindV2.SQLITE
    capabilities = SourceAdapterCapabilities(
        can_test_connection=True,
        can_introspect_schema=True,
        can_observe_traffic=False,
        can_execute_query=True,
    )
    declared_capabilities = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.SCHEMA,
    })

    def __init__(self, *, db_path: str | Path) -> None:
        self._db_path = Path(db_path).expanduser().resolve()

    def _scope_identifiers(self) -> dict[str, str]:
        return {"path": str(self._db_path)}

    def _scope_context(self) -> ScopeContext:
        return ScopeContext(
            scope=ExtractionScope.DATABASE,
            identifiers=self._scope_identifiers(),
        )

    def _make_meta(
        self,
        adapter: PersistedSourceAdapter,
        capability: AdapterCapability,
        row_count: int,
        duration_ms: float,
    ) -> ExtractionMeta:
        return ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=self.kind,
            capability=capability,
            scope_context=self._scope_context(),
            captured_at=datetime.now(UTC),
            duration_ms=duration_ms,
            row_count=row_count,
        )

    def _make_probe_results(
        self,
        caps_to_probe: frozenset[AdapterCapability],
        available: bool,
        scope_ctx: ScopeContext,
        message: str | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        return tuple(
            CapabilityProbeResult(
                capability=capability,
                available=available,
                scope=scope_ctx.scope,
                scope_context=scope_ctx,
                message=message,
            )
            for capability in caps_to_probe
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"{self._db_path.as_uri()}?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        return connection

    def _load_user_objects(self) -> list[sqlite3.Row]:
        with self._connect() as connection:
            cursor = connection.execute(_USER_OBJECTS_SQL)
            return list(cursor.fetchall())

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities
        scope_context = self._scope_context()

        if not self._db_path.is_file():
            return self._make_probe_results(
                caps_to_probe,
                available=False,
                scope_ctx=scope_context,
                message=f"File not found: {self._db_path}",
            )

        def _probe_database() -> None:
            with self._connect() as connection:
                connection.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()

        try:
            await asyncio.to_thread(_probe_database)
        except sqlite3.Error as exc:
            return self._make_probe_results(
                caps_to_probe,
                available=False,
                scope_ctx=scope_context,
                message=f"Not a valid SQLite database: {exc}",
            )

        return self._make_probe_results(
            caps_to_probe,
            available=True,
            scope_ctx=scope_context,
        )

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        started_at = time.perf_counter()
        rows = await asyncio.to_thread(self._load_user_objects)
        containers = tuple(
            DiscoveredContainer(
                container_id=f"{adapter.key}/{row['name']}",
                container_type=str(row["type"]),
                display_name=str(row["name"]),
                metadata={"schema_name": _SQLITE_SCHEMA_NAME},
            )
            for row in rows
        )
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        return DiscoverySnapshot(
            meta=self._make_meta(
                adapter,
                AdapterCapability.DISCOVER,
                len(containers),
                duration_ms,
            ),
            containers=containers,
        )

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        started_at = time.perf_counter()

        def _extract_schema_sync() -> tuple[tuple[SchemaObject, ...], tuple[ObjectDependencyV2, ...]]:
            objects: list[SchemaObject] = []
            dependency_keys: set[tuple[str, str, str, str]] = set()

            with self._connect() as connection:
                object_rows = connection.execute(_USER_OBJECTS_SQL).fetchall()
                for row in object_rows:
                    object_name = str(row["name"])
                    object_type = str(row["type"])
                    quoted_object = _quote_identifier(object_name)

                    column_rows = connection.execute(
                        f"PRAGMA table_info({quoted_object})"
                    ).fetchall()
                    columns = tuple(
                        ColumnSchemaV2(
                            name=str(column["name"]),
                            data_type=(str(column["type"]).strip() or "ANY"),
                            is_nullable=not bool(column["notnull"]),
                        )
                        for column in column_rows
                    )

                    row_count_result = connection.execute(
                        f"SELECT COUNT(*) AS row_count FROM {quoted_object}"
                    ).fetchone()
                    row_count = int(row_count_result["row_count"]) if row_count_result is not None else 0

                    objects.append(
                        SchemaObject(
                            schema_name=_SQLITE_SCHEMA_NAME,
                            object_name=object_name,
                            kind=(
                                SchemaObjectKindV2.VIEW
                                if object_type == "view"
                                else SchemaObjectKindV2.TABLE
                            ),
                            columns=columns,
                            row_count=row_count,
                        )
                    )

                    for dependency_row in connection.execute(
                        f"PRAGMA foreign_key_list({quoted_object})"
                    ).fetchall():
                        dependency_keys.add(
                            (
                                _SQLITE_SCHEMA_NAME,
                                object_name,
                                _SQLITE_SCHEMA_NAME,
                                str(dependency_row["table"]),
                            )
                        )

            dependencies = tuple(
                ObjectDependencyV2(
                    source_schema=source_schema,
                    source_object=source_object,
                    target_schema=target_schema,
                    target_object=target_object,
                )
                for source_schema, source_object, target_schema, target_object in sorted(dependency_keys)
            )
            return tuple(objects), dependencies

        objects, dependencies = await asyncio.to_thread(_extract_schema_sync)
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        return SchemaSnapshotV2(
            meta=self._make_meta(
                adapter,
                AdapterCapability.SCHEMA,
                len(objects),
                duration_ms,
            ),
            objects=objects,
            dependencies=dependencies,
        )

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
        dry_run: bool = False,
    ) -> QueryResult:
        del adapter, probe_target, dry_run
        row_limit = max_rows if max_rows and max_rows > 0 else _DEFAULT_QUERY_ROW_LIMIT
        started_at = time.perf_counter()

        def _run_query() -> tuple[tuple[dict[str, object], ...], bool]:
            with self._connect() as connection:
                cursor = connection.execute(sql)
                rows = cursor.fetchmany(row_limit + 1)
                truncated = len(rows) > row_limit
                visible_rows = rows[:row_limit]
                return tuple(_row_to_dict(row) for row in visible_rows), truncated

        try:
            rows, truncated = await asyncio.to_thread(_run_query)
        except sqlite3.Error as exc:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            return QueryResult(
                success=False,
                row_count=0,
                duration_ms=duration_ms,
                error_message=str(exc),
            )

        duration_ms = (time.perf_counter() - started_at) * 1000.0
        return QueryResult(
            success=True,
            row_count=len(rows),
            duration_ms=duration_ms,
            rows=rows,
            truncated=truncated,
        )

    def get_setup_instructions(self) -> SetupInstructions:
        return SetupInstructions(
            title="SQLite Source Adapter Setup",
            summary=(
                "Register a local SQLite database file so Atlas can discover tables, "
                "views, and foreign-key relationships using read-only connections."
            ),
            steps=(
                "Point Atlas at an existing .sqlite or .db file with alma-atlas connect sqlite --path /path/to/database.sqlite.",
                "Use alma-atlas connect sqlite --dir /path/to/databases --glob '*.sqlite' to register multiple databases at once.",
                "Atlas opens SQLite databases in read-only mode, so write statements are rejected automatically.",
            ),
        )
