"""PostgreSQL source adapter for alma-connectors.

Connects to a PostgreSQL database to discover tables, views, and
query traffic from pg_stat_statements (if enabled).

Requires: ``alma-connectors[postgres]`` (psycopg)
"""

from __future__ import annotations

from alma_connectors.domain import ColumnDef, QueryRecord, SchemaSnapshot, TrafficObservationResult


class PostgresAdapter:
    """Source adapter for PostgreSQL.

    Discovers all tables and views in the public schema (configurable),
    captures column-level schemas, and retrieves query traffic from
    pg_stat_statements where available.
    """

    def __init__(
        self,
        dsn: str,
        schema: str = "public",
        database: str | None = None,
    ) -> None:
        """Initialize the Postgres adapter.

        Args:
            dsn: PostgreSQL connection string (e.g. ``postgresql://user:pass@host/db``).
            schema: Schema to scan. Defaults to ``public``.
            database: Database name override. Usually parsed from dsn.
        """
        try:
            import psycopg  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "PostgreSQL support requires psycopg. Install with: pip install alma-connectors[postgres]"
            ) from e

        self._dsn = dsn
        self._schema = schema
        self._database = database or dsn.rsplit("/", 1)[-1].split("?")[0]
        self._conn = psycopg.connect(dsn)

    @property
    def source_id(self) -> str:
        return f"postgres:{self._database}"

    @property
    def source_type(self) -> str:
        return "postgres"

    def list_assets(self) -> list[dict]:
        """List all tables and views in the configured schema."""
        assets: list[dict] = []
        rows = self._conn.execute(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (self._schema,),
        ).fetchall()
        for row in rows:
            assets.append(
                {
                    "id": f"{self._database}.{self._schema}.{row[0]}",
                    "name": row[0],
                    "kind": "view" if row[1] == "VIEW" else "table",
                    "source": self.source_id,
                    "metadata": {"schema": self._schema, "database": self._database},
                }
            )
        return assets

    def get_schema(self, asset_id: str) -> SchemaSnapshot | None:
        """Retrieve column schema for a table (expects database.schema.table format)."""
        parts = asset_id.split(".")
        table_name = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else self._schema

        rows = self._conn.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table_name),
        ).fetchall()

        if not rows:
            return None

        columns = [ColumnDef(name=r[0], type=r[1], nullable=r[2] == "YES") for r in rows]
        return SchemaSnapshot(asset_id=asset_id, source_type=self.source_type, columns=columns)

    def get_traffic(self) -> TrafficObservationResult:
        """Retrieve query stats from pg_stat_statements."""
        queries: list[QueryRecord] = []
        try:
            rows = self._conn.execute(
                """
                SELECT query, calls, total_exec_time
                FROM pg_stat_statements
                ORDER BY calls DESC
                LIMIT 5000
                """
            ).fetchall()
            for row in rows:
                queries.append(
                    QueryRecord(
                        sql=row[0],
                        source_type=self.source_type,
                        execution_ms=int(row[2] / max(row[1], 1)),
                    )
                )
        except Exception as e:
            return TrafficObservationResult(source_id=self.source_id, source_type=self.source_type, error=str(e))

        return TrafficObservationResult(source_id=self.source_id, source_type=self.source_type, queries=queries)
