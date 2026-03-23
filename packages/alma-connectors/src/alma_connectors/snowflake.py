"""Snowflake source adapter for alma-connectors.

Connects to Snowflake to discover databases, schemas, tables, views, and
query history from QUERY_HISTORY.

Requires: ``alma-connectors[snowflake]`` (snowflake-connector-python)
"""

from __future__ import annotations

from typing import Any

from alma_connectors.domain import ColumnDef, QueryRecord, SchemaSnapshot, TrafficObservationResult


class SnowflakeAdapter:
    """Source adapter for Snowflake.

    Discovers all tables and views across schemas in a Snowflake account,
    captures column-level schemas via INFORMATION_SCHEMA, and retrieves
    query history from ACCOUNT_USAGE.QUERY_HISTORY.
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str | None = None,
        private_key: bytes | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str = "PUBLIC",
        role: str | None = None,
    ) -> None:
        """Initialize the Snowflake adapter.

        Args:
            account: Snowflake account identifier (e.g. ``xy12345.us-east-1``).
            user: Snowflake username.
            password: Password (mutually exclusive with private_key).
            private_key: RSA private key bytes for key-pair auth.
            warehouse: Warehouse to use for queries.
            database: Default database.
            schema: Default schema. Defaults to PUBLIC.
            role: Role to assume.
        """
        try:
            import snowflake.connector  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "Snowflake support requires snowflake-connector-python. "
                "Install with: pip install alma-connectors[snowflake]"
            ) from e

        connect_kwargs: dict[str, Any] = {"account": account, "user": user}
        if password:
            connect_kwargs["password"] = password
        if private_key:
            connect_kwargs["private_key"] = private_key
        if warehouse:
            connect_kwargs["warehouse"] = warehouse
        if database:
            connect_kwargs["database"] = database
        if role:
            connect_kwargs["role"] = role

        self._account = account
        self._database = database or account
        self._schema = schema
        self._conn = snowflake.connector.connect(**connect_kwargs)

    @property
    def source_id(self) -> str:
        return f"snowflake:{self._account}"

    @property
    def source_type(self) -> str:
        return "snowflake"

    def list_assets(self) -> list[dict]:
        """List all tables and views in the configured database/schema."""
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self._schema}'"
        )
        assets: list[dict] = []
        for row in cursor.fetchall():
            assets.append(
                {
                    "id": f"{self._database}.{self._schema}.{row[0]}",
                    "name": row[0],
                    "kind": "view" if row[1] == "VIEW" else "table",
                    "source": self.source_id,
                    "metadata": {"database": self._database, "schema": self._schema},
                }
            )
        return assets

    def get_schema(self, asset_id: str) -> SchemaSnapshot | None:
        """Retrieve column schema for a Snowflake table."""
        parts = asset_id.split(".")
        table_name = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else self._schema

        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}' "
            f"ORDER BY ORDINAL_POSITION"
        )
        rows = cursor.fetchall()
        if not rows:
            return None

        columns = [ColumnDef(name=r[0], type=r[1], nullable=r[2] == "YES") for r in rows]
        return SchemaSnapshot(asset_id=asset_id, source_type=self.source_type, columns=columns)

    def get_traffic(self) -> TrafficObservationResult:
        """Retrieve recent queries from ACCOUNT_USAGE.QUERY_HISTORY."""
        queries: list[QueryRecord] = []
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT QUERY_TEXT, USER_NAME, START_TIME, EXECUTION_TIME
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                  AND QUERY_TYPE = 'SELECT'
                ORDER BY START_TIME DESC
                LIMIT 10000
                """
            )
            for row in cursor.fetchall():
                queries.append(
                    QueryRecord(
                        sql=row[0],
                        source_type=self.source_type,
                        user=row[1],
                        timestamp=str(row[2]),
                        execution_ms=int(row[3]) if row[3] else None,
                    )
                )
        except Exception as e:
            return TrafficObservationResult(source_id=self.source_id, source_type=self.source_type, error=str(e))

        return TrafficObservationResult(source_id=self.source_id, source_type=self.source_type, queries=queries)
