"""Source schema and drift storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class SchemaReader(Protocol):
    """Read-only access to source schemas and drift events."""

    def get_latest_source_snapshot_timestamps(
        self, *, source_name: str, target_id: str | None = None, limit: int = 2
    ) -> list[datetime]: ...

    def get_source_snapshot_rows(
        self, *, source_name: str, captured_at: datetime, target_id: str | None = None
    ) -> list[dict[str, Any]]: ...

    def get_table_types(
        self, *, source_name: str | None = None, target_id: str | None = None
    ) -> dict[str, str]: ...

    def get_schema_columns_for_table(
        self,
        *,
        schema_name: str,
        table_name: str,
        source_name: str | None = None,
        target_id: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def list_schema_drift_events(
        self,
        *,
        target_id: str | None = None,
        drift_scope: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]: ...

    def list_view_dependencies(
        self, *, target_id: str | None = None
    ) -> list[dict[str, Any]]: ...

    def get_contracts_for_table(
        self,
        *,
        schema_name: str,
        table_name: str,
    ) -> list[str]: ...


@runtime_checkable
class SchemaWriter(Protocol):
    """Write access to source schemas and drift events."""

    def insert_source_schema_snapshot_rows(
        self,
        *,
        target_id: str,
        source_name: str,
        rows: list[dict[str, Any]],
        captured_at: datetime | None = None,
        source_adapter_id: str | UUID | None = None,
    ) -> int: ...

    def insert_schema_drift_event(
        self,
        *,
        target_id: str,
        drift_scope: str,
        table_name: str,
        column_name: str,
        change_type: str,
        previous_value: dict[str, Any] | None = None,
        current_value: dict[str, Any] | None = None,
        status: str = "open",
        source_adapter_id: str | UUID | None = None,
        severity: str | None = None,
        affected_contract_ids: list[str] | None = None,
    ) -> UUID: ...

    def upsert_view_dependency(
        self,
        *,
        target_id: str,
        view_schema: str,
        view_name: str,
        view_kind: str,
        depends_on_schema: str,
        depends_on_name: str,
    ) -> None: ...


@runtime_checkable
class SchemaRepository(SchemaReader, SchemaWriter, Protocol):
    """Full schema storage."""

    ...
