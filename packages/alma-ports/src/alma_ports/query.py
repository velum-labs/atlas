"""Query-event storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class QueryReader(Protocol):
    """Read-only access to query events and traffic stats."""

    def get_query_events(
        self,
        *,
        source: str | None = None,
        source_adapter_id: str | None = None,
        target_id: str | None = None,
        backend_system: str | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]: ...

    def get_query_event(self, query_event_id: str | UUID) -> dict[str, Any] | None: ...

    def get_query_event_by_source_and_hash(
        self,
        source: str,
        signature_hash: str,
        target_id: str | None = None,
        backend_system: str | None = None,
    ) -> dict[str, Any] | None: ...

    def get_nontrivial_query_events(
        self,
        *,
        source_adapter_id: str | None = None,
        target_id: str | None = None,
        backend_system: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_traffic_stats(
        self,
        *,
        source: str | None = None,
        target_id: str | None = None,
        backend_system: str | None = None,
        since: datetime | None = None,
    ) -> dict[str, Any]: ...


@runtime_checkable
class QueryWriter(Protocol):
    """Write access to query events."""

    def insert_query_event(
        self,
        *,
        id: UUID | None = None,
        source_adapter_id: UUID | str | None = None,
        target_id: str | None = None,
        capture_source_id: str,
        capture_source_kind: str,
        backend_system: str,
        source: str,
        database_name: str | None = None,
        database_user: str | None = None,
        client_addr: str | None = None,
        query_type: str | None = None,
        statement_id: str | None = None,
        error_message: str | None = None,
        sql_text: str,
        sql_hash: str,
        duration_ms: float | None = None,
        row_count: int | None = None,
        captured_at: datetime | None = None,
        signature_hash: str | None = None,
        cluster_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> UUID: ...


@runtime_checkable
class QueryRepository(QueryReader, QueryWriter, Protocol):
    """Full query-event storage."""

    ...
