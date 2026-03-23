"""Consumer registry storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class ConsumerReader(Protocol):
    """Read-only access to the consumer registry."""

    def list_consumers(self, target_id: str, limit: int = 100) -> list[dict[str, Any]]: ...

    def search_consumers(
        self, query: str, target_id: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]: ...

    def list_consumer_dependencies(
        self,
        target_id: str,
        consumer_id: str | None = None,
        asset_id: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]: ...

    def get_traffic_based_query_edges(
        self, *, filter_target_id: str | None = None, limit: int = 500
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class ConsumerWriter(Protocol):
    """Write access to the consumer registry."""

    def upsert_consumer(
        self,
        *,
        target_id: str,
        consumer_key: str,
        source_type: str = "unknown",
        user_email: str | None = None,
        dag_id: str | None = None,
        task_id: str | None = None,
        identity_confidence: float = 0.5,
        metadata: dict[str, Any] | None = None,
        seen_at: datetime | None = None,
    ) -> dict[str, Any] | None: ...

    def upsert_consumer_dependency(
        self,
        *,
        target_id: str,
        consumer_id: str | UUID,
        asset_id: str | UUID,
        query_fingerprint: str,
        query_volume: int = 1,
        last_seen: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class ConsumerRepository(ConsumerReader, ConsumerWriter, Protocol):
    """Full consumer storage."""

    ...
