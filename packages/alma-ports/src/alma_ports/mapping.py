"""Semantic and target mapping storage protocols."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class MappingReader(Protocol):
    """Read-only access to semantic and target mappings."""

    def get_mappings(
        self, *, status: str | None = None, table: str | None = None
    ) -> list[dict[str, Any]]: ...

    def list_target_mappings(self, *, env: str | None = None) -> list[dict[str, Any]]: ...


@runtime_checkable
class MappingWriter(Protocol):
    """Write access to semantic and target mappings."""

    def update_mapping_status(self, id: UUID, status: str) -> dict[str, Any] | None: ...

    def upsert_target_mapping(
        self,
        *,
        target_id: str,
        env: str,
        country: str,
        bq_project: str,
        bq_location: str = "us",
        bq_raw_dataset: str | None = None,
        bq_curated_dataset: str | None = None,
        gcs_bucket: str | None = None,
        dbt_project: str | None = None,
        source_naming_rule: str | None = None,
        destination_naming_rule: str | None = None,
        source_secret_names: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None: ...

    def upsert_semantic_mapping(
        self,
        *,
        physical_table: str,
        physical_column: str | None,
        concept: str,
        attribute: str | None,
        confidence: float,
        evidence: list[dict[str, Any]],
        proposed_by: str,
    ) -> UUID: ...


@runtime_checkable
class MappingRepository(MappingReader, MappingWriter, Protocol):
    """Full mapping storage."""

    ...
