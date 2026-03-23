"""Asset and consumer registry storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class AssetReader(Protocol):
    """Read-only access to the asset registry."""

    def list_assets(
        self,
        *,
        layer: str | None = None,
        owner: str | None = None,
        contract_status: str | None = None,
        target_id: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]: ...

    def get_asset(self, asset_id: str | UUID) -> dict[str, Any] | None: ...

    def get_asset_by_canonical_name(self, canonical_name: str) -> dict[str, Any] | None: ...

    def list_asset_physical_names(self, asset_id: str | UUID) -> list[dict[str, Any]]: ...

    def list_asset_physical_names_batch(
        self, asset_ids: list[str]
    ) -> dict[str, list[dict[str, Any]]]: ...

    def list_asset_columns(self, asset_id: str | UUID) -> list[dict[str, Any]]: ...

    def search_assets(self, query: str, limit: int = 20) -> list[dict[str, Any]]: ...

    def get_consumers_for_asset(
        self, asset_id: str, target_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]: ...

    def get_assets_for_consumer(
        self, consumer_key: str, target_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class AssetWriter(Protocol):
    """Write access to the asset registry."""

    def upsert_asset(
        self,
        *,
        canonical_name: str,
        layer: str = "raw",
        owner: str | None = None,
        contract_status: str = "unobserved",
        copy_strategy: str | None = None,
        quality_flags: list[str] | None = None,
        source_provenance: str = "inferred",
        row_count: int | None = None,
        column_count: int | None = None,
        size_bytes: int | None = None,
        last_data_updated_at: datetime | None = None,
    ) -> dict[str, Any] | None: ...

    def upsert_asset_physical_name(
        self,
        *,
        asset_id: str | UUID,
        target_id: str,
        system: str,
        physical_name: str,
    ) -> dict[str, Any] | None: ...

    def upsert_asset_column(
        self,
        *,
        asset_id: str | UUID,
        column_name: str,
        data_type: str,
        is_nullable: bool,
        ordinal_position: int | None = None,
        description: str | None = None,
        source_provenance: str = "information_schema",
    ) -> dict[str, Any] | None: ...

    def upsert_lineage_edge(
        self,
        *,
        target_id: str,
        upstream_asset_id: str | UUID,
        downstream_asset_id: str | UUID,
        edge_type: str = "observed",
        confidence: float = 1.0,
        provenance: dict[str, Any] | None = None,
        observed_at: datetime | None = None,
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class AssetRepository(AssetReader, AssetWriter, Protocol):
    """Full asset storage."""

    ...
