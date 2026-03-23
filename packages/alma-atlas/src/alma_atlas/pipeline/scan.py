"""Scan pipeline — drives source adapters and writes assets to the store.

Orchestrates one full scan cycle for a registered source:
    1. Instantiate the appropriate SourceAdapter for the source kind.
    2. Build a PersistedSourceAdapter record from the source config.
    3. Call ``introspect_schema`` to discover all tables / views / models.
    4. Upsert each asset into the Atlas store.
    5. Call ``observe_traffic`` to collect recent query observations.
    6. Hand observations to the stitch pipeline for edge derivation.

Returns a ``ScanResult`` summarising what was written.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field

from alma_atlas.config import AtlasConfig, SourceConfig


@dataclass
class ScanResult:
    """Summary of a completed scan for one source."""

    source_id: str
    asset_count: int = 0
    edge_count: int = 0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)


def run_scan(source: SourceConfig, cfg: AtlasConfig) -> ScanResult:
    """Run a full scan for a single registered source.

    Args:
        source: The source configuration (kind, id, params).
        cfg:    Atlas configuration (used to open the SQLite store).

    Returns:
        A ScanResult summarising assets written and edges derived.
    """
    from alma_atlas.pipeline.stitch import stitch
    from alma_atlas_store.asset_repository import Asset, AssetRepository
    from alma_atlas_store.db import Database

    try:
        adapter, persisted = _build_adapter(source)
    except (ValueError, ImportError) as exc:
        return ScanResult(source_id=source.id, error=str(exc))

    try:
        snapshot = asyncio.run(adapter.introspect_schema(persisted))
    except Exception as exc:
        return ScanResult(source_id=source.id, error=f"Schema introspection failed: {exc}")

    with Database(cfg.db_path) as db:  # type: ignore[arg-type]
        repo = AssetRepository(db)

        for obj in snapshot.objects:
            asset_id = f"{source.id}::{obj.schema_name}.{obj.object_name}"
            repo.upsert(
                Asset(
                    id=asset_id,
                    source=source.id,
                    kind=obj.object_kind.value,
                    name=f"{obj.schema_name}.{obj.object_name}",
                )
            )

        try:
            traffic = asyncio.run(adapter.observe_traffic(persisted))
        except Exception as exc:
            return ScanResult(
                source_id=source.id,
                asset_count=len(snapshot.objects),
                warnings=[f"Traffic observation failed: {exc}"],
            )

        edge_count = stitch(traffic, db, source_id=source.id, source_kind=source.kind)

    return ScanResult(
        source_id=source.id,
        asset_count=len(snapshot.objects),
        edge_count=edge_count,
    )


def _build_adapter(source: SourceConfig):  # type: ignore[return]
    """Instantiate the correct SourceAdapter and a synthetic PersistedSourceAdapter.

    Builds a minimal PersistedSourceAdapter from the source config so that the
    CLI can drive the connector adapters without a full service layer.

    Args:
        source: Registered source configuration.

    Returns:
        Tuple of (adapter_instance, persisted_adapter).

    Raises:
        ValueError: If the source kind is not recognised.
    """
    import os

    from alma_connectors.source_adapter import (
        ExternalSecretRef,
        PersistedSourceAdapter,
        SourceAdapterStatus,
    )

    adapter_id = str(uuid.uuid5(uuid.NAMESPACE_URL, source.id))

    def _resolve_env(secret: object) -> str:
        ref = getattr(secret, "reference", None)
        return os.environ.get(ref, "") if ref else ""

    kind = source.kind

    if kind == "bigquery":
        from alma_connectors.adapters.bigquery import BigQueryAdapter
        from alma_connectors.source_adapter import BigQueryAdapterConfig, SourceAdapterKind

        config = BigQueryAdapterConfig(
            service_account_secret=ExternalSecretRef(
                provider="env",
                reference=source.params.get("service_account_env", "BQ_SERVICE_ACCOUNT_JSON"),
            ),
            project_id=source.params["project_id"],
            location=source.params.get("location", "us"),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=source.id,
            display_name=source.id,
            kind=SourceAdapterKind.BIGQUERY,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
        )
        return BigQueryAdapter(resolve_secret=_resolve_env), persisted

    if kind == "postgres":
        from alma_connectors.adapters.postgres import PostgresAdapter
        from alma_connectors.source_adapter import PostgresAdapterConfig, SourceAdapterKind

        config = PostgresAdapterConfig(
            database_secret=ExternalSecretRef(
                provider="env",
                reference=source.params.get("dsn_env", "PG_DATABASE_URL"),
            ),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=source.id,
            display_name=source.id,
            kind=SourceAdapterKind.POSTGRES,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
        )
        return PostgresAdapter(resolve_secret=_resolve_env), persisted

    raise ValueError(f"Unknown source kind: {kind!r}. Supported: bigquery, postgres")
