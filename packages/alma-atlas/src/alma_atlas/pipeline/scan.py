"""Scan pipeline — drives source adapters and writes assets to the store.

Orchestrates one full scan cycle for a registered source:
    1. Instantiate the appropriate SourceAdapter for the source kind.
    2. Call ``list_assets`` to discover all tables / views / models.
    3. Upsert each asset into the Atlas store.
    4. Call ``get_traffic`` to collect recent query observations.
    5. Hand observations to the stitch pipeline for edge derivation.

Returns a ``ScanResult`` summarising what was written.
"""

from __future__ import annotations

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

    adapter = _build_adapter(source)

    with Database(cfg.db_path) as db:  # type: ignore[arg-type]
        repo = AssetRepository(db)

        raw_assets = adapter.list_assets()
        for raw in raw_assets:
            repo.upsert(
                Asset(
                    id=raw["id"],
                    source=source.id,
                    kind=raw.get("kind", "unknown"),
                    name=raw.get("name", raw["id"]),
                    description=raw.get("description"),
                    tags=raw.get("tags", []),
                    metadata={k: v for k, v in raw.items() if k not in {"id", "kind", "name", "description", "tags"}},
                )
            )

        traffic = adapter.get_traffic()
        edge_count = stitch(traffic, db)

    return ScanResult(
        source_id=source.id,
        asset_count=len(raw_assets),
        edge_count=edge_count,
    )


def _build_adapter(source: SourceConfig) -> object:
    """Instantiate the correct SourceAdapter for a given source config.

    Args:
        source: Registered source configuration.

    Returns:
        A SourceAdapter instance.

    Raises:
        ValueError: If the source kind is not recognised.
    """
    kind = source.kind

    if kind == "bigquery":
        from alma_connectors.bigquery import BigQueryAdapter

        return BigQueryAdapter(**source.params)

    if kind == "postgres":
        from alma_connectors.postgres import PostgresAdapter

        return PostgresAdapter(**source.params)

    if kind == "snowflake":
        from alma_connectors.snowflake import SnowflakeAdapter

        return SnowflakeAdapter(**source.params)

    if kind == "dbt":
        from alma_connectors.dbt import DbtAdapter

        return DbtAdapter(**source.params)

    raise ValueError(f"Unknown source kind: {kind!r}. Supported: bigquery, postgres, snowflake, dbt")
