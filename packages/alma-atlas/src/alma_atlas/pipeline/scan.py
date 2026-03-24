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
from typing import TYPE_CHECKING

from alma_atlas.config import AtlasConfig, SourceConfig

if TYPE_CHECKING:
    from alma_connectors.source_adapter import SchemaSnapshot


@dataclass
class ScanResult:
    """Summary of a completed scan for one source."""

    source_id: str
    asset_count: int = 0
    edge_count: int = 0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    snapshot: SchemaSnapshot | None = None


@dataclass
class ScanAllResult:
    """Summary of a completed multi-source scan including cross-system edges."""

    results: list[ScanResult] = field(default_factory=list)
    cross_system_edge_count: int = 0


async def run_scan_async(source: SourceConfig, cfg: AtlasConfig) -> ScanResult:
    """Run a full scan for a single registered source (async implementation).

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
        snapshot = await adapter.introspect_schema(persisted)
    except Exception as exc:
        return ScanResult(source_id=source.id, error=f"Schema introspection failed: {exc}")

    with Database(cfg.db_path) as db:  # type: ignore[arg-type]
        repo = AssetRepository(db)

        asset_id_map: dict[tuple[str, str], str] = {}
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
            asset_id_map[(obj.schema_name, obj.object_name)] = asset_id

        # Persist schema-level dependency edges (e.g. from dbt manifest lineage).
        dep_edge_count = 0
        if snapshot.dependencies:
            from alma_atlas_store.edge_repository import Edge, EdgeRepository

            edge_repo = EdgeRepository(db)
            for dep in snapshot.dependencies:
                upstream_id = asset_id_map.get((dep.target_schema, dep.target_object))
                downstream_id = asset_id_map.get((dep.source_schema, dep.source_object))
                if upstream_id and downstream_id:
                    edge_repo.upsert(
                        Edge(
                            upstream_id=upstream_id,
                            downstream_id=downstream_id,
                            kind="depends_on",
                        )
                    )
                    dep_edge_count += 1

        try:
            traffic = await adapter.observe_traffic(persisted)
        except Exception as exc:
            return ScanResult(
                source_id=source.id,
                asset_count=len(snapshot.objects),
                edge_count=dep_edge_count,
                warnings=[f"Traffic observation failed: {exc}"],
                snapshot=snapshot,
            )

        edge_count = stitch(traffic, db, source_id=source.id, source_kind=source.kind) + dep_edge_count

        _run_enforcement(snapshot, source.id, db)

    return ScanResult(
        source_id=source.id,
        asset_count=len(snapshot.objects),
        edge_count=edge_count,
        snapshot=snapshot,
    )


def run_scan(source: SourceConfig, cfg: AtlasConfig) -> ScanResult:
    """Sync entry point for a full scan — safe from both sync and async contexts.

    Args:
        source: The source configuration (kind, id, params).
        cfg:    Atlas configuration (used to open the SQLite store).

    Returns:
        A ScanResult summarising assets written and edges derived.
    """
    return asyncio.run(run_scan_async(source, cfg))


def _run_enforcement(snapshot: SchemaSnapshot, source_id: str, db: object) -> None:
    """Run drift detection + enforcement for any assets that have contracts.

    Silently skips assets without contracts.  Enforcement violations are
    always persisted to the store; the mode on each contract controls whether
    the result is merely logged (shadow), surfaced (warn), or blocking
    (enforce — currently logged but not raising, as the pipeline caller is
    responsible for acting on ScanResult.warnings).

    Args:
        snapshot:  Schema snapshot from the adapter.
        source_id: Source identifier for asset ID construction.
        db:        Open Database connection (shared from the caller).
    """
    import logging

    from alma_atlas.enforcement.drift import DriftDetector
    from alma_atlas.enforcement.engine import EnforcementEngine
    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository
    from alma_atlas_store.schema_repository import SchemaSnapshot as StoreSnapshot

    log = logging.getLogger(__name__)
    detector = DriftDetector()

    contract_repo = ContractRepository(db)
    schema_repo = SchemaRepository(db)
    engine = EnforcementEngine(db)

    for obj in snapshot.objects:
        asset_id = f"{source_id}::{obj.schema_name}.{obj.object_name}"
        contracts = contract_repo.list_for_asset(asset_id)
        if not contracts:
            continue

        previous = schema_repo.get_latest(asset_id)
        # Build current StoreSnapshot from the connector SchemaSnapshot object.
        current_cols = [
            ColumnInfo(
                name=c.name,
                type=getattr(c, "data_type", None) or getattr(c, "type", "unknown"),
                nullable=getattr(c, "nullable", True),
            )
            for c in (obj.columns or [])
        ]
        current = StoreSnapshot(asset_id=asset_id, columns=current_cols)

        report = detector.detect(asset_id, previous, current)
        if not report.has_violations:
            continue

        for contract in contracts:
            result = engine.enforce(report, contract.mode)
            if result.blocked:
                log.warning(
                    "[enforcement/enforce] Pipeline BLOCKED for asset %s — "
                    "%d error violation(s) detected.",
                    asset_id,
                    report.error_count,
                )


def run_scan_all(sources: list[SourceConfig], cfg: AtlasConfig) -> ScanAllResult:
    """Run scans for all sources, then discover cross-system edges.

    Calls :func:`run_scan` for each source in order, collects the resulting
    :class:`ScanResult` objects and their schema snapshots, then hands all
    snapshots to :func:`~alma_atlas.pipeline.cross_system_edges.discover_cross_system_edges`
    to find edges that span system boundaries.

    Args:
        sources: List of source configurations to scan.
        cfg:     Atlas configuration (used to open the SQLite store).

    Returns:
        A :class:`ScanAllResult` aggregating per-source results and the total
        number of cross-system edges discovered.
    """
    from alma_atlas.pipeline.cross_system_edges import discover_cross_system_edges
    from alma_atlas_store.db import Database

    results: list[ScanResult] = []
    snapshots: dict[str, SchemaSnapshot] = {}

    for source in sources:
        result = run_scan(source, cfg)
        results.append(result)
        if result.snapshot is not None:
            snapshots[source.id] = result.snapshot

    cross_system_edge_count = 0
    if len(snapshots) >= 2:
        with Database(cfg.db_path) as db:  # type: ignore[arg-type]
            cross_system_edge_count = discover_cross_system_edges(snapshots, db)

    return ScanAllResult(results=results, cross_system_edge_count=cross_system_edge_count)


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
        SourceAdapterSecret,
        SourceAdapterStatus,
    )

    adapter_id = str(uuid.uuid5(uuid.NAMESPACE_URL, source.id))
    # PersistedSourceAdapter.key must match ^[a-z0-9][a-z0-9_-]*$ — sanitize colons.
    adapter_key = source.id.replace(":", "-")

    def _resolve_env(secret: object) -> str:
        provider = getattr(secret, "provider", "env")
        ref = getattr(secret, "reference", None)
        if provider == "literal":
            return ref or ""
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
            project_id=source.params.get("project_id") or source.params["project"],
            location=source.params.get("location", "us"),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
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

        # Support direct DSN from params or env-var reference
        if "dsn" in source.params:
            db_secret: SourceAdapterSecret = ExternalSecretRef(
                provider="literal",
                reference=source.params["dsn"],
            )
        else:
            db_secret = ExternalSecretRef(
                provider="env",
                reference=source.params.get("dsn_env", "PG_DATABASE_URL"),
            )

        # Pass through schema filter if provided
        include_schemas = (
            (source.params["schema"],) if "schema" in source.params else ("public",)
        )

        config = PostgresAdapterConfig(
            database_secret=db_secret,
            include_schemas=include_schemas,
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.POSTGRES,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
        )
        return PostgresAdapter(resolve_secret=_resolve_env), persisted

    if kind == "dbt":
        from alma_connectors.adapters.dbt import DbtAdapter
        from alma_connectors.source_adapter import DbtAdapterConfig, SourceAdapterKind

        manifest_path = source.params.get("manifest_path", "")
        catalog_path = source.params.get("catalog_path")
        run_results_path = source.params.get("run_results_path")
        project_name = source.params.get("project_name")
        config = DbtAdapterConfig(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            run_results_path=run_results_path,
            project_name=project_name,
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.DBT,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
        )
        return DbtAdapter(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            run_results_path=run_results_path,
            project_name=project_name,
        ), persisted

    if kind == "snowflake":
        from alma_connectors.adapters.snowflake import SnowflakeAdapter
        from alma_connectors.source_adapter import SnowflakeAdapterConfig, SourceAdapterKind

        config = SnowflakeAdapterConfig(
            account_secret=ExternalSecretRef(
                provider="env",
                reference=source.params.get("account_secret_env", "SNOWFLAKE_CONNECTION_JSON"),
            ),
            account=source.params.get("account", ""),
            warehouse=source.params.get("warehouse", "COMPUTE_WH"),
            database=source.params.get("database", ""),
            role=source.params.get("role", ""),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKind.SNOWFLAKE,
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=config,
        )
        return SnowflakeAdapter(resolve_secret=_resolve_env), persisted

    if kind == "airflow":
        from alma_connectors.adapters.airflow import AirflowAdapter
        from alma_connectors.source_adapter_v2 import SourceAdapterKindV2

        auth_token: str | None = source.params.get("auth_token") or (
            os.environ.get(source.params["auth_token_env"])
            if "auth_token_env" in source.params
            else os.environ.get("AIRFLOW_AUTH_TOKEN") or None
        )
        adapter = AirflowAdapter(
            base_url=source.params.get("base_url", ""),
            auth_token=auth_token,
            username=source.params.get("username"),
            password=source.params.get("password"),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKindV2.AIRFLOW,  # type: ignore[arg-type]
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=None,  # type: ignore[arg-type]
        )
        return adapter, persisted

    if kind == "looker":
        from alma_connectors.adapters.looker import LookerAdapter
        from alma_connectors.source_adapter_v2 import SourceAdapterKindV2

        client_id = source.params.get("client_id") or os.environ.get(
            source.params.get("client_id_env", "LOOKER_CLIENT_ID"), ""
        )
        client_secret = source.params.get("client_secret") or os.environ.get(
            source.params.get("client_secret_env", "LOOKER_CLIENT_SECRET"), ""
        )
        adapter = LookerAdapter(
            instance_url=source.params.get("instance_url", ""),
            client_id=client_id,
            client_secret=client_secret,
            port=int(source.params.get("port", 19999)),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKindV2.LOOKER,  # type: ignore[arg-type]
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=None,  # type: ignore[arg-type]
        )
        return adapter, persisted

    if kind == "fivetran":
        from alma_connectors.adapters.fivetran import FivetranAdapter
        from alma_connectors.source_adapter_v2 import SourceAdapterKindV2

        api_key = source.params.get("api_key") or os.environ.get(
            source.params.get("api_key_env", "FIVETRAN_API_KEY"), ""
        )
        api_secret = source.params.get("api_secret") or os.environ.get(
            source.params.get("api_secret_env", "FIVETRAN_API_SECRET"), ""
        )
        adapter = FivetranAdapter(
            api_key=api_key,
            api_secret=api_secret,
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKindV2.FIVETRAN,  # type: ignore[arg-type]
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=None,  # type: ignore[arg-type]
        )
        return adapter, persisted

    if kind == "metabase":
        from alma_connectors.adapters.metabase import MetabaseAdapter
        from alma_connectors.source_adapter_v2 import SourceAdapterKindV2

        api_key_mb: str | None = source.params.get("api_key") or os.environ.get(
            source.params.get("api_key_env", "METABASE_API_KEY")
        ) or None
        adapter = MetabaseAdapter(
            instance_url=source.params.get("instance_url", ""),
            api_key=api_key_mb,
            username=source.params.get("username"),
            password=source.params.get("password"),
        )
        persisted = PersistedSourceAdapter(
            id=adapter_id,
            key=adapter_key,
            display_name=source.id,
            kind=SourceAdapterKindV2.METABASE,  # type: ignore[arg-type]
            target_id=source.id,
            status=SourceAdapterStatus.READY,
            config=None,  # type: ignore[arg-type]
        )
        return adapter, persisted

    _SUPPORTED_KINDS = {"airflow", "bigquery", "dbt", "fivetran", "looker", "metabase", "postgres", "snowflake"}
    raise ValueError(f"Unknown source kind: {kind!r}. Supported: {', '.join(sorted(_SUPPORTED_KINDS))}")
