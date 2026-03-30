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
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from alma_atlas.config import AtlasConfig, SourceConfig
from alma_ports.errors import ConfigurationError, ExtractionError

if TYPE_CHECKING:
    from alma_connectors.source_adapter import SchemaSnapshot

logger = logging.getLogger(__name__)

# Default per-source scan timeout in seconds.
_DEFAULT_SCAN_TIMEOUT = 300

# Default maximum number of sources scanned concurrently.
_DEFAULT_MAX_CONCURRENT = 4


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


async def run_scan_async(
    source: SourceConfig,
    cfg: AtlasConfig,
    *,
    timeout: float = _DEFAULT_SCAN_TIMEOUT,
    dry_run: bool = False,
) -> ScanResult:
    """Run a full scan for a single registered source (async implementation).

    Args:
        source:  The source configuration (kind, id, params).
        cfg:     Atlas configuration (used to open the SQLite store).
        timeout: Per-source scan timeout in seconds (default 300).  If the
                 scan exceeds this limit it is cancelled and a timed-out
                 ScanResult is returned.
        dry_run: When True, validate config and build the adapter without
                 extracting or writing any data.

    Returns:
        A ScanResult summarising assets written and edges derived.
    """
    from alma_ports.errors import AtlasError

    try:
        result = await asyncio.wait_for(
            _run_scan_impl(source, cfg, dry_run=dry_run),
            timeout=timeout,
        )
    except TimeoutError:
        logger.error(
            "[scan/%s] Scan timed out after %.0fs — returning partial result.",
            source.id,
            timeout,
        )
        return ScanResult(
            source_id=source.id,
            error=f"TimeoutError: Scan timed out after {timeout:.0f}s",
        )
    except AtlasError as exc:
        logger.exception("Scan failed for source %s", source.id)
        return ScanResult(source_id=source.id, error=f"{type(exc).__name__}: {exc}")
    except Exception as exc:
        logger.exception("Unexpected error scanning source %s", source.id)
        return ScanResult(source_id=source.id, error=f"{type(exc).__name__}: {exc}")
    return result


async def _run_scan_impl(
    source: SourceConfig,
    cfg: AtlasConfig,
    *,
    dry_run: bool = False,
) -> ScanResult:
    """Inner implementation of run_scan_async (no timeout wrapper)."""
    from alma_atlas.pipeline.stitch import stitch
    from alma_atlas_store.asset_repository import Asset, AssetRepository
    from alma_atlas_store.db import Database

    t0 = time.monotonic()
    logger.info("[scan/%s] Starting scan (kind=%s, dry_run=%s)", source.id, source.kind, dry_run)

    # Phase: adapter construction — bad config skips this source, not the whole pipeline.
    try:
        adapter, persisted = _build_adapter(source)
    except (ValueError, ImportError) as exc:
        raise ConfigurationError(str(exc)) from exc

    # Dry-run: validate config and build the adapter without extracting data.
    if dry_run:
        logger.info("[scan/%s] Dry-run complete — adapter constructed successfully.", source.id)
        return ScanResult(source_id=source.id)

    # Phase: schema introspection.
    t_schema = time.monotonic()
    logger.info("[scan/%s] Phase: schema introspection", source.id)
    try:
        snapshot = await adapter.introspect_schema(persisted)
    except Exception as exc:
        raise ExtractionError(f"Schema introspection failed: {exc}") from exc
    logger.info(
        "[scan/%s] Schema introspection complete: %d object(s) in %.2fs",
        source.id,
        len(snapshot.objects),
        time.monotonic() - t_schema,
    )

    with Database(cfg.db_path) as db:  # type: ignore[arg-type]
        repo = AssetRepository(db)

        from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository
        from alma_atlas_store.schema_repository import SchemaSnapshot as StoreSnapshot

        schema_repo = SchemaRepository(db)

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

            # Persist schema snapshot (column-level info) for every asset —
            # enables atlas_get_schema, atlas_check_contract, and drift detection.
            if obj.columns:
                store_snapshot = StoreSnapshot(
                    asset_id=asset_id,
                    columns=[
                        ColumnInfo(
                            name=c.name,
                            type=getattr(c, "data_type", None) or getattr(c, "type", "unknown"),
                            nullable=getattr(c, "nullable", True),
                        )
                        for c in obj.columns
                    ],
                )
                schema_repo.upsert(store_snapshot)

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

        # Phase: traffic observation.
        t_traffic = time.monotonic()
        logger.info("[scan/%s] Phase: traffic observation", source.id)
        try:
            traffic = await adapter.observe_traffic(persisted)
        except Exception as exc:
            logger.warning("Traffic observation failed for source %s: %s", source.id, exc)
            return ScanResult(
                source_id=source.id,
                asset_count=len(snapshot.objects),
                edge_count=dep_edge_count,
                warnings=[f"ExtractionError: Traffic observation failed: {exc}"],
                snapshot=snapshot,
            )
        logger.info(
            "[scan/%s] Traffic observation complete: %d event(s) in %.2fs",
            source.id,
            getattr(traffic, "scanned_records", 0),
            time.monotonic() - t_traffic,
        )

        # Phase: stitch (lineage edge derivation).
        t_stitch = time.monotonic()
        logger.info("[scan/%s] Phase: stitch", source.id)
        edge_count = stitch(traffic, db, source_id=source.id, source_kind=source.kind) + dep_edge_count
        logger.info(
            "[scan/%s] Stitch complete: %d edge(s) in %.2fs",
            source.id,
            edge_count,
            time.monotonic() - t_stitch,
        )

        # Phase: enforcement.
        t_enforce = time.monotonic()
        logger.info("[scan/%s] Phase: enforcement", source.id)
        enforcement_blocked = False
        enforcement_violations = False
        try:
            enforcement_blocked, enforcement_violations = _run_enforcement(snapshot, source.id, db)
        except Exception as exc:
            logger.exception("Enforcement check failed for source %s: %s", source.id, exc)
        logger.info(
            "[scan/%s] Enforcement complete in %.2fs",
            source.id,
            time.monotonic() - t_enforce,
        )

    logger.info(
        "[scan/%s] Scan finished: %d asset(s), %d edge(s) in %.2fs",
        source.id,
        len(snapshot.objects),
        edge_count,
        time.monotonic() - t0,
    )
    result = ScanResult(
        source_id=source.id,
        asset_count=len(snapshot.objects),
        edge_count=edge_count,
        snapshot=snapshot,
    )
    if enforcement_blocked:
        result.warnings.append("enforcement_blocked: schema violations detected in enforce mode")

    # Fire drift_detected hooks if any violations were found.
    if enforcement_violations and cfg.hooks:
        from datetime import UTC, datetime

        from alma_atlas.hooks import HookEvent, HookExecutor

        executor = HookExecutor(cfg.hooks)
        event = HookEvent(
            event_type="drift_detected",
            source_id=source.id,
            timestamp=datetime.now(UTC).isoformat(),
            data={
                "blocked": enforcement_blocked,
                "asset_count": len(snapshot.objects),
            },
        )
        await executor.fire(event)

    return result


def run_scan(
    source: SourceConfig,
    cfg: AtlasConfig,
    *,
    timeout: float = _DEFAULT_SCAN_TIMEOUT,
) -> ScanResult:
    """Sync entry point for a full scan — safe from both sync and async contexts.

    Args:
        source:  The source configuration (kind, id, params).
        cfg:     Atlas configuration (used to open the SQLite store).
        timeout: Per-source scan timeout in seconds (default 300).

    Returns:
        A ScanResult summarising assets written and edges derived.
    """
    return asyncio.run(run_scan_async(source, cfg, timeout=timeout))


def _run_enforcement(snapshot: SchemaSnapshot, source_id: str, db: object) -> tuple[bool, bool]:
    """Run drift detection + enforcement for any assets that have contracts.

    Silently skips assets without contracts.  Enforcement violations are
    always persisted to the store; the mode on each contract controls whether
    the result is merely logged (shadow), surfaced (warn), or blocking
    (enforce).

    Args:
        snapshot:  Schema snapshot from the adapter.
        source_id: Source identifier for asset ID construction.
        db:        Open Database connection (shared from the caller).

    Returns:
        Tuple of (any_blocked, has_violations) where any_blocked is True if a
        contract in enforce mode was violated, and has_violations is True if any
        drift violations were detected across all modes.
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

    any_blocked = False
    has_violations = False

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

        # Persist the current snapshot so future scans can detect drift.
        schema_repo.upsert(current)

        report = detector.detect(asset_id, previous, current)
        if not report.has_violations:
            continue

        has_violations = True
        for contract in contracts:
            result = engine.enforce(report, contract.mode)
            if result.blocked:
                any_blocked = True
                log.warning(
                    "[enforcement/enforce] Pipeline BLOCKED for asset %s — "
                    "%d error violation(s) detected.",
                    asset_id,
                    report.error_count,
                )

    return any_blocked, has_violations


async def _run_scan_all_async(
    sources: list[SourceConfig],
    cfg: AtlasConfig,
    *,
    max_concurrent: int = _DEFAULT_MAX_CONCURRENT,
    timeout: float = _DEFAULT_SCAN_TIMEOUT,
    repo_path: Path | None = None,
    no_learn: bool = False,
) -> ScanAllResult:
    """Async implementation of run_scan_all with concurrency control."""
    from alma_atlas.pipeline.cross_system_edges import (
        discover_cross_system_edges,
        resolve_dbt_source_edges,
    )
    from alma_atlas_store.db import Database

    semaphore = asyncio.Semaphore(max_concurrent)

    async def _scan_with_sem(source: SourceConfig) -> ScanResult:
        async with semaphore:
            return await run_scan_async(source, cfg, timeout=timeout)

    raw_results = await asyncio.gather(
        *[_scan_with_sem(s) for s in sources],
        return_exceptions=True,
    )

    results: list[ScanResult] = []
    snapshots: dict[str, SchemaSnapshot] = {}

    for source, raw in zip(sources, raw_results, strict=False):
        if isinstance(raw, BaseException):
            logger.error("[scan] Unexpected error scanning %s: %s", source.id, raw)
            results.append(ScanResult(source_id=source.id, error=str(raw)))
        else:
            results.append(raw)
            if raw.snapshot is not None:
                snapshots[source.id] = raw.snapshot

    kind_by_id = {s.id: s.kind for s in sources}
    dbt_snapshots = {sid: snap for sid, snap in snapshots.items() if kind_by_id.get(sid) == "dbt"}
    warehouse_snapshots = {sid: snap for sid, snap in snapshots.items() if kind_by_id.get(sid) != "dbt"}

    cross_system_edge_count = 0
    if len(snapshots) >= 2:
        with Database(cfg.db_path) as db:  # type: ignore[arg-type]
            cross_system_edge_count = discover_cross_system_edges(snapshots, db)
            cross_system_edge_count += resolve_dbt_source_edges(dbt_snapshots, warehouse_snapshots, db)

    # Run learning phase if repo_path provided and a real (non-mock) provider is configured.
    if repo_path is not None and not no_learn and cfg.learning.provider != "mock":
        from alma_atlas.pipeline.learn import run_asset_annotation, run_edge_learning

        with Database(cfg.db_path) as db:  # type: ignore[arg-type]
            logger.info("[scan] Running learning phase from %s", repo_path)
            await run_edge_learning(db, repo_path, config=cfg.learning)
            await run_asset_annotation(db, repo_path, config=cfg.learning)

    return ScanAllResult(results=results, cross_system_edge_count=cross_system_edge_count)


def run_scan_all(
    sources: list[SourceConfig],
    cfg: AtlasConfig,
    *,
    max_concurrent: int = _DEFAULT_MAX_CONCURRENT,
    timeout: float = _DEFAULT_SCAN_TIMEOUT,
    repo_path: Path | None = None,
    no_learn: bool = False,
) -> ScanAllResult:
    """Run scans for all sources concurrently, then discover cross-system edges.

    Sources are scanned concurrently up to *max_concurrent* at a time using
    ``asyncio.Semaphore``.  Each source scan is bounded by *timeout* seconds.
    Results for all sources are collected even if individual scans fail.

    When *repo_path* is provided and ``cfg.learning.provider`` is not ``mock``,
    the learning phase (edge learning + asset annotation) runs automatically
    after cross-system edge discovery.  Pass ``no_learn=True`` to suppress this.

    Args:
        sources:        List of source configurations to scan.
        cfg:            Atlas configuration (used to open the SQLite store).
        max_concurrent: Maximum number of concurrent source scans (default 4).
        timeout:        Per-source scan timeout in seconds (default 300).
        repo_path:      Optional path to the code repository.  When provided
                        with a real agent provider, learning runs after scan.
        no_learn:       Skip the learning phase even when agents are configured.

    Returns:
        A :class:`ScanAllResult` aggregating per-source results and the total
        number of cross-system edges discovered.
    """
    return asyncio.run(
        _run_scan_all_async(
            sources,
            cfg,
            max_concurrent=max_concurrent,
            timeout=timeout,
            repo_path=repo_path,
            no_learn=no_learn,
        )
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
