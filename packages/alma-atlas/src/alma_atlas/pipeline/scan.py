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
import inspect
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alma_atlas.application.scan.runtime_config import DEFAULT_SCAN_RUNTIME_CONFIG
from alma_atlas.config import AtlasConfig, SourceConfig
from alma_ports.errors import ConfigurationError, ExtractionError

if TYPE_CHECKING:
    from alma_connectors.source_adapter_v2 import SchemaSnapshotV2 as SchemaSnapshot

logger = logging.getLogger(__name__)

# Backward-compatible defaults for callers that still import the module constants.
_DEFAULT_SCAN_TIMEOUT = DEFAULT_SCAN_RUNTIME_CONFIG.timeout_seconds
_DEFAULT_MAX_CONCURRENT = DEFAULT_SCAN_RUNTIME_CONFIG.max_concurrent


@dataclass
class ScanResult:
    """Summary of a completed scan for one source."""

    source_id: str
    capabilities_run: list[str] = field(default_factory=list)
    capabilities_skipped: list[str] = field(default_factory=list)
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


def _capability_skip_warnings(plan: Any) -> list[str]:
    warnings: list[str] = []
    for capability in getattr(plan, "skipped", []):
        probe_result = getattr(plan, "probe_results", {}).get(capability)
        if probe_result is None:
            warnings.append(f"capability_skipped:{capability.value}")
            continue
        parts = [f"capability_skipped:{capability.value}"]
        if probe_result.message:
            parts.append(str(probe_result.message))
        if probe_result.permissions_missing:
            missing = ", ".join(probe_result.permissions_missing)
            parts.append(f"missing permissions: {missing}")
        warnings.append(" — ".join(parts))
    return warnings


def _canonical_object_ref(source_kind: str, ref: str) -> str:
    normalized = ref.strip().replace('"', "")
    if source_kind == "airflow" and normalized.startswith("airflow://"):
        dag_id = normalized.rsplit("/", 1)[-1]
        return f"airflow://{dag_id}"
    if source_kind == "looker" and normalized.startswith("looker://explore/"):
        parts = normalized.split("/")
        if len(parts) >= 5:
            return f"{parts[-2]}.{parts[-1]}"
    return normalized


async def _close_runtime_adapter(adapter: object) -> None:
    for method_name in ("aclose", "close"):
        closer = getattr(adapter, method_name, None)
        if closer is None:
            continue
        result = closer()
        if inspect.isawaitable(result):
            await result
        return


def _canonical_asset_id(source_id: str, source_kind: str, ref: str) -> str:
    return f"{source_id}::{_canonical_object_ref(source_kind, ref)}"


def _infer_placeholder_kind(ref: str) -> str:
    if ref.startswith("airflow://"):
        return "dag"
    if ref.startswith("fivetran://connector/"):
        return "connector"
    if ref.startswith("looker://"):
        return "semantic_model"
    if ref.startswith("metabase://database/"):
        return "database"
    if ref.startswith("metabase://collection/"):
        return "collection"
    if "." in ref:
        return "table"
    return "external"


def _merge_tags(existing: list[str], incoming: list[str]) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for tag in [*existing, *incoming]:
        if tag in seen:
            continue
        seen.add(tag)
        merged.append(tag)
    return merged


def _upsert_asset(
    repo: Any,
    *,
    asset_id: str,
    source_id: str,
    kind: str,
    name: str,
    description: str | None = None,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    preserve_existing_kind: bool = False,
) -> bool:
    from alma_atlas_store.asset_repository import Asset

    existing = repo.get(asset_id)
    merged_metadata = {
        **(existing.metadata if existing is not None else {}),
        **(metadata or {}),
    }
    merged_tags = _merge_tags(existing.tags if existing is not None else [], tags or [])
    repo.upsert(
        Asset(
            id=asset_id,
            source=source_id,
            kind=(
                existing.kind
                if preserve_existing_kind and existing is not None
                else kind or (existing.kind if existing is not None else "external")
            ),
            name=name or (existing.name if existing is not None else asset_id),
            description=description or (existing.description if existing is not None else None),
            tags=merged_tags,
            metadata=merged_metadata,
        )
    )
    return existing is None

def _store_discovery_assets(
    *,
    db: Any,
    source: SourceConfig,
    discovery_result: Any,
) -> int:
    from alma_atlas_store.asset_repository import AssetRepository

    asset_repo = AssetRepository(db)
    written = 0
    for container in discovery_result.containers:
        canonical_ref = _canonical_object_ref(source.kind, container.container_id)
        written += int(_upsert_asset(
            asset_repo,
            asset_id=f"{source.id}::{canonical_ref}",
            source_id=source.id,
            kind=container.container_type,
            name=container.display_name,
            metadata={
                "container_id": container.container_id,
                "location": container.location,
                **container.metadata,
            },
        ))
    return written


def _store_orchestration_assets(
    *,
    db: Any,
    source: SourceConfig,
    orchestration_result: Any,
) -> int:
    from alma_atlas_store.asset_repository import AssetRepository

    asset_repo = AssetRepository(db)
    written = 0
    for unit in orchestration_result.units:
        canonical_ref = _canonical_object_ref(source.kind, unit.unit_id)
        written += int(_upsert_asset(
            asset_repo,
            asset_id=f"{source.id}::{canonical_ref}",
            source_id=source.id,
            kind=unit.unit_type,
            name=unit.display_name,
            metadata={
                "schedule": unit.schedule,
                "last_run_at": unit.last_run_at.isoformat() if unit.last_run_at else None,
                "last_run_status": unit.last_run_status,
                "task_count": len(unit.tasks),
                **unit.metadata,
            },
        ))
    return written


def _store_schema_projection(
    *,
    db: Any,
    source: SourceConfig,
    schema_result: Any,
    definition_result: Any | None,
) -> tuple[int, int, Any]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.edge_repository import Edge, EdgeRepository
    from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository
    from alma_atlas_store.schema_repository import SchemaSnapshot as StoreSnapshot

    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)
    schema_repo = SchemaRepository(db)

    definitions_by_key: dict[tuple[str, str], Any] = {}
    if definition_result is not None:
        definitions_by_key = {
            (definition.schema_name, definition.object_name): definition
            for definition in definition_result.definitions
        }

    asset_id_map: dict[tuple[str, str], str] = {}
    asset_count = 0
    edge_count = 0
    for obj in schema_result.objects:
        asset_id = f"{source.id}::{obj.schema_name}.{obj.object_name}"
        definition = definitions_by_key.get((obj.schema_name, obj.object_name))
        metadata: dict[str, Any] = {
            "row_count": obj.row_count,
            "size_bytes": obj.size_bytes,
            "language": obj.language,
            "return_type": obj.return_type,
            "model_type": obj.model_type,
            "feature_columns": list(obj.feature_columns),
            "label_column": obj.label_column,
            "partition_column": obj.partition_column,
            "clustering_columns": list(obj.clustering_columns),
            "owner": obj.owner,
            "source_metadata": dict(obj.metadata),
        }
        if definition is not None:
            metadata["definition_text"] = definition.definition_text
            metadata["definition_language"] = definition.definition_language
            metadata["definition_metadata"] = dict(definition.metadata)

        asset_count += int(_upsert_asset(
            asset_repo,
            asset_id=asset_id,
            source_id=source.id,
            kind=obj.kind.value,
            name=f"{obj.schema_name}.{obj.object_name}",
            description=obj.description,
            tags=list(obj.tags),
            metadata=metadata,
        ))
        asset_id_map[(obj.schema_name, obj.object_name)] = asset_id

        if obj.columns:
            schema_repo.upsert(
                StoreSnapshot(
                    asset_id=asset_id,
                    columns=[
                        ColumnInfo(
                            name=column.name,
                            type=column.data_type,
                            nullable=column.is_nullable,
                            description=column.description,
                        )
                        for column in obj.columns
                    ],
                )
            )

    for dependency in schema_result.dependencies:
        upstream_id = asset_id_map.get((dependency.target_schema, dependency.target_object))
        downstream_id = asset_id_map.get((dependency.source_schema, dependency.source_object))
        if upstream_id is None or downstream_id is None:
            continue
        edge_repo.upsert(
            Edge(
                upstream_id=upstream_id,
                downstream_id=downstream_id,
                kind="depends_on",
            )
        )
        edge_count += 1

    return asset_count, edge_count, schema_result


def _store_lineage_projection(
    *,
    db: Any,
    source: SourceConfig,
    lineage_result: Any,
) -> tuple[int, int]:
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.edge_repository import Edge, EdgeRepository

    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)

    asset_count = 0
    edge_count = 0
    for lineage_edge in lineage_result.edges:
        upstream_ref = _canonical_object_ref(source.kind, lineage_edge.source_object)
        downstream_ref = _canonical_object_ref(source.kind, lineage_edge.target_object)
        upstream_id = f"{source.id}::{upstream_ref}"
        downstream_id = f"{source.id}::{downstream_ref}"
        edge_kind = "depends_on" if source.kind == "dbt" else lineage_edge.edge_kind.value

        asset_count += int(_upsert_asset(
            asset_repo,
            asset_id=upstream_id,
            source_id=source.id,
            kind=_infer_placeholder_kind(upstream_ref),
            name=upstream_ref,
            preserve_existing_kind=True,
        ))
        asset_count += int(_upsert_asset(
            asset_repo,
            asset_id=downstream_id,
            source_id=source.id,
            kind=_infer_placeholder_kind(downstream_ref),
            name=downstream_ref,
            preserve_existing_kind=True,
        ))
        edge_repo.upsert(
            Edge(
                upstream_id=upstream_id,
                downstream_id=downstream_id,
                kind=edge_kind,
                metadata={
                    "confidence": lineage_edge.confidence,
                    "column_mappings": [list(pair) for pair in lineage_edge.column_mappings],
                    "transformation_sql": lineage_edge.transformation_sql,
                    **lineage_edge.metadata,
                },
            )
        )
        edge_count += 1

    return asset_count, edge_count


def _store_scan_results(
    *,
    db: Any,
    cfg: AtlasConfig,
    source: SourceConfig,
    persisted: Any,
    results: dict[Any, Any],
) -> tuple[int, int, Any | None]:
    from alma_atlas.pipeline.capability_execution import upsert_extraction_result
    from alma_atlas.pipeline.stitch import stitch
    from alma_connectors.source_adapter_v2 import AdapterCapability

    for capability, result in results.items():
        upsert_extraction_result(db, persisted.key, capability, result)

    asset_count = 0
    edge_count = 0
    snapshot_result = None

    schema_result = results.get(AdapterCapability.SCHEMA)
    definition_result = results.get(AdapterCapability.DEFINITIONS)
    discovery_result = results.get(AdapterCapability.DISCOVER)
    lineage_result = results.get(AdapterCapability.LINEAGE)
    traffic_result = results.get(AdapterCapability.TRAFFIC)
    orchestration_result = results.get(AdapterCapability.ORCHESTRATION)

    if schema_result is not None:
        stored_assets, stored_edges, snapshot_result = _store_schema_projection(
            db=db,
            source=source,
            schema_result=schema_result,
            definition_result=definition_result,
        )
        asset_count += stored_assets
        edge_count += stored_edges
    elif discovery_result is not None:
        asset_count += _store_discovery_assets(
            db=db,
            source=source,
            discovery_result=discovery_result,
        )

    if orchestration_result is not None and schema_result is None:
        asset_count += _store_orchestration_assets(
            db=db,
            source=source,
            orchestration_result=orchestration_result,
        )

    if lineage_result is not None:
        lineage_assets, lineage_edges = _store_lineage_projection(
            db=db,
            source=source,
            lineage_result=lineage_result,
        )
        asset_count += lineage_assets
        edge_count += lineage_edges

    if traffic_result is not None:
        edge_count += stitch(
            traffic_result,
            db,
            source_id=source.id,
            source_kind=source.kind,
            query_storage_mode=cfg.privacy.query_storage_mode,
            query_retention_days=cfg.privacy.query_retention_days,
        )
        if traffic_result.observation_cursor is not None:
            source.params["observation_cursor"] = dict(traffic_result.observation_cursor)

    return asset_count, edge_count, snapshot_result


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
    from alma_atlas.pipeline.capability_execution import CapabilityRouter, ExtractionPipeline
    from alma_atlas_store.db import Database
    from alma_connectors.source_adapter_v2 import SourceAdapterV2

    if cfg.db_path is None:
        raise ConfigurationError("Atlas db_path is not configured")

    t0 = time.monotonic()
    logger.info("[scan/%s] Starting scan (kind=%s, dry_run=%s)", source.id, source.kind, dry_run)

    # Phase: adapter construction — bad config skips this source, not the whole pipeline.
    try:
        adapter, persisted = _build_adapter(source)
    except (ValueError, ImportError) as exc:
        raise ConfigurationError(str(exc)) from exc

    try:
        # Dry-run: validate config and build the adapter without extracting data.
        if dry_run:
            logger.info("[scan/%s] Dry-run complete — adapter constructed successfully.", source.id)
            return ScanResult(source_id=source.id)

        if not isinstance(adapter, SourceAdapterV2):
            raise ExtractionError(
                f"Adapter {type(adapter).__name__} does not implement SourceAdapterV2; "
                "the v1 scan path has been removed"
            )

        # Phase: capability probing.
        t_probe = time.monotonic()
        logger.info("[scan/%s] Phase: capability probe", source.id)
        try:
            probe_results = await adapter.probe(persisted)
        except Exception as exc:
            raise ExtractionError(f"Capability probe failed: {exc}") from exc
        logger.info(
            "[scan/%s] Capability probe complete: %d result(s) in %.2fs",
            source.id,
            len(probe_results),
            time.monotonic() - t_probe,
        )

        router = CapabilityRouter()
        plan = router.build_plan(probe_results)
        warnings = _capability_skip_warnings(plan)
        capabilities_run = [cap.value for cap in plan.capabilities]
        capabilities_skipped = [cap.value for cap in plan.skipped]
        if not plan.capabilities:
            return ScanResult(
                source_id=source.id,
                capabilities_run=capabilities_run,
                capabilities_skipped=capabilities_skipped,
                warnings=warnings or ["No capabilities available; nothing extracted."],
            )

        # Phase: extraction.
        t_extract = time.monotonic()
        logger.info("[scan/%s] Phase: capability extraction", source.id)
        try:
            results, extraction_warnings = await ExtractionPipeline(adapter, persisted).execute(plan)
        except Exception as exc:
            raise ExtractionError(f"Capability extraction failed: {exc}") from exc
        warnings.extend(extraction_warnings)
        from alma_connectors.source_adapter_v2 import AdapterCapability

        if (
            AdapterCapability.SCHEMA in plan.capabilities
            and AdapterCapability.SCHEMA not in results
        ):
            raise ExtractionError("Schema extraction failed")
        logger.info(
            "[scan/%s] Capability extraction complete: %d capability result(s) in %.2fs",
            source.id,
            len(results),
            time.monotonic() - t_extract,
        )

        snapshot = None
        asset_count = 0
        edge_count = 0
        enforcement_blocked = False
        enforcement_violations = False

        with Database(cfg.db_path) as db, db.transaction():
            t_store = time.monotonic()
            logger.info("[scan/%s] Phase: projection + persistence", source.id)
            asset_count, edge_count, snapshot = _store_scan_results(
                db=db,
                cfg=cfg,
                source=source,
                persisted=persisted,
                results=results,
            )
            logger.info(
                "[scan/%s] Projection complete: %d asset(s), %d edge(s) in %.2fs",
                source.id,
                asset_count,
                edge_count,
                time.monotonic() - t_store,
            )

            if snapshot is not None:
                t_enforce = time.monotonic()
                logger.info("[scan/%s] Phase: enforcement", source.id)
                try:
                    from alma_atlas.application.enforcement.use_cases import run_enforcement_for_snapshot

                    enforcement_blocked, enforcement_violations = run_enforcement_for_snapshot(
                        snapshot,
                        source.id,
                        db,
                    )
                except Exception as exc:
                    logger.exception("Enforcement check failed for source %s: %s", source.id, exc)
                    warnings.append(f"EnforcementError: {exc}")
                logger.info(
                    "[scan/%s] Enforcement complete in %.2fs",
                    source.id,
                    time.monotonic() - t_enforce,
                )

        logger.info(
            "[scan/%s] Scan finished: %d asset(s), %d edge(s) in %.2fs",
            source.id,
            asset_count,
            edge_count,
            time.monotonic() - t0,
        )
        result = ScanResult(
            source_id=source.id,
            capabilities_run=capabilities_run,
            capabilities_skipped=capabilities_skipped,
            asset_count=asset_count,
            edge_count=edge_count,
            warnings=warnings,
            snapshot=snapshot,
        )
        if enforcement_blocked:
            result.warnings.append("enforcement_blocked: schema violations detected in enforce mode")

        from alma_atlas.application.scan.post_scan import fire_drift_hooks

        await fire_drift_hooks(
            cfg,
            source_id=source.id,
            asset_count=asset_count,
            blocked=enforcement_blocked,
            has_violations=enforcement_violations,
        )

        return result
    finally:
        await _close_runtime_adapter(adapter)


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
    from alma_atlas.async_utils import run_sync

    return run_sync(run_scan_async(source, cfg, timeout=timeout))


def _run_enforcement(snapshot: SchemaSnapshot, source_id: str, db: Any) -> tuple[bool, bool]:
    from alma_atlas.application.enforcement.use_cases import run_enforcement_for_snapshot

    return run_enforcement_for_snapshot(snapshot, source_id, db)


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
    semaphore = asyncio.Semaphore(max_concurrent)
    if cfg.db_path is None:
        raise ConfigurationError("Atlas db_path is not configured")

    async def _scan_with_sem(source: SourceConfig) -> ScanResult:
        async with semaphore:
            return await run_scan_async(source, cfg, timeout=timeout)

    raw_results = await asyncio.gather(
        *[_scan_with_sem(s) for s in sources],
        return_exceptions=True,
    )

    results: list[ScanResult] = []

    for source, raw in zip(sources, raw_results, strict=False):
        if isinstance(raw, BaseException):
            logger.error("[scan] Unexpected error scanning %s: %s", source.id, raw)
            results.append(ScanResult(source_id=source.id, error=str(raw)))
        else:
            results.append(raw)
    from alma_atlas.application.scan.post_scan import run_multi_source_post_scan

    cross_system_edge_count = await run_multi_source_post_scan(
        sources=sources,
        results=results,
        cfg=cfg,
        repo_path=repo_path,
        no_learn=no_learn,
    )

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

    When *repo_path* is provided and the configured learning agents are enabled,
    the learning phase (edge learning + asset annotation) runs automatically
    after cross-system edge discovery. Pass ``no_learn=True`` to suppress this.

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
    from alma_atlas.async_utils import run_sync

    return run_sync(
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
    """Build the runtime adapter pair for one source configuration."""
    from alma_atlas.source_runtime import build_runtime_adapter

    return build_runtime_adapter(source)
