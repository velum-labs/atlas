"""Capability-aware extraction pipeline for SourceAdapterV2 adapters.

Implements:
  - CapabilityRouter: builds an ordered extraction plan from probe() results
  - ExtractionPipeline: executes the plan in canonical capability order
  - ScannerV2: top-level orchestrator with automatic v1 fallback
  - run_scan_v2(): drop-in replacement for run_scan() for v2 adapters
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DefinitionSnapshot,
    DiscoverySnapshot,
    LineageSnapshot,
    OrchestrationSnapshot,
    SchemaSnapshotV2,
    SourceAdapterV2,
    TrafficExtractionResult,
)

if TYPE_CHECKING:
    from alma_atlas.config import AtlasConfig, SourceConfig
    from alma_connectors.source_adapter import PersistedSourceAdapter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical execution order (per protocol spec)
# ---------------------------------------------------------------------------

_CAPABILITY_ORDER: tuple[AdapterCapability, ...] = (
    AdapterCapability.DISCOVER,
    AdapterCapability.SCHEMA,
    AdapterCapability.DEFINITIONS,
    AdapterCapability.TRAFFIC,
    AdapterCapability.LINEAGE,
    AdapterCapability.ORCHESTRATION,
)

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

ExtractionResultV2 = (
    DiscoverySnapshot
    | SchemaSnapshotV2
    | DefinitionSnapshot
    | TrafficExtractionResult
    | LineageSnapshot
    | OrchestrationSnapshot
)


@dataclass
class ScanResultV2:
    """Summary of a completed v2 scan for one source."""

    source_id: str
    capabilities_run: list[AdapterCapability] = field(default_factory=list)
    capabilities_skipped: list[AdapterCapability] = field(default_factory=list)
    asset_count: int = 0
    edge_count: int = 0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    results: dict[AdapterCapability, Any] = field(default_factory=dict)


@dataclass
class ExtractionPlan:
    """Ordered list of capabilities to execute, with skip reasoning."""

    capabilities: list[AdapterCapability]
    skipped: list[AdapterCapability]
    probe_results: dict[AdapterCapability, CapabilityProbeResult]


# ---------------------------------------------------------------------------
# CapabilityRouter
# ---------------------------------------------------------------------------


class CapabilityRouter:
    """Builds an ordered extraction plan from probe() results.

    Filters to available capabilities only and preserves canonical order.
    Unavailable capabilities are logged and recorded in the skipped list.
    """

    def build_plan(
        self,
        probe_results: tuple[CapabilityProbeResult, ...],
    ) -> ExtractionPlan:
        """Build an ordered extraction plan from probe results.

        Args:
            probe_results: Results returned by adapter.probe().

        Returns:
            ExtractionPlan with ordered available capabilities and skipped list.
        """
        probe_map: dict[AdapterCapability, CapabilityProbeResult] = {
            r.capability: r for r in probe_results
        }

        capabilities: list[AdapterCapability] = []
        skipped: list[AdapterCapability] = []

        for cap in _CAPABILITY_ORDER:
            if cap not in probe_map:
                continue
            result = probe_map[cap]
            if result.available:
                capabilities.append(cap)
            else:
                msg = result.message or "probe returned available=False"
                if result.permissions_missing:
                    missing = ", ".join(result.permissions_missing)
                    msg = f"{msg} (missing permissions: {missing})"
                logger.warning("Skipping capability %s: %s", cap, msg)
                skipped.append(cap)

        return ExtractionPlan(
            capabilities=capabilities,
            skipped=skipped,
            probe_results=probe_map,
        )


# ---------------------------------------------------------------------------
# ExtractionPipeline
# ---------------------------------------------------------------------------


class ExtractionPipeline:
    """Executes an extraction plan against a SourceAdapterV2 adapter.

    Runs each capability in plan order. Failures are caught per-capability —
    a warning is recorded and execution continues with the next capability.
    """

    def __init__(
        self,
        adapter: SourceAdapterV2,
        persisted: PersistedSourceAdapter,
    ) -> None:
        self._adapter = adapter
        self._persisted = persisted

    async def execute(
        self,
        plan: ExtractionPlan,
    ) -> tuple[dict[AdapterCapability, Any], list[str]]:
        """Execute the extraction plan in capability order.

        Args:
            plan: The plan built by CapabilityRouter.

        Returns:
            Tuple of (results dict, warnings list). Results contains only
            capabilities that succeeded; failed capabilities appear in warnings.
        """
        results: dict[AdapterCapability, Any] = {}
        warnings: list[str] = []

        for cap in plan.capabilities:
            started_at = datetime.now(UTC)
            try:
                result = await self._run_capability(cap)
                duration_ms = (datetime.now(UTC) - started_at).total_seconds() * 1000
                logger.info("Completed %s extraction in %.1fms", cap, duration_ms)
                results[cap] = result
            except Exception as exc:
                duration_ms = (datetime.now(UTC) - started_at).total_seconds() * 1000
                msg = f"{cap} extraction failed after {duration_ms:.0f}ms: {exc}"
                logger.warning(msg)
                warnings.append(msg)

        return results, warnings

    async def _run_capability(self, cap: AdapterCapability) -> Any:
        """Dispatch to the correct adapter method for the given capability."""
        if cap == AdapterCapability.DISCOVER:
            return await self._adapter.discover(self._persisted)
        if cap == AdapterCapability.SCHEMA:
            return await self._adapter.extract_schema(self._persisted)
        if cap == AdapterCapability.DEFINITIONS:
            return await self._adapter.extract_definitions(self._persisted)
        if cap == AdapterCapability.TRAFFIC:
            return await self._adapter.extract_traffic(self._persisted)
        if cap == AdapterCapability.LINEAGE:
            return await self._adapter.extract_lineage(self._persisted)
        if cap == AdapterCapability.ORCHESTRATION:
            return await self._adapter.extract_orchestration(self._persisted)
        raise ValueError(f"Unknown capability: {cap}")  # pragma: no cover


# ---------------------------------------------------------------------------
# ScannerV2
# ---------------------------------------------------------------------------


class ScannerV2:
    """Top-level orchestrator for v2 adapter scans.

    Detects whether an adapter implements SourceAdapterV2 or the legacy v1
    SourceAdapter and routes accordingly, ensuring backward compatibility.
    """

    def __init__(self, cfg: AtlasConfig) -> None:
        self._cfg = cfg

    def scan(self, source: SourceConfig) -> ScanResultV2:
        """Run a full scan for one source.

        v2 adapters go through: probe → route → extract → store.
        v1 adapters delegate to run_scan() and the result is wrapped.

        Args:
            source: The source configuration.

        Returns:
            ScanResultV2 summarising what was extracted.
        """
        from alma_atlas.pipeline.scan import _build_adapter, run_scan
        from alma_ports.errors import ConfigurationError

        try:
            adapter, persisted = _build_adapter(source)
        except (ValueError, ImportError) as exc:
            raise ConfigurationError(str(exc)) from exc

        if isinstance(adapter, SourceAdapterV2):
            return self._scan_v2(adapter, persisted, source)

        # v1 fallback
        logger.debug("Adapter %r does not implement SourceAdapterV2; using v1 path", source.kind)
        v1_result = run_scan(source, self._cfg)
        return ScanResultV2(
            source_id=v1_result.source_id,
            asset_count=v1_result.asset_count,
            edge_count=v1_result.edge_count,
            error=v1_result.error,
            warnings=list(v1_result.warnings),
        )

    def _scan_v2(
        self,
        adapter: SourceAdapterV2,
        persisted: PersistedSourceAdapter,
        source: SourceConfig,
    ) -> ScanResultV2:
        """Run the full v2 extraction pipeline for one source."""
        router = CapabilityRouter()
        pipeline = ExtractionPipeline(adapter, persisted)

        from alma_ports.errors import ExtractionError

        # --- probe ---
        try:
            probe_results = asyncio.run(adapter.probe(persisted))
        except Exception as exc:
            raise ExtractionError(f"Capability probing failed: {exc}") from exc

        plan = router.build_plan(probe_results)

        if not plan.capabilities:
            return ScanResultV2(
                source_id=source.id,
                capabilities_skipped=plan.skipped,
                warnings=["No capabilities available; nothing extracted."],
            )

        # --- extract ---
        try:
            results, extraction_warnings = asyncio.run(pipeline.execute(plan))
        except Exception as exc:
            raise ExtractionError(f"Extraction pipeline failed: {exc}") from exc

        # --- store ---
        store_warnings: list[str] = []
        asset_count = 0
        edge_count = 0
        try:
            asset_count, edge_count = _store_v2_results(
                results, persisted, source, self._cfg
            )
        except Exception as exc:
            logger.warning("Failed to store extraction results for source %s: %s", source.id, exc)
            store_warnings.append(f"ExtractionError: Failed to store extraction results: {exc}")

        return ScanResultV2(
            source_id=source.id,
            capabilities_run=list(results.keys()),
            capabilities_skipped=plan.skipped,
            asset_count=asset_count,
            edge_count=edge_count,
            warnings=extraction_warnings + store_warnings,
            results=results,
        )


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------


def _store_v2_results(
    results: dict[AdapterCapability, Any],
    persisted: PersistedSourceAdapter,
    source: SourceConfig,
    cfg: AtlasConfig,
) -> tuple[int, int]:
    """Persist v2 extraction results to the Atlas SQLite store.

    For every capability result:
      - Serialise and upsert into v2_extraction_results.
    Additionally:
      - SCHEMA: upsert objects as assets in the assets table.
      - LINEAGE: upsert edges in the edges table.

    Returns:
        Tuple of (asset_count, edge_count) written.
    """
    from alma_atlas_store.asset_repository import Asset, AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import Edge, EdgeRepository  # Edge used for schema deps

    asset_count = 0
    edge_count = 0

    with Database(cfg.db_path) as db:
        # Raw serialised snapshots
        for cap, result in results.items():
            _upsert_extraction_result(db, persisted.key, cap, result)

        # Derived: assets from SCHEMA
        if AdapterCapability.SCHEMA in results:
            schema_result: SchemaSnapshotV2 = results[AdapterCapability.SCHEMA]
            repo = AssetRepository(db)
            for obj in schema_result.objects:
                asset_id = f"{source.id}::{obj.schema_name}.{obj.object_name}"
                repo.upsert(
                    Asset(
                        id=asset_id,
                        source=source.id,
                        kind=obj.kind.value,
                        name=f"{obj.schema_name}.{obj.object_name}",
                    )
                )
                asset_count += 1

            # Schema-level object dependencies
            if schema_result.dependencies:
                edge_repo = EdgeRepository(db)
                asset_id_map = {
                    (obj.schema_name, obj.object_name): f"{source.id}::{obj.schema_name}.{obj.object_name}"
                    for obj in schema_result.objects
                }
                for dep in schema_result.dependencies:
                    upstream_id = asset_id_map.get((dep.target_schema, dep.target_object))
                    downstream_id = asset_id_map.get((dep.source_schema, dep.source_object))
                    if upstream_id and downstream_id:
                        edge_repo.upsert(
                            Edge(upstream_id=upstream_id, downstream_id=downstream_id, kind="depends_on")
                        )
                        edge_count += 1

        # Derived: lineage edges — stored in v2_lineage_edges (no FK constraints)
        if AdapterCapability.LINEAGE in results:
            lineage_result: LineageSnapshot = results[AdapterCapability.LINEAGE]
            captured_at = lineage_result.meta.captured_at.isoformat()
            for edge in lineage_result.edges:
                row_id = f"{persisted.key}:{edge.source_object}:{edge.target_object}:{edge.edge_kind.value}"
                db.conn.execute(
                    """
                    INSERT INTO v2_lineage_edges
                        (id, adapter_key, source_object, target_object, edge_kind, confidence, metadata, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        confidence  = excluded.confidence,
                        metadata    = excluded.metadata,
                        captured_at = excluded.captured_at,
                        stored_at   = CURRENT_TIMESTAMP
                    """,
                    (
                        row_id,
                        persisted.key,
                        edge.source_object,
                        edge.target_object,
                        edge.edge_kind.value,
                        edge.confidence,
                        json.dumps(_serialise(edge.metadata)),
                        captured_at,
                    ),
                )
                edge_count += 1
            db.conn.commit()

    return asset_count, edge_count


def _upsert_extraction_result(
    db: Any,
    adapter_key: str,
    cap: AdapterCapability,
    result: Any,
) -> None:
    """Serialise one extraction result and upsert into v2_extraction_results."""
    meta = result.meta
    row_id = f"{adapter_key}:{cap.value}:{meta.captured_at.isoformat()}"
    payload = json.dumps(_serialise(result))

    db.conn.execute(
        """
        INSERT INTO v2_extraction_results
            (id, adapter_key, adapter_kind, capability, scope, captured_at, duration_ms, row_count, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            payload   = excluded.payload,
            stored_at = CURRENT_TIMESTAMP
        """,
        (
            row_id,
            adapter_key,
            meta.adapter_kind.value,
            cap.value,
            meta.scope_context.scope.value,
            meta.captured_at.isoformat(),
            meta.duration_ms,
            meta.row_count,
            payload,
        ),
    )
    db.conn.commit()


def _serialise(obj: Any) -> Any:
    """Recursively convert dataclasses / enums / datetimes to JSON-safe types."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialise(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "value"):  # StrEnum / Enum
        return obj.value
    if isinstance(obj, (list, tuple)):
        return [_serialise(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    return obj


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_scan_v2(source: SourceConfig, cfg: AtlasConfig) -> ScanResultV2:
    """Run a capability-aware v2 scan for a single registered source.

    Mirrors the signature of :func:`~alma_atlas.pipeline.scan.run_scan` so
    callers can swap implementations without interface changes.  Catches
    :class:`~alma_ports.errors.ConfigurationError` and unexpected exceptions,
    returning a :class:`ScanResultV2` with a structured ``error`` field of
    the form ``"ExceptionType: message"`` rather than propagating.

    Args:
        source: The source configuration (kind, id, params).
        cfg:    Atlas configuration (used to open the SQLite store).

    Returns:
        A ScanResultV2 summarising capabilities run, assets written, and edges derived.
    """
    from alma_ports.errors import AtlasError

    try:
        return ScannerV2(cfg).scan(source)
    except AtlasError as exc:
        logger.exception("Scan failed for source %s", source.id)
        return ScanResultV2(source_id=source.id, error=f"{type(exc).__name__}: {exc}")
    except Exception as exc:
        logger.exception("Unexpected error scanning source %s", source.id)
        return ScanResultV2(source_id=source.id, error=f"{type(exc).__name__}: {exc}")
