"""Capability-aware helper types for the canonical scan runtime.

`pipeline/scan.py` is the authoritative scan spine. This module keeps the
capability planning/execution helpers used by that runtime plus a small
compatibility facade (`ScannerV2` / `run_scan_v2`) for legacy call sites.
"""

from __future__ import annotations

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
    from alma_atlas.pipeline.scan import ScanResult
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
    """Compatibility facade over the canonical scan runtime."""

    def __init__(self, cfg: AtlasConfig) -> None:
        self._cfg = cfg

    def scan(self, source: SourceConfig) -> ScanResultV2:
        """Run a full scan using the canonical scan runtime."""
        from alma_atlas.pipeline.scan import run_scan

        result = run_scan(source, self._cfg)
        return scan_result_to_v2(result)


def scan_result_to_v2(result: ScanResult) -> ScanResultV2:
    """Convert the canonical scan result into the v2 compatibility shape."""

    return ScanResultV2(
        source_id=result.source_id,
        capabilities_run=[AdapterCapability(cap) for cap in result.capabilities_run],
        capabilities_skipped=[AdapterCapability(cap) for cap in result.capabilities_skipped],
        asset_count=result.asset_count,
        edge_count=result.edge_count,
        error=result.error,
        warnings=list(result.warnings),
    )


def _upsert_extraction_result(
    db: Any,
    adapter_key: str,
    cap: AdapterCapability,
    result: Any,
) -> None:
    """Serialise one extraction result and upsert into `v2_extraction_results`."""

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


def _serialise(obj: Any) -> Any:
    """Recursively convert dataclasses / enums / datetimes to JSON-safe types."""

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialise(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "value"):
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
