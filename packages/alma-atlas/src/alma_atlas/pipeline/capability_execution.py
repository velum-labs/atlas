"""Canonical capability planning and execution helpers for scan orchestration."""

from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    SourceAdapterV2,
)

if TYPE_CHECKING:
    from alma_connectors.source_adapter import PersistedSourceAdapter

logger = logging.getLogger(__name__)

_CAPABILITY_ORDER: tuple[AdapterCapability, ...] = (
    AdapterCapability.DISCOVER,
    AdapterCapability.SCHEMA,
    AdapterCapability.DEFINITIONS,
    AdapterCapability.TRAFFIC,
    AdapterCapability.LINEAGE,
    AdapterCapability.ORCHESTRATION,
)


@dataclass
class ExtractionPlan:
    """Ordered list of capabilities to execute, with skip reasoning."""

    capabilities: list[AdapterCapability]
    skipped: list[AdapterCapability]
    probe_results: dict[AdapterCapability, CapabilityProbeResult]


class CapabilityRouter:
    """Build an ordered extraction plan from probe() results."""

    def build_plan(
        self,
        probe_results: tuple[CapabilityProbeResult, ...],
    ) -> ExtractionPlan:
        probe_map: dict[AdapterCapability, CapabilityProbeResult] = {
            result.capability: result for result in probe_results
        }

        capabilities: list[AdapterCapability] = []
        skipped: list[AdapterCapability] = []

        for capability in _CAPABILITY_ORDER:
            if capability not in probe_map:
                continue
            result = probe_map[capability]
            if result.available:
                capabilities.append(capability)
            else:
                msg = result.message or "probe returned available=False"
                if result.permissions_missing:
                    missing = ", ".join(result.permissions_missing)
                    msg = f"{msg} (missing permissions: {missing})"
                logger.warning("Skipping capability %s: %s", capability, msg)
                skipped.append(capability)

        return ExtractionPlan(
            capabilities=capabilities,
            skipped=skipped,
            probe_results=probe_map,
        )


class ExtractionPipeline:
    """Execute an extraction plan against a SourceAdapterV2 adapter."""

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
        results: dict[AdapterCapability, Any] = {}
        warnings: list[str] = []

        for capability in plan.capabilities:
            started_at = datetime.now(UTC)
            try:
                result = await self._run_capability(capability)
                duration_ms = (datetime.now(UTC) - started_at).total_seconds() * 1000
                logger.info("Completed %s extraction in %.1fms", capability, duration_ms)
                results[capability] = result
            except Exception as exc:
                duration_ms = (datetime.now(UTC) - started_at).total_seconds() * 1000
                msg = f"{capability} extraction failed after {duration_ms:.0f}ms: {exc}"
                logger.warning(msg)
                warnings.append(msg)

        return results, warnings

    async def _run_capability(self, capability: AdapterCapability) -> Any:
        if capability == AdapterCapability.DISCOVER:
            return await self._adapter.discover(self._persisted)
        if capability == AdapterCapability.SCHEMA:
            return await self._adapter.extract_schema(self._persisted)
        if capability == AdapterCapability.DEFINITIONS:
            return await self._adapter.extract_definitions(self._persisted)
        if capability == AdapterCapability.TRAFFIC:
            return await self._adapter.extract_traffic(self._persisted)
        if capability == AdapterCapability.LINEAGE:
            return await self._adapter.extract_lineage(self._persisted)
        if capability == AdapterCapability.ORCHESTRATION:
            return await self._adapter.extract_orchestration(self._persisted)
        raise ValueError(f"Unknown capability: {capability}")


def upsert_extraction_result(
    db: Any,
    adapter_key: str,
    capability: AdapterCapability,
    result: Any,
) -> None:
    """Serialize one extraction result and upsert into `v2_extraction_results`."""
    meta = result.meta
    row_id = f"{adapter_key}:{capability.value}:{meta.captured_at.isoformat()}"
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
            capability.value,
            meta.scope_context.scope.value,
            meta.captured_at.isoformat(),
            meta.duration_ms,
            meta.row_count,
            payload,
        ),
    )


def _serialise(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {key: _serialise(value) for key, value in dataclasses.asdict(obj).items()}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "value"):
        return obj.value
    if isinstance(obj, (list, tuple)):
        return [_serialise(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _serialise(value) for key, value in obj.items()}
    return obj
