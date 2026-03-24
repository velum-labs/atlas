"""Shared base class for v2 source adapters.

Provides ``_make_meta`` (keyed on ``self.kind`` + ``_scope_identifiers()``) and
default ``NotImplementedError`` stubs for every v2 extraction capability.  Adapters
that do not support a given capability inherit the stub automatically — no need to
duplicate boilerplate.

Usage::

    class MyAdapter(BaseAdapterV2):
        kind = SourceAdapterKindV2.MY_ADAPTER
        declared_capabilities: frozenset[AdapterCapability] = frozenset({...})

        def _scope_identifiers(self) -> dict[str, str]:
            return {"base_url": self._base_url}

        # Only implement the capabilities you declare.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DefinitionSnapshot,
    DiscoverySnapshot,
    ExtractionMeta,
    ExtractionScope,
    LineageSnapshot,
    OrchestrationSnapshot,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

if TYPE_CHECKING:
    from datetime import datetime as DateTime

    from alma_connectors.source_adapter import PersistedSourceAdapter, QueryResult


class BaseAdapterV2:
    """Base class for SourceAdapterV2 implementations.

    Subclasses must set the ``kind`` class variable to their
    :class:`~alma_connectors.source_adapter_v2.SourceAdapterKindV2` value and
    override ``_scope_identifiers()`` to supply the identifiers dict used in
    every :class:`~alma_connectors.source_adapter_v2.ExtractionMeta` built by
    this adapter.

    All six extraction methods default to ``NotImplementedError``.  Override only
    those listed in ``declared_capabilities``.
    """

    kind: SourceAdapterKindV2
    declared_capabilities: frozenset[AdapterCapability]

    # ------------------------------------------------------------------
    # Scope helpers — override in subclasses
    # ------------------------------------------------------------------

    def _scope_identifiers(self) -> dict[str, str]:
        """Return identifiers for ScopeContext.  Override in each adapter."""
        return {}

    # ------------------------------------------------------------------
    # Shared ExtractionMeta factory
    # ------------------------------------------------------------------

    def _make_meta(
        self,
        adapter: PersistedSourceAdapter,
        capability: AdapterCapability,
        row_count: int,
        duration_ms: float,
    ) -> ExtractionMeta:
        """Build an ExtractionMeta from the adapter record and timing info."""
        return ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=self.kind,
            capability=capability,
            scope_context=ScopeContext(
                scope=ExtractionScope.GLOBAL,
                identifiers=self._scope_identifiers(),
            ),
            captured_at=datetime.now(UTC),
            duration_ms=duration_ms,
            row_count=row_count,
        )

    # ------------------------------------------------------------------
    # Shared probe helper
    # ------------------------------------------------------------------

    def _make_probe_results(
        self,
        caps_to_probe: frozenset[AdapterCapability],
        available: bool,
        scope_ctx: ScopeContext,
        message: str | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Build a uniform probe-result tuple for all requested capabilities."""
        return tuple(
            CapabilityProbeResult(
                capability=cap,
                available=available,
                scope=ExtractionScope.GLOBAL,
                scope_context=scope_ctx,
                message=message,
            )
            for cap in caps_to_probe
        )

    # ------------------------------------------------------------------
    # Default stubs — raise NotImplementedError for undeclared capabilities
    # ------------------------------------------------------------------

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """Not supported by this adapter."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support SCHEMA extraction "
            "(AdapterCapability.SCHEMA is not in declared_capabilities)"
        )

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """Not supported by this adapter."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support DEFINITIONS extraction "
            "(AdapterCapability.DEFINITIONS is not in declared_capabilities)"
        )

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: DateTime | None = None,
    ) -> TrafficExtractionResult:
        """Not supported by this adapter."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support TRAFFIC extraction "
            "(AdapterCapability.TRAFFIC is not in declared_capabilities)"
        )

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """Not supported by this adapter."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support LINEAGE extraction "
            "(AdapterCapability.LINEAGE is not in declared_capabilities)"
        )

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """Not supported by this adapter."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support ORCHESTRATION extraction "
            "(AdapterCapability.ORCHESTRATION is not in declared_capabilities)"
        )

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """Not supported by this adapter."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support DISCOVER extraction "
            "(AdapterCapability.DISCOVER is not in declared_capabilities)"
        )

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
        dry_run: bool = False,
    ) -> QueryResult:
        """Not supported by this adapter."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support query execution"
        )
