"""Tests for canonical capability execution helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from alma_atlas.pipeline.capability_execution import (
    _CAPABILITY_ORDER,
    CapabilityRouter,
    ExtractionPipeline,
    ExtractionPlan,
    upsert_extraction_result,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DefinitionSnapshot,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionMeta,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    ObjectDefinition,
    OrchestrationSnapshot,
    SchemaObject,
    SchemaObjectKind,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

_SCOPE = ScopeContext(scope=ExtractionScope.DATABASE, identifiers={"db": "test"})
_ADAPTER_KEY = "test-adapter"
_ADAPTER_KIND = SourceAdapterKindV2.POSTGRES


def _meta(capability: AdapterCapability, row_count: int = 0) -> ExtractionMeta:
    return ExtractionMeta(
        adapter_key=_ADAPTER_KEY,
        adapter_kind=_ADAPTER_KIND,
        capability=capability,
        scope_context=_SCOPE,
        captured_at=datetime.now(UTC),
        duration_ms=10.0,
        row_count=row_count,
    )


def _probe(
    capability: AdapterCapability,
    *,
    available: bool = True,
    message: str | None = None,
) -> CapabilityProbeResult:
    return CapabilityProbeResult(
        capability=capability,
        available=available,
        scope=ExtractionScope.DATABASE,
        message=message,
    )


def _all_probes(available: bool = True) -> tuple[CapabilityProbeResult, ...]:
    return tuple(_probe(capability, available=available) for capability in AdapterCapability)


class TestCapabilityRouter:
    def test_all_available_capabilities_included(self) -> None:
        router = CapabilityRouter()
        plan = router.build_plan(_all_probes(available=True))
        assert set(plan.capabilities) == set(AdapterCapability)
        assert plan.skipped == []

    def test_all_unavailable_all_skipped(self) -> None:
        router = CapabilityRouter()
        plan = router.build_plan(_all_probes(available=False))
        assert plan.capabilities == []
        assert set(plan.skipped) == set(AdapterCapability)

    def test_partial_capabilities(self) -> None:
        router = CapabilityRouter()
        probes = (
            _probe(AdapterCapability.DISCOVER, available=True),
            _probe(AdapterCapability.SCHEMA, available=True),
            _probe(AdapterCapability.TRAFFIC, available=False),
            _probe(AdapterCapability.LINEAGE, available=False),
        )
        plan = router.build_plan(probes)
        assert AdapterCapability.DISCOVER in plan.capabilities
        assert AdapterCapability.SCHEMA in plan.capabilities
        assert AdapterCapability.TRAFFIC in plan.skipped
        assert AdapterCapability.LINEAGE in plan.skipped
        assert AdapterCapability.DEFINITIONS not in plan.capabilities
        assert AdapterCapability.DEFINITIONS not in plan.skipped

    def test_canonical_order_preserved(self) -> None:
        router = CapabilityRouter()
        probes = tuple(reversed([_probe(capability) for capability in _CAPABILITY_ORDER]))
        plan = router.build_plan(probes)
        assert plan.capabilities == list(_CAPABILITY_ORDER)

    def test_empty_probe_results(self) -> None:
        router = CapabilityRouter()
        plan = router.build_plan(())
        assert plan.capabilities == []
        assert plan.skipped == []

    def test_missing_permissions_logged_in_skip(self, caplog) -> None:
        probe = CapabilityProbeResult(
            capability=AdapterCapability.TRAFFIC,
            available=False,
            scope=ExtractionScope.DATABASE,
            message="access denied",
            permissions_missing=("pg_stat_statements",),
        )
        router = CapabilityRouter()
        import logging

        with caplog.at_level(logging.WARNING):
            plan = router.build_plan((probe,))
        assert AdapterCapability.TRAFFIC in plan.skipped
        assert "pg_stat_statements" in caplog.text

    def test_probe_results_stored_in_plan(self) -> None:
        probes = _all_probes()
        router = CapabilityRouter()
        plan = router.build_plan(probes)
        for capability in AdapterCapability:
            assert capability in plan.probe_results


def _make_mock_adapter_v2() -> MagicMock:
    adapter = MagicMock()
    adapter.declared_capabilities = frozenset(AdapterCapability)

    adapter.probe = AsyncMock(return_value=_all_probes())
    adapter.discover = AsyncMock(
        return_value=DiscoverySnapshot(
            meta=_meta(AdapterCapability.DISCOVER),
            containers=(
                DiscoveredContainer(
                    container_id="db1",
                    container_type="database",
                    display_name="Database 1",
                ),
            ),
        )
    )
    adapter.extract_schema = AsyncMock(
        return_value=SchemaSnapshotV2(
            meta=_meta(AdapterCapability.SCHEMA, row_count=2),
            objects=(
                SchemaObject(
                    schema_name="public",
                    object_name="orders",
                    kind=SchemaObjectKind.TABLE,
                ),
                SchemaObject(
                    schema_name="public",
                    object_name="order_view",
                    kind=SchemaObjectKind.VIEW,
                ),
            ),
        )
    )
    adapter.extract_definitions = AsyncMock(
        return_value=DefinitionSnapshot(
            meta=_meta(AdapterCapability.DEFINITIONS),
            definitions=(
                ObjectDefinition(
                    schema_name="public",
                    object_name="order_view",
                    object_kind=SchemaObjectKind.VIEW,
                    definition_text="SELECT * FROM orders",
                    definition_language="sql",
                ),
            ),
        )
    )
    adapter.extract_traffic = AsyncMock(
        return_value=TrafficExtractionResult(
            meta=_meta(AdapterCapability.TRAFFIC),
            events=(),
        )
    )
    adapter.extract_lineage = AsyncMock(
        return_value=LineageSnapshot(
            meta=_meta(AdapterCapability.LINEAGE),
            edges=(
                LineageEdge(
                    source_object="public.orders",
                    target_object="public.order_view",
                    edge_kind=LineageEdgeKind.DECLARED,
                    confidence=1.0,
                ),
            ),
        )
    )
    adapter.extract_orchestration = AsyncMock(
        return_value=OrchestrationSnapshot(
            meta=_meta(AdapterCapability.ORCHESTRATION),
            units=(),
        )
    )
    return adapter


class TestExtractionPipeline:
    def _persisted(self) -> MagicMock:
        persisted = MagicMock()
        persisted.key = _ADAPTER_KEY
        return persisted

    def test_all_capabilities_executed(self) -> None:
        adapter = _make_mock_adapter_v2()
        plan = ExtractionPlan(
            capabilities=list(_CAPABILITY_ORDER),
            skipped=[],
            probe_results={},
        )
        pipeline = ExtractionPipeline(adapter, self._persisted())

        import asyncio

        results, warnings = asyncio.run(pipeline.execute(plan))
        assert set(results.keys()) == set(_CAPABILITY_ORDER)
        assert warnings == []

    def test_execution_order_matches_canonical(self) -> None:
        adapter = _make_mock_adapter_v2()
        call_order: list[AdapterCapability] = []

        async def _discover(_persisted):
            call_order.append(AdapterCapability.DISCOVER)
            return AdapterCapability.DISCOVER.value

        async def _schema(_persisted):
            call_order.append(AdapterCapability.SCHEMA)
            return AdapterCapability.SCHEMA.value

        async def _definitions(_persisted):
            call_order.append(AdapterCapability.DEFINITIONS)
            return AdapterCapability.DEFINITIONS.value

        async def _traffic(_persisted):
            call_order.append(AdapterCapability.TRAFFIC)
            return AdapterCapability.TRAFFIC.value

        async def _lineage(_persisted):
            call_order.append(AdapterCapability.LINEAGE)
            return AdapterCapability.LINEAGE.value

        async def _orchestration(_persisted):
            call_order.append(AdapterCapability.ORCHESTRATION)
            return AdapterCapability.ORCHESTRATION.value

        adapter.discover.side_effect = _discover
        adapter.extract_schema.side_effect = _schema
        adapter.extract_definitions.side_effect = _definitions
        adapter.extract_traffic.side_effect = _traffic
        adapter.extract_lineage.side_effect = _lineage
        adapter.extract_orchestration.side_effect = _orchestration

        plan = ExtractionPlan(
            capabilities=list(_CAPABILITY_ORDER),
            skipped=[],
            probe_results={},
        )
        pipeline = ExtractionPipeline(adapter, self._persisted())

        import asyncio

        asyncio.run(pipeline.execute(plan))
        assert call_order == list(_CAPABILITY_ORDER)

    def test_capability_failure_does_not_abort_remaining_plan(self) -> None:
        adapter = _make_mock_adapter_v2()
        adapter.extract_schema.side_effect = RuntimeError("boom")
        plan = ExtractionPlan(
            capabilities=[AdapterCapability.DISCOVER, AdapterCapability.SCHEMA, AdapterCapability.LINEAGE],
            skipped=[],
            probe_results={},
        )
        pipeline = ExtractionPipeline(adapter, self._persisted())

        import asyncio

        results, warnings = asyncio.run(pipeline.execute(plan))

        assert AdapterCapability.DISCOVER in results
        assert AdapterCapability.LINEAGE in results
        assert AdapterCapability.SCHEMA not in results
        assert len(warnings) == 1
        assert "boom" in warnings[0]


class _FakeConn:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))


class _FakeDB:
    def __init__(self):
        self.conn = _FakeConn()


class TestUpsertExtractionResult:
    def test_upsert_discovers(self) -> None:
        result = DiscoverySnapshot(
            meta=_meta(AdapterCapability.DISCOVER),
            containers=(DiscoveredContainer(container_id="db", container_type="database", display_name="DB"),),
        )
        db = _FakeDB()
        upsert_extraction_result(db, _ADAPTER_KEY, AdapterCapability.DISCOVER, result)
        assert len(db.conn.calls) == 1
        sql, params = db.conn.calls[0]
        assert "INSERT INTO v2_extraction_results" in sql
        assert params[1] == _ADAPTER_KEY
        assert params[3] == "discover"

    def test_upsert_schema(self) -> None:
        result = SchemaSnapshotV2(
            meta=_meta(AdapterCapability.SCHEMA, row_count=1),
            objects=(SchemaObject(schema_name="public", object_name="orders", kind=SchemaObjectKind.TABLE),),
        )
        db = _FakeDB()
        upsert_extraction_result(db, _ADAPTER_KEY, AdapterCapability.SCHEMA, result)
        assert db.conn.calls[0][1][3] == "schema"
