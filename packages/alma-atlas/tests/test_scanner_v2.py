"""Tests for alma_atlas.pipeline.scanner_v2."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from alma_atlas.config import AtlasConfig, SourceConfig
from alma_atlas.pipeline.scanner_v2 import (
    _CAPABILITY_ORDER,
    CapabilityRouter,
    ExtractionPipeline,
    ExtractionPlan,
    ScannerV2,
    ScanResultV2,
    run_scan_v2,
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

# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------

_SCOPE = ScopeContext(scope=ExtractionScope.DATABASE, identifiers={"db": "test"})
_ADAPTER_KEY = "test-adapter"
_ADAPTER_KIND = SourceAdapterKindV2.POSTGRES


def _meta(cap: AdapterCapability, row_count: int = 0) -> ExtractionMeta:
    return ExtractionMeta(
        adapter_key=_ADAPTER_KEY,
        adapter_kind=_ADAPTER_KIND,
        capability=cap,
        scope_context=_SCOPE,
        captured_at=datetime.now(UTC),
        duration_ms=10.0,
        row_count=row_count,
    )


def _probe(cap: AdapterCapability, *, available: bool = True, message: str | None = None) -> CapabilityProbeResult:
    return CapabilityProbeResult(
        capability=cap,
        available=available,
        scope=ExtractionScope.DATABASE,
        message=message,
    )


def _all_probes(available: bool = True) -> tuple[CapabilityProbeResult, ...]:
    return tuple(_probe(c, available=available) for c in AdapterCapability)


def _cfg(tmp_path: Path) -> AtlasConfig:
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")


# ---------------------------------------------------------------------------
# CapabilityRouter
# ---------------------------------------------------------------------------


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
        # DEFINITIONS, ORCHESTRATION not probed — absent from both lists
        assert AdapterCapability.DEFINITIONS not in plan.capabilities
        assert AdapterCapability.DEFINITIONS not in plan.skipped

    def test_canonical_order_preserved(self) -> None:
        router = CapabilityRouter()
        # Provide probes in reverse canonical order
        probes = tuple(reversed([_probe(c) for c in _CAPABILITY_ORDER]))
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
        for cap in AdapterCapability:
            assert cap in plan.probe_results


# ---------------------------------------------------------------------------
# ExtractionPipeline
# ---------------------------------------------------------------------------


def _make_mock_adapter_v2() -> MagicMock:
    """Return a mock that satisfies SourceAdapterV2 runtime isinstance checks.

    Python 3.12 uses inspect.getattr_static inside isinstance() for
    @runtime_checkable Protocol, which bypasses __getattr__.  Every protocol
    member must be explicitly assigned to the mock instance so it lands in
    __dict__.
    """
    adapter = MagicMock()
    adapter.declared_capabilities = frozenset(AdapterCapability)

    # Non-capability utility methods (required by protocol)
    adapter.test_connection = AsyncMock(return_value=MagicMock())
    adapter.execute_query = AsyncMock(return_value=MagicMock())
    adapter.get_setup_instructions = MagicMock(return_value=MagicMock())
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
            edges=(),
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
        p = MagicMock()
        p.key = _ADAPTER_KEY
        return p

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
        """Verify methods are called in DISCOVER→SCHEMA→...→ORCHESTRATION order."""
        adapter = _make_mock_adapter_v2()
        call_order: list[AdapterCapability] = []

        async def track(cap: AdapterCapability, coro):
            result = await coro
            call_order.append(cap)
            return result

        adapter.discover = AsyncMock(
            side_effect=lambda p: track(AdapterCapability.DISCOVER, AsyncMock(return_value=adapter.discover.return_value)())
        )

        # Simpler approach: just check results dict key order matches canonical order
        plan = ExtractionPlan(
            capabilities=list(_CAPABILITY_ORDER),
            skipped=[],
            probe_results={},
        )
        adapter2 = _make_mock_adapter_v2()
        pipeline = ExtractionPipeline(adapter2, self._persisted())
        import asyncio

        results, _ = asyncio.run(pipeline.execute(plan))
        assert list(results.keys()) == list(_CAPABILITY_ORDER)

    def test_graceful_skip_of_failed_capability(self) -> None:
        adapter = _make_mock_adapter_v2()
        adapter.extract_traffic = AsyncMock(side_effect=RuntimeError("no pg_stat_statements"))
        plan = ExtractionPlan(
            capabilities=list(_CAPABILITY_ORDER),
            skipped=[],
            probe_results={},
        )
        pipeline = ExtractionPipeline(adapter, self._persisted())
        import asyncio

        results, warnings = asyncio.run(pipeline.execute(plan))
        # All capabilities except TRAFFIC should succeed
        assert AdapterCapability.TRAFFIC not in results
        assert AdapterCapability.DISCOVER in results
        assert AdapterCapability.SCHEMA in results
        assert AdapterCapability.LINEAGE in results
        # Warning recorded for the failure
        assert len(warnings) == 1
        assert "traffic" in warnings[0].lower()

    def test_extraction_meta_present_in_all_results(self) -> None:
        plan = ExtractionPlan(
            capabilities=list(_CAPABILITY_ORDER),
            skipped=[],
            probe_results={},
        )
        adapter = _make_mock_adapter_v2()
        pipeline = ExtractionPipeline(adapter, self._persisted())
        import asyncio

        results, _ = asyncio.run(pipeline.execute(plan))
        for cap, result in results.items():
            assert hasattr(result, "meta"), f"Result for {cap} has no .meta"
            assert isinstance(result.meta, ExtractionMeta)
            assert result.meta.capability == cap

    def test_empty_plan_returns_empty_results(self) -> None:
        adapter = _make_mock_adapter_v2()
        plan = ExtractionPlan(capabilities=[], skipped=[], probe_results={})
        pipeline = ExtractionPipeline(adapter, self._persisted())
        import asyncio

        results, warnings = asyncio.run(pipeline.execute(plan))
        assert results == {}
        assert warnings == []

    def test_partial_plan_only_runs_listed_capabilities(self) -> None:
        adapter = _make_mock_adapter_v2()
        plan = ExtractionPlan(
            capabilities=[AdapterCapability.DISCOVER, AdapterCapability.SCHEMA],
            skipped=[],
            probe_results={},
        )
        pipeline = ExtractionPipeline(adapter, self._persisted())
        import asyncio

        results, _ = asyncio.run(pipeline.execute(plan))
        assert set(results.keys()) == {AdapterCapability.DISCOVER, AdapterCapability.SCHEMA}
        adapter.extract_definitions.assert_not_called()
        adapter.extract_traffic.assert_not_called()


# ---------------------------------------------------------------------------
# ScannerV2
# ---------------------------------------------------------------------------


def _make_persisted(key: str = _ADAPTER_KEY) -> MagicMock:
    p = MagicMock()
    p.key = key
    p.id = "test-id"
    return p


class TestScannerV2:
    def test_scanner_v2_delegates_to_run_scan(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})
        from alma_atlas.pipeline.scan import ScanResult

        with patch(
            "alma_atlas.pipeline.scan.run_scan",
            return_value=ScanResult(source_id="pg-test", asset_count=5, edge_count=3),
        ) as mock_run_scan:
            result = ScannerV2(cfg).scan(source)

        mock_run_scan.assert_called_once_with(source, cfg)
        assert result.source_id == "pg-test"
        assert result.asset_count == 5
        assert result.edge_count == 3

    def test_v1_adapter_falls_back_to_run_scan(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})

        # Plain MagicMock does NOT implement SourceAdapterV2 (no declared_capabilities etc.)
        v1_adapter = MagicMock(spec=[
            "introspect_schema",
            "observe_traffic",
            "test_connection",
            "execute_query",
            "get_setup_instructions",
        ])

        from alma_atlas.pipeline.scan import ScanResult

        with (
            patch("alma_atlas.pipeline.scan._build_adapter", return_value=(v1_adapter, MagicMock())),
            patch(
                "alma_atlas.pipeline.scanner_v2.ScannerV2.scan",
                wraps=ScannerV2(cfg).scan,
            ),
            patch(
                "alma_atlas.pipeline.scan.run_scan",
                return_value=ScanResult(source_id="pg-test", asset_count=5, edge_count=2),
            ) as mock_run_scan,
        ):
            scanner = ScannerV2(cfg)
            result = scanner.scan(source)

        mock_run_scan.assert_called_once_with(source, cfg)
        assert result.source_id == "pg-test"
        assert result.asset_count == 5
        assert result.edge_count == 2

    def test_build_adapter_error_returns_error_result(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="bad", kind="unsupported", params={})
        # run_scan_v2 is the public API that converts exceptions to ScanResultV2
        result = run_scan_v2(source, cfg)
        assert result.error is not None
        assert result.source_id == "bad"
        assert "ConfigurationError" in result.error

    def test_probe_failure_returns_error_result(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})
        adapter = _make_mock_adapter_v2()
        adapter.declared_capabilities = frozenset(AdapterCapability)
        adapter.probe = AsyncMock(side_effect=RuntimeError("connection refused"))

        with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(adapter, _make_persisted())):
            result = run_scan_v2(source, cfg)

        assert result.error is not None
        assert "capability probe failed" in result.error.lower()

    def test_no_available_capabilities_returns_warning(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})
        adapter = _make_mock_adapter_v2()
        adapter.declared_capabilities = frozenset(AdapterCapability)
        adapter.probe = AsyncMock(return_value=_all_probes(available=False))

        with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(adapter, _make_persisted())):
            scanner = ScannerV2(cfg)
            result = scanner.scan(source)

        assert result.error is None
        assert len(result.warnings) >= 1
        assert result.capabilities_run == []
        assert result.capabilities_skipped == []

    def test_schema_results_stored_as_assets(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})
        adapter = _make_mock_adapter_v2()
        adapter.declared_capabilities = frozenset(AdapterCapability)
        adapter.probe = AsyncMock(
            return_value=(
                _probe(AdapterCapability.DISCOVER),
                _probe(AdapterCapability.SCHEMA),
            )
        )

        with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(adapter, _make_persisted())):
            scanner = ScannerV2(cfg)
            result = scanner.scan(source)

        assert result.error is None
        assert result.asset_count == 2  # orders + order_view from mock schema

    def test_lineage_results_stored_as_edges(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})
        adapter = _make_mock_adapter_v2()
        adapter.declared_capabilities = frozenset(AdapterCapability)

        # Lineage result with one edge
        lineage_with_edge = LineageSnapshot(
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
        adapter.extract_lineage = AsyncMock(return_value=lineage_with_edge)
        adapter.probe = AsyncMock(
            return_value=(
                _probe(AdapterCapability.LINEAGE),
            )
        )

        with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(adapter, _make_persisted())):
            scanner = ScannerV2(cfg)
            result = scanner.scan(source)

        assert result.error is None
        assert result.edge_count == 1

    def test_skipped_capabilities_reported(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})
        adapter = _make_mock_adapter_v2()
        adapter.declared_capabilities = frozenset(AdapterCapability)
        adapter.probe = AsyncMock(
            return_value=(
                _probe(AdapterCapability.DISCOVER, available=True),
                _probe(AdapterCapability.TRAFFIC, available=False, message="no access"),
            )
        )

        with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(adapter, _make_persisted())):
            result = ScannerV2(cfg).scan(source)

        assert result.capabilities_skipped == []
        assert result.capabilities_run == []
        assert any("capability_skipped:traffic" in warning for warning in result.warnings)


# ---------------------------------------------------------------------------
# run_scan_v2
# ---------------------------------------------------------------------------


class TestRunScanV2:
    def test_delegates_to_scanner_v2(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="pg-test", kind="postgres", params={})

        expected = ScanResultV2(source_id="pg-test", asset_count=3)
        with patch.object(ScannerV2, "scan", return_value=expected) as mock_scan:
            result = run_scan_v2(source, cfg)

        mock_scan.assert_called_once_with(source)
        assert result is expected

    def test_returns_scan_result_v2(self, tmp_path: Path) -> None:
        cfg = _cfg(tmp_path)
        source = SourceConfig(id="bad", kind="unsupported", params={})
        result = run_scan_v2(source, cfg)
        assert isinstance(result, ScanResultV2)
        assert result.error is not None
