"""Tests for alma_atlas.pipeline.scan — run_scan() and _build_adapter()."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alma_atlas.config import AtlasConfig, SourceConfig
from alma_atlas.pipeline.scan import ScanResult, _build_adapter, run_scan
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    ExtractionMeta,
    ExtractionScope,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

# ---------------------------------------------------------------------------
# ScanResult dataclass
# ---------------------------------------------------------------------------


def test_scan_result_defaults() -> None:
    r = ScanResult(source_id="test")
    assert r.asset_count == 0
    assert r.edge_count == 0
    assert r.error is None
    assert r.warnings == []


def test_scan_result_with_values() -> None:
    r = ScanResult(source_id="test", asset_count=5, edge_count=3, error="oops", warnings=["w1"])
    assert r.asset_count == 5
    assert r.edge_count == 3
    assert r.error == "oops"
    assert r.warnings == ["w1"]


# ---------------------------------------------------------------------------
# _build_adapter — error cases
# ---------------------------------------------------------------------------


def test_build_adapter_unknown_kind_raises() -> None:
    source = SourceConfig(id="x", kind="mysql", params={})
    with pytest.raises(ValueError, match="Unknown source kind"):
        _build_adapter(source)


def test_build_adapter_postgres_returns_adapter() -> None:
    source = SourceConfig(id="pg-mydb", kind="postgres", params={"dsn_env": "PG_URL"})
    adapter, persisted = _build_adapter(source)
    from alma_connectors.adapters.postgres import PostgresAdapter

    assert isinstance(adapter, PostgresAdapter)
    assert persisted.key == "pg-mydb"


def test_build_adapter_bigquery_returns_adapter() -> None:
    source = SourceConfig(
        id="bq-proj",
        kind="bigquery",
        params={"project_id": "my-proj", "service_account_env": "BQ_SA"},
    )
    adapter, persisted = _build_adapter(source)
    from alma_connectors.adapters.bigquery import BigQueryAdapter

    assert isinstance(adapter, BigQueryAdapter)
    assert persisted.key == "bq-proj"


def test_build_adapter_uses_uuid5_for_id() -> None:
    import uuid

    source = SourceConfig(id="pg-mydb", kind="postgres", params={"dsn_env": "PG_URL"})
    _, persisted = _build_adapter(source)
    expected_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "pg-mydb"))
    assert persisted.id == expected_id


# ---------------------------------------------------------------------------
# run_scan — success path (mocked adapter)
# ---------------------------------------------------------------------------


def _make_scan_cfg(tmp_path: Path) -> AtlasConfig:
    """Create an AtlasConfig with a writable db path."""
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")


def _probe(capability: AdapterCapability, *, available: bool = True, message: str | None = None) -> CapabilityProbeResult:
    return CapabilityProbeResult(
        capability=capability,
        available=available,
        scope=ExtractionScope.DATABASE,
        scope_context=ScopeContext(scope=ExtractionScope.DATABASE, identifiers={"db": "test"}),
        message=message,
    )


def _make_v2_adapter(
    *,
    schema_result: object,
    traffic_result: object | Exception,
) -> MagicMock:
    adapter = MagicMock()
    adapter.declared_capabilities = frozenset({AdapterCapability.SCHEMA, AdapterCapability.TRAFFIC})
    adapter.test_connection = AsyncMock(return_value=MagicMock())
    adapter.execute_query = AsyncMock(return_value=MagicMock())
    adapter.get_setup_instructions = MagicMock(return_value=MagicMock())
    adapter.probe = AsyncMock(
        return_value=(
            _probe(AdapterCapability.SCHEMA, available=True),
            _probe(AdapterCapability.TRAFFIC, available=True),
        )
    )
    adapter.discover = AsyncMock(side_effect=NotImplementedError)
    adapter.extract_definitions = AsyncMock(side_effect=NotImplementedError)
    adapter.extract_lineage = AsyncMock(side_effect=NotImplementedError)
    adapter.extract_orchestration = AsyncMock(side_effect=NotImplementedError)
    adapter.extract_schema = AsyncMock(return_value=schema_result)
    if isinstance(traffic_result, Exception):
        adapter.extract_traffic = AsyncMock(side_effect=traffic_result)
    else:
        adapter.extract_traffic = AsyncMock(return_value=traffic_result)
    return adapter


def _make_v2_schema_snapshot(mock_schema_snapshot):
    from datetime import UTC, datetime

    from alma_connectors.source_adapter_v2 import SchemaObject, SchemaObjectKind, SchemaSnapshotV2

    return SchemaSnapshotV2(
        meta=ExtractionMeta(
            adapter_key="pg-test",
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.SCHEMA,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE, identifiers={"db": "test"}),
            captured_at=mock_schema_snapshot.captured_at or datetime.now(UTC),
            duration_ms=10.0,
            row_count=len(mock_schema_snapshot.objects),
        ),
        objects=tuple(
            SchemaObject(
                schema_name=obj.schema_name,
                object_name=obj.object_name,
                kind=SchemaObjectKind.TABLE,
            )
            for obj in mock_schema_snapshot.objects
        ),
    )


def _make_v2_traffic_result(mock_traffic_result):
    from datetime import UTC, datetime

    return TrafficExtractionResult(
        meta=ExtractionMeta(
            adapter_key="pg-test",
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.TRAFFIC,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE, identifiers={"db": "test"}),
            captured_at=datetime.now(UTC),
            duration_ms=10.0,
            row_count=len(mock_traffic_result.events),
        ),
        events=mock_traffic_result.events,
        observation_cursor=mock_traffic_result.observation_cursor,
    )


def test_run_scan_returns_asset_count(tmp_path: Path, mock_schema_snapshot, mock_traffic_result) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = _make_v2_adapter(
        schema_result=_make_v2_schema_snapshot(mock_schema_snapshot),
        traffic_result=_make_v2_traffic_result(mock_traffic_result),
    )

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock(key="pg-test"))),
        patch("alma_atlas.pipeline.stitch.stitch", return_value=0),
    ):
        result = run_scan(source, cfg)

    assert result.source_id == "pg-test"
    assert result.asset_count == 1
    assert result.error is None


def test_run_scan_returns_edge_count(tmp_path: Path, mock_schema_snapshot, mock_traffic_result) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = _make_v2_adapter(
        schema_result=_make_v2_schema_snapshot(mock_schema_snapshot),
        traffic_result=_make_v2_traffic_result(mock_traffic_result),
    )

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock(key="pg-test"))),
        patch("alma_atlas.pipeline.stitch.stitch", return_value=3),
    ):
        result = run_scan(source, cfg)

    assert result.edge_count == 3


def test_run_scan_build_adapter_error(tmp_path: Path) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="bad", kind="unsupported_kind", params={})
    result = run_scan(source, cfg)
    assert result.error is not None
    assert result.asset_count == 0


def test_run_scan_schema_failure(tmp_path: Path) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = _make_v2_adapter(
        schema_result=MagicMock(),
        traffic_result=MagicMock(),
    )
    mock_adapter.extract_schema = AsyncMock(side_effect=RuntimeError("connection refused"))

    with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock(key="pg-test"))):
        result = run_scan(source, cfg)

    assert result.error is not None
    assert "schema extraction failed" in result.error.lower()
    assert result.asset_count == 0


def test_run_scan_traffic_failure_returns_warning(tmp_path: Path, mock_schema_snapshot) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = _make_v2_adapter(
        schema_result=_make_v2_schema_snapshot(mock_schema_snapshot),
        traffic_result=RuntimeError("traffic error"),
    )

    with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock(key="pg-test"))):
        result = run_scan(source, cfg)

    assert result.error is None
    assert result.asset_count == 1
    assert len(result.warnings) >= 1
    assert "traffic" in result.warnings[0].lower()


def test_run_scan_empty_snapshot(tmp_path: Path, mock_traffic_result) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    from datetime import UTC, datetime

    from alma_connectors.source_adapter_v2 import SchemaSnapshotV2

    empty_snapshot = SchemaSnapshotV2(
        meta=ExtractionMeta(
            adapter_key="pg-test",
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.SCHEMA,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE, identifiers={"db": "test"}),
            captured_at=datetime.now(UTC),
            duration_ms=10.0,
            row_count=0,
        ),
        objects=(),
    )

    mock_adapter = _make_v2_adapter(
        schema_result=empty_snapshot,
        traffic_result=_make_v2_traffic_result(mock_traffic_result),
    )

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock(key="pg-test"))),
        patch("alma_atlas.pipeline.stitch.stitch", return_value=0),
    ):
        result = run_scan(source, cfg)

    assert result.asset_count == 0
    assert result.error is None
