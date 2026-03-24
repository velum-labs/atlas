"""Tests for alma_atlas.pipeline.scan — run_scan() and _build_adapter()."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alma_atlas.config import AtlasConfig, SourceConfig
from alma_atlas.pipeline.scan import ScanResult, _build_adapter, run_scan

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

    source = SourceConfig(id="pg-mydb", kind="postgres", params={})
    _, persisted = _build_adapter(source)
    expected_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "pg-mydb"))
    assert persisted.id == expected_id


# ---------------------------------------------------------------------------
# run_scan — success path (mocked adapter)
# ---------------------------------------------------------------------------


def _make_scan_cfg(tmp_path: Path) -> AtlasConfig:
    """Create an AtlasConfig with a writable db path."""
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")


def test_run_scan_returns_asset_count(tmp_path: Path, mock_schema_snapshot, mock_traffic_result) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(return_value=mock_schema_snapshot)
    mock_adapter.observe_traffic = AsyncMock(return_value=mock_traffic_result)

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())),
        patch("alma_atlas.pipeline.stitch.stitch", return_value=0),
    ):
        result = run_scan(source, cfg)

    assert result.source_id == "pg-test"
    assert result.asset_count == 1
    assert result.error is None


def test_run_scan_returns_edge_count(tmp_path: Path, mock_schema_snapshot, mock_traffic_result) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(return_value=mock_schema_snapshot)
    mock_adapter.observe_traffic = AsyncMock(return_value=mock_traffic_result)

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())),
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


def test_run_scan_introspect_failure(tmp_path: Path) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(side_effect=RuntimeError("connection refused"))

    with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())):
        result = run_scan(source, cfg)

    assert result.error is not None
    assert "introspection" in result.error.lower()
    assert result.asset_count == 0


def test_run_scan_traffic_failure_returns_warning(tmp_path: Path, mock_schema_snapshot) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(return_value=mock_schema_snapshot)
    mock_adapter.observe_traffic = AsyncMock(side_effect=RuntimeError("traffic error"))

    with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())):
        result = run_scan(source, cfg)

    assert result.error is None
    assert result.asset_count == 1
    assert len(result.warnings) == 1
    assert "traffic" in result.warnings[0].lower()


def test_run_scan_empty_snapshot(tmp_path: Path, mock_traffic_result) -> None:
    cfg = _make_scan_cfg(tmp_path)
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    from alma_connectors.source_adapter import SchemaSnapshot

    empty_snapshot = SchemaSnapshot(captured_at=None, objects=(), dependencies=())

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(return_value=empty_snapshot)
    mock_adapter.observe_traffic = AsyncMock(return_value=mock_traffic_result)

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())),
        patch("alma_atlas.pipeline.stitch.stitch", return_value=0),
    ):
        result = run_scan(source, cfg)

    assert result.asset_count == 0
    assert result.error is None
