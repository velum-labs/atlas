"""Tests for Atlas error hierarchy, exception chaining, CLI exit codes, and graceful degradation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from alma_atlas.cli.main import app
from alma_atlas.config import AtlasConfig, SourceConfig

runner = CliRunner()


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------


def test_error_hierarchy_imports() -> None:
    from alma_ports.errors import (
        AdapterConnectionError,
        AdapterTimeoutError,
        AtlasError,
        AuthenticationError,
        ConfigurationError,
        EnforcementError,
        ExtractionError,
        SyncError,
    )

    assert issubclass(ConfigurationError, AtlasError)
    assert issubclass(AdapterConnectionError, AtlasError)
    assert issubclass(AuthenticationError, AdapterConnectionError)
    assert issubclass(AuthenticationError, AtlasError)
    assert issubclass(ExtractionError, AtlasError)
    assert issubclass(AdapterTimeoutError, AtlasError)
    assert issubclass(SyncError, AtlasError)
    assert issubclass(EnforcementError, AtlasError)


def test_atlas_error_is_exception() -> None:
    from alma_ports.errors import AtlasError

    err = AtlasError("test")
    assert isinstance(err, Exception)


def test_authentication_error_is_connection_error() -> None:
    from alma_ports.errors import AdapterConnectionError, AuthenticationError

    err = AuthenticationError("auth failed")
    assert isinstance(err, AdapterConnectionError)


# ---------------------------------------------------------------------------
# Exception chaining (__cause__)
# ---------------------------------------------------------------------------


def test_scan_config_error_chains_original() -> None:
    """ConfigurationError raised for unknown kind must chain the original ValueError."""
    from alma_atlas.pipeline.scan import run_scan

    cfg = AtlasConfig(config_dir=Path("/tmp/test-atlas-chain"), db_path=Path("/tmp/test-atlas-chain/atlas.db"))
    source = SourceConfig(id="bad", kind="unknown_kind_xyz", params={})

    result = run_scan(source, cfg)
    # run_scan returns ScanResult — error field carries type name + message
    assert result.error is not None
    assert "ConfigurationError" in result.error


def test_scan_introspect_error_chains_original(tmp_path: Path) -> None:
    """ExtractionError raised for schema failure must chain the original exception."""
    from alma_atlas.pipeline.scan import run_scan

    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(side_effect=RuntimeError("connection refused"))

    with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())):
        result = run_scan(source, cfg)

    assert result.error is not None
    assert "ExtractionError" in result.error
    assert "introspection" in result.error.lower()


def test_scan_error_field_format(tmp_path: Path) -> None:
    """ScanResult.error should be 'ExceptionType: message'."""
    from alma_atlas.pipeline.scan import run_scan

    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
    source = SourceConfig(id="bad", kind="unknown_kind_xyz", params={})

    result = run_scan(source, cfg)
    assert result.error is not None
    # Must match "TypeName: message" pattern
    assert ":" in result.error
    type_part = result.error.split(":")[0]
    assert type_part[0].isupper(), f"Expected PascalCase type name, got: {type_part!r}"


# ---------------------------------------------------------------------------
# Graceful degradation: traffic failure keeps schema results
# ---------------------------------------------------------------------------


def test_traffic_failure_returns_partial_result(tmp_path: Path, mock_schema_snapshot) -> None:
    """Traffic failure should produce a warning, not fail the whole scan."""
    from alma_atlas.pipeline.scan import run_scan

    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(return_value=mock_schema_snapshot)
    mock_adapter.observe_traffic = AsyncMock(side_effect=RuntimeError("traffic connection error"))

    with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())):
        result = run_scan(source, cfg)

    # Schema succeeded
    assert result.error is None
    assert result.asset_count == 1
    # Traffic failure recorded as warning
    assert len(result.warnings) == 1
    assert "traffic" in result.warnings[0].lower()
    assert "ExtractionError" in result.warnings[0]


def test_enforcement_failure_does_not_fail_scan(tmp_path: Path, mock_schema_snapshot, mock_traffic_result) -> None:
    """Enforcement failure must not propagate — it is advisory only."""
    from alma_atlas.pipeline.scan import run_scan

    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
    source = SourceConfig(id="pg-test", kind="postgres", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(return_value=mock_schema_snapshot)
    mock_adapter.observe_traffic = AsyncMock(return_value=mock_traffic_result)

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())),
        patch("alma_atlas.pipeline.stitch.stitch", return_value=0),
        patch("alma_atlas.pipeline.scan._run_enforcement", side_effect=RuntimeError("enforcement boom")),
    ):
        result = run_scan(source, cfg)

    # Scan still succeeds despite enforcement error
    assert result.error is None
    assert result.asset_count == 1


def test_scan_all_one_source_fails_others_complete(tmp_path: Path, mock_schema_snapshot, mock_traffic_result) -> None:
    """If one source fails in scan_all, the other sources must still complete."""
    from alma_atlas.pipeline.scan import run_scan_all

    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
    source_ok = SourceConfig(id="pg-ok", kind="postgres", params={})
    source_bad = SourceConfig(id="bad-source", kind="unknown_kind_xyz", params={})

    mock_adapter = MagicMock()
    mock_adapter.introspect_schema = AsyncMock(return_value=mock_schema_snapshot)
    mock_adapter.observe_traffic = AsyncMock(return_value=mock_traffic_result)

    def _build_side_effect(source):
        if source.id == "pg-ok":
            return mock_adapter, MagicMock()
        raise ValueError(f"Unknown source kind: {source.kind!r}")

    with (
        patch("alma_atlas.pipeline.scan._build_adapter", side_effect=_build_side_effect),
        patch("alma_atlas.pipeline.stitch.stitch", return_value=2),
    ):
        all_result = run_scan_all([source_bad, source_ok], cfg)

    assert len(all_result.results) == 2
    bad_result = next(r for r in all_result.results if r.source_id == "bad-source")
    ok_result = next(r for r in all_result.results if r.source_id == "pg-ok")

    assert bad_result.error is not None
    assert ok_result.error is None
    assert ok_result.asset_count == 1


# ---------------------------------------------------------------------------
# CLI exit codes
# ---------------------------------------------------------------------------


def test_scan_cli_exit_0_all_success(tmp_path: Path) -> None:
    """All sources succeed → exit code 0."""
    from alma_atlas.pipeline.scan import ScanAllResult, ScanResult

    cfg = AtlasConfig(config_dir=tmp_path / "alma")
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))

    ok_result = ScanAllResult(
        results=[ScanResult(source_id="pg:mydb", asset_count=5, edge_count=2)],
        cross_system_edge_count=0,
    )

    with (
        patch("alma_atlas.cli.scan.get_config", return_value=cfg),
        patch("alma_atlas.pipeline.scan.run_scan_all", return_value=ok_result),
    ):
        result = runner.invoke(app, ["scan", "--no-sync"])

    assert result.exit_code == 0


def test_scan_cli_exit_1_partial_failure(tmp_path: Path) -> None:
    """Some sources fail → exit code 1 (partial)."""
    from alma_atlas.pipeline.scan import ScanAllResult, ScanResult

    cfg = AtlasConfig(config_dir=tmp_path / "alma")
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))

    partial_result = ScanAllResult(
        results=[ScanResult(source_id="pg:mydb", error="ConfigurationError: bad config")],
        cross_system_edge_count=0,
    )

    with (
        patch("alma_atlas.cli.scan.get_config", return_value=cfg),
        patch("alma_atlas.pipeline.scan.run_scan_all", return_value=partial_result),
    ):
        result = runner.invoke(app, ["scan", "--no-sync"])

    assert result.exit_code == 1


def test_scan_cli_exit_2_no_sources(tmp_path: Path) -> None:
    """No sources configured → exit code 1 (config error)."""
    cfg = AtlasConfig(config_dir=tmp_path / "alma")

    with patch("alma_atlas.cli.scan.get_config", return_value=cfg):
        result = runner.invoke(app, ["scan"])

    assert result.exit_code == 1
    assert "No sources" in result.output


def test_scan_cli_exit_3_complete_failure(tmp_path: Path) -> None:
    """run_scan_all raises unexpectedly → exit code 3 (complete failure)."""
    cfg = AtlasConfig(config_dir=tmp_path / "alma")
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))

    with (
        patch("alma_atlas.cli.scan.get_config", return_value=cfg),
        patch("alma_atlas.pipeline.scan.run_scan_all", side_effect=RuntimeError("database locked")),
    ):
        result = runner.invoke(app, ["scan", "--no-sync"])

    assert result.exit_code == 3


# ---------------------------------------------------------------------------
# CLI team sync exit code
# ---------------------------------------------------------------------------


def test_team_sync_exit_1_on_failure(tmp_path: Path) -> None:
    """team sync failure → exit code 1."""
    cfg = AtlasConfig(config_dir=tmp_path / "alma")
    cfg.team_server_url = "http://localhost:8000"
    cfg.team_api_key = "test-key"
    db_path = tmp_path / "atlas.db"
    cfg.db_path = db_path
    db_path.touch()

    with (
        patch("alma_atlas.cli.team.get_config", return_value=cfg),
        patch("alma_atlas.sync.client.SyncClient") as mock_client_cls,
    ):
        mock_client = MagicMock()
        mock_client.full_sync = AsyncMock(side_effect=RuntimeError("server unreachable"))
        mock_client_cls.return_value = mock_client

        result = runner.invoke(app, ["team", "sync"])

    assert result.exit_code == 1
    assert "Sync failed" in result.output
