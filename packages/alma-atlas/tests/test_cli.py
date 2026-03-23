"""Tests for alma_atlas CLI commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from alma_atlas.cli.main import app
from alma_atlas.config import AtlasConfig, SourceConfig

runner = CliRunner()


def _cfg(tmp_path: Path) -> AtlasConfig:
    return AtlasConfig(config_dir=tmp_path / "alma")


# ---------------------------------------------------------------------------
# --version flag
# ---------------------------------------------------------------------------


def test_version_flag() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "alma-atlas" in result.output


# ---------------------------------------------------------------------------
# connect bigquery
# ---------------------------------------------------------------------------


def test_connect_bigquery(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "bigquery", "--project", "my-project"])
    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert len(sources) == 1
    assert sources[0].kind == "bigquery"
    assert sources[0].params["project"] == "my-project"


def test_connect_bigquery_with_credentials(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app, ["connect", "bigquery", "--project", "proj", "--credentials", "/path/to/creds.json"]
        )
    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].params.get("credentials") == "/path/to/creds.json"


# ---------------------------------------------------------------------------
# connect postgres
# ---------------------------------------------------------------------------


def test_connect_postgres(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "postgres", "--dsn", "postgresql://user:pass@localhost/mydb"])
    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].id == "postgres:mydb"
    assert sources[0].kind == "postgres"


# ---------------------------------------------------------------------------
# connect dbt
# ---------------------------------------------------------------------------


def test_connect_dbt(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    manifest = str(tmp_path / "manifest.json")
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "dbt", "--manifest", manifest])
    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].kind == "dbt"
    assert sources[0].params["manifest_path"] == manifest


# ---------------------------------------------------------------------------
# connect list
# ---------------------------------------------------------------------------


def test_connect_list_no_sources(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "list"])
    assert result.exit_code == 0
    assert "No sources" in result.output


def test_connect_list_with_sources(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "list"])
    assert result.exit_code == 0
    assert "pg:mydb" in result.output


# ---------------------------------------------------------------------------
# connect remove
# ---------------------------------------------------------------------------


def test_connect_remove_existing(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "remove", "pg:mydb"])
    assert result.exit_code == 0
    assert cfg.load_sources() == []


def test_connect_remove_nonexistent(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "remove", "nonexistent"])
    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# scan — no sources
# ---------------------------------------------------------------------------


def test_scan_no_sources(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.scan.get_config", return_value=cfg):
        result = runner.invoke(app, ["scan"])
    assert result.exit_code == 1
    assert "No sources" in result.output


def test_scan_dry_run(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.scan.get_config", return_value=cfg):
        result = runner.invoke(app, ["scan", "--dry-run"])
    assert result.exit_code == 0
    assert "Dry run" in result.output
    assert "pg:mydb" in result.output


def test_scan_source_not_found(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.scan.get_config", return_value=cfg):
        result = runner.invoke(app, ["scan", "--source", "nonexistent"])
    assert result.exit_code == 1
