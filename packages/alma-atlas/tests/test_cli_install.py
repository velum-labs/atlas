"""Tests for `alma-atlas install cursor` and `alma-atlas install claude`.

Verifies the merge logic, .bak fallback on parse failure, --scope flag,
detection of "client not installed", and token validation at the CLI boundary.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from alma_atlas.cli import install as install_cli

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_cursor_present():
    """Treat Cursor as installed for the duration of a test."""
    with patch.object(install_cli, "_cursor_appears_installed", return_value=True):
        yield


@pytest.fixture
def mock_cursor_absent():
    with patch.object(install_cli, "_cursor_appears_installed", return_value=False):
        yield


@pytest.fixture
def mock_claude_present():
    with patch.object(install_cli, "_claude_desktop_appears_installed", return_value=True):
        yield


@pytest.fixture
def mock_claude_absent():
    with patch.object(install_cli, "_claude_desktop_appears_installed", return_value=False):
        yield


# ---------------------------------------------------------------------------
# install cursor: client detection
# ---------------------------------------------------------------------------


def test_install_cursor_aborts_when_cursor_not_installed(mock_cursor_absent):
    result = runner.invoke(install_cli.app, ["cursor"])
    assert result.exit_code == 1
    assert "doesn't appear to be installed" in result.stdout


def test_install_claude_aborts_when_claude_not_installed(mock_claude_absent):
    result = runner.invoke(install_cli.app, ["claude"])
    assert result.exit_code == 1
    assert "doesn't appear to be installed" in result.stdout


# ---------------------------------------------------------------------------
# install cursor: --scope
# ---------------------------------------------------------------------------


def test_install_cursor_writes_to_global_path_by_default(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor"])
    assert result.exit_code == 0
    assert target.exists()
    cfg = json.loads(target.read_text())
    assert "atlas" in cfg["mcpServers"]
    assert cfg["mcpServers"]["atlas"]["command"] == "alma-atlas"
    assert cfg["mcpServers"]["atlas"]["args"] == ["serve"]


def test_install_cursor_writes_to_project_path_with_project_scope(tmp_path, mock_cursor_present, monkeypatch):
    monkeypatch.chdir(tmp_path)
    project_target = tmp_path / ".cursor" / "mcp.json"
    with patch.object(install_cli, "_CURSOR_PROJECT_PATH", Path(".cursor/mcp.json")):
        result = runner.invoke(install_cli.app, ["cursor", "--scope", "project"])
    assert result.exit_code == 0
    assert project_target.exists()


def test_install_cursor_rejects_invalid_scope(mock_cursor_present):
    result = runner.invoke(install_cli.app, ["cursor", "--scope", "weird"])
    assert result.exit_code == 1
    assert "Invalid --scope" in result.stdout


# ---------------------------------------------------------------------------
# install cursor: --token validation
# ---------------------------------------------------------------------------


def test_install_cursor_rejects_short_token(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor", "--token", "short"])
    assert result.exit_code == 1
    assert "Invalid --token" in result.stdout


def test_install_cursor_with_token_writes_alma_token_arg(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    long_token = "a" * 32
    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor", "--token", long_token])
    assert result.exit_code == 0
    cfg = json.loads(target.read_text())
    args = cfg["mcpServers"]["atlas"]["args"]
    assert "--alma-token" in args
    assert long_token in args


# ---------------------------------------------------------------------------
# install cursor: merge with existing config
# ---------------------------------------------------------------------------


def test_install_cursor_preserves_existing_mcp_servers(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "dbt-mcp": {"command": "dbt-mcp", "args": ["serve"]},
                    "snowflake-mcp": {"command": "snow-mcp"},
                },
                "otherTopLevelKey": {"x": 1},
            },
            indent=2,
        )
    )

    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor"])

    assert result.exit_code == 0
    cfg = json.loads(target.read_text())
    # Atlas added alongside existing servers
    assert set(cfg["mcpServers"].keys()) == {"dbt-mcp", "snowflake-mcp", "atlas"}
    # Other top-level keys preserved
    assert cfg["otherTopLevelKey"] == {"x": 1}


def test_install_cursor_overwrites_existing_atlas_entry(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps({"mcpServers": {"atlas": {"command": "old-atlas", "args": ["x"]}}})
    )

    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor"])

    assert result.exit_code == 0
    cfg = json.loads(target.read_text())
    # Updated to point at alma-atlas
    assert cfg["mcpServers"]["atlas"]["command"] == "alma-atlas"


def test_install_cursor_creates_config_when_missing(tmp_path, mock_cursor_present):
    target = tmp_path / "newdir" / "mcp.json"  # parent doesn't exist
    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor"])
    assert result.exit_code == 0
    assert target.exists()
    cfg = json.loads(target.read_text())
    assert "atlas" in cfg["mcpServers"]


# ---------------------------------------------------------------------------
# install cursor: .bak fallback on malformed config
# ---------------------------------------------------------------------------


def test_install_cursor_backs_up_malformed_json(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{ this isn't valid json }")

    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor"])

    assert result.exit_code == 0
    backup = target.with_suffix(target.suffix + ".bak")
    assert backup.exists(), "Malformed config should have been backed up to .bak"
    assert backup.read_text() == "{ this isn't valid json }"
    cfg = json.loads(target.read_text())
    assert "atlas" in cfg["mcpServers"]


def test_install_cursor_backs_up_when_root_isnt_object(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(["this", "is", "an", "array"]))

    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor"])

    assert result.exit_code == 0
    backup = target.with_suffix(target.suffix + ".bak")
    assert backup.exists()


def test_install_cursor_backs_up_when_mcp_servers_wrong_shape(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"mcpServers": ["this", "should", "be", "a", "dict"]}))

    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        result = runner.invoke(install_cli.app, ["cursor"])

    assert result.exit_code == 0
    backup = target.with_suffix(target.suffix + ".bak")
    assert backup.exists()
    cfg = json.loads(target.read_text())
    assert isinstance(cfg["mcpServers"], dict)
    assert "atlas" in cfg["mcpServers"]


def test_install_cursor_backup_does_not_clobber_existing_bak(tmp_path, mock_cursor_present):
    target = tmp_path / "cursor_global.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{not valid")
    existing_bak = target.with_suffix(target.suffix + ".bak")
    existing_bak.write_text("PREVIOUS BACKUP")

    with patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target):
        runner.invoke(install_cli.app, ["cursor"])

    # Original .bak preserved; new backup gets a numeric suffix
    assert existing_bak.read_text() == "PREVIOUS BACKUP"
    assert target.with_suffix(target.suffix + ".bak.1").exists()


# ---------------------------------------------------------------------------
# install claude: end-to-end on Mac path layout
# ---------------------------------------------------------------------------


def test_install_claude_writes_to_resolved_config_path(tmp_path, mock_claude_present):
    fake_path = tmp_path / "claude" / "config.json"
    with patch.object(install_cli, "_claude_desktop_config_path", return_value=fake_path):
        result = runner.invoke(install_cli.app, ["claude"])
    assert result.exit_code == 0
    cfg = json.loads(fake_path.read_text())
    assert cfg["mcpServers"]["atlas"]["command"] == "alma-atlas"
