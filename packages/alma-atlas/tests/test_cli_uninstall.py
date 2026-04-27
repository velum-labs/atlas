"""Tests for `alma-atlas uninstall` — deletes ~/.alma-atlas/, idempotent."""

from __future__ import annotations

from unittest.mock import patch

from typer.testing import CliRunner

from alma_atlas.cli import uninstall as uninstall_cli

runner = CliRunner()


def test_uninstall_removes_directory_when_present(tmp_path):
    fake_home = tmp_path
    target = fake_home / ".alma-atlas"
    target.mkdir()
    (target / "secrets.json").write_text("encrypted-secret-blob")
    (target / "atlas.db").write_text("sqlite-bytes")

    with patch("alma_atlas.cli.uninstall.Path.home", return_value=fake_home):
        result = runner.invoke(uninstall_cli.app, ["--yes"])

    assert result.exit_code == 0
    assert not target.exists()
    assert "Removed:" in result.stdout


def test_uninstall_idempotent_when_directory_missing(tmp_path):
    fake_home = tmp_path
    # No ~/.alma-atlas directory
    with patch("alma_atlas.cli.uninstall.Path.home", return_value=fake_home):
        result = runner.invoke(uninstall_cli.app, ["--yes"])

    assert result.exit_code == 0
    assert "Nothing to uninstall" in result.stdout


def test_uninstall_aborts_when_user_declines_confirmation(tmp_path):
    fake_home = tmp_path
    target = fake_home / ".alma-atlas"
    target.mkdir()
    (target / "secrets.json").write_text("important-data")

    with patch("alma_atlas.cli.uninstall.Path.home", return_value=fake_home):
        # Stdin "n\n" -> user declines the typer.confirm prompt
        result = runner.invoke(uninstall_cli.app, [], input="n\n")

    assert result.exit_code == 0
    assert "Aborted" in result.stdout
    assert target.exists()  # nothing actually deleted
    assert (target / "secrets.json").read_text() == "important-data"


def test_uninstall_proceeds_when_user_confirms(tmp_path):
    fake_home = tmp_path
    target = fake_home / ".alma-atlas"
    target.mkdir()
    (target / "secrets.json").write_text("blob")

    with patch("alma_atlas.cli.uninstall.Path.home", return_value=fake_home):
        result = runner.invoke(uninstall_cli.app, [], input="y\n")

    assert result.exit_code == 0
    assert not target.exists()
