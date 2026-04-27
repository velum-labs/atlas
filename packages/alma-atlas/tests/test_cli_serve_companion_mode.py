"""Tests for `alma-atlas serve --alma-token` wiring.

Verifies that the CLI flag (and ALMA_INVITE_TOKEN env var) flips the serve
command into Companion mode: passes COMPANION_CATEGORY_MODULES to
create_server and supplies a token_validator closure that calls validate_token
with the configured token + endpoint.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from alma_atlas.cli import serve as serve_cli
from alma_atlas.mcp import tools

runner = CliRunner()


def _no_op_run(coro=None, *_args, **_kwargs) -> None:
    """Stand-in for asyncio.run so we don't actually start the MCP server.

    Closes the passed coroutine to silence "never awaited" RuntimeWarnings.
    """
    if coro is not None and hasattr(coro, "close"):
        coro.close()


def test_default_serve_passes_no_modules_no_validator():
    """`alma-atlas serve` (no --alma-token) -> default surface, no auth."""
    fake_server = MagicMock()
    with (
        patch("alma_atlas.mcp.server.create_server", return_value=fake_server) as mock_create,
        patch("alma_atlas.bootstrap.load_config", return_value=MagicMock()),
        patch("asyncio.run", _no_op_run),
    ):
        result = runner.invoke(serve_cli.app, [])

    assert result.exit_code == 0
    mock_create.assert_called_once()
    _args, kwargs = mock_create.call_args
    assert kwargs["modules"] is None
    assert kwargs["token_validator"] is None


def test_alma_token_flag_switches_to_companion_modules():
    """`--alma-token X` -> COMPANION_CATEGORY_MODULES + validator."""
    fake_server = MagicMock()
    with (
        patch("alma_atlas.mcp.server.create_server", return_value=fake_server) as mock_create,
        patch("alma_atlas.bootstrap.load_config", return_value=MagicMock()),
        patch("asyncio.run", _no_op_run),
    ):
        result = runner.invoke(serve_cli.app, ["--alma-token", "a" * 32])

    assert result.exit_code == 0
    _args, kwargs = mock_create.call_args
    assert kwargs["modules"] is tools.COMPANION_CATEGORY_MODULES
    assert callable(kwargs["token_validator"])


def test_companion_validator_calls_validate_token_with_supplied_endpoint():
    """The validator closure forwards token + endpoint to auth.validate_token."""
    fake_server = MagicMock()
    with (
        patch("alma_atlas.mcp.server.create_server", return_value=fake_server) as mock_create,
        patch("alma_atlas.bootstrap.load_config", return_value=MagicMock()),
        patch("alma_atlas.auth.invite_token.validate_token") as mock_validate,
        patch("asyncio.run", _no_op_run),
    ):
        runner.invoke(
            serve_cli.app,
            ["--alma-token", "a" * 32, "--alma-endpoint", "https://staging.alma.dev"],
        )
        validator = mock_create.call_args.kwargs["token_validator"]
        validator()  # invoke the closure

    mock_validate.assert_called_once_with("a" * 32, "https://staging.alma.dev")


def test_alma_token_via_env_var_also_triggers_companion_mode(monkeypatch):
    """ALMA_INVITE_TOKEN env var is equivalent to --alma-token."""
    fake_server = MagicMock()
    monkeypatch.setenv("ALMA_INVITE_TOKEN", "x" * 32)
    with (
        patch("alma_atlas.mcp.server.create_server", return_value=fake_server) as mock_create,
        patch("alma_atlas.bootstrap.load_config", return_value=MagicMock()),
        patch("asyncio.run", _no_op_run),
    ):
        result = runner.invoke(serve_cli.app, [])

    assert result.exit_code == 0
    kwargs = mock_create.call_args.kwargs
    assert kwargs["modules"] is tools.COMPANION_CATEGORY_MODULES
    assert callable(kwargs["token_validator"])


def test_alma_endpoint_defaults_to_app_alma_dev():
    """Without --alma-endpoint, the validator hits app.alma.dev."""
    fake_server = MagicMock()
    with (
        patch("alma_atlas.mcp.server.create_server", return_value=fake_server) as mock_create,
        patch("alma_atlas.bootstrap.load_config", return_value=MagicMock()),
        patch("alma_atlas.auth.invite_token.validate_token") as mock_validate,
        patch("asyncio.run", _no_op_run),
    ):
        runner.invoke(serve_cli.app, ["--alma-token", "a" * 32])
        validator = mock_create.call_args.kwargs["token_validator"]
        validator()

    mock_validate.assert_called_once_with("a" * 32, "https://app.alma.dev")
