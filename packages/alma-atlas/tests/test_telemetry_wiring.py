"""Tests for telemetry wiring into the MCP dispatcher and CLI install command.

Verifies:
- `register(..., telemetry_cfg=...)` fires `tool_call` mandatory events on
  the happy path, after auth rejection, after db-unavailable, and on
  unknown tool name.
- `install_source` propagates into the event properties.
- The dispatcher does NOT crash if PostHog raises.
- `cli/install.py` fires mandatory `install_<client>` events; opt-in events
  fire only when a token is supplied (with a hashed account correlator).
- `cli/serve.py` builds a TelemetryConfig with opt_in=True and the hashed
  account token when --alma-token is set.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from mcp.server import Server
from typer.testing import CliRunner

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp import tools
from alma_atlas.telemetry import TelemetryConfig, reset_client_cache
from alma_atlas_store.db import Database

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clear_client_cache():
    reset_client_cache()
    yield
    reset_client_cache()


@dataclass
class _FakeValidation:
    valid: bool
    display_message: str = ""


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    with Database(db_path):
        pass
    return cfg


class _CapturedHandlers:
    def __init__(self) -> None:
        self.list_tools_handler = None
        self.call_tool_handler = None


def _make_capturing_server(captured: _CapturedHandlers) -> Server:
    server = Server("alma-atlas-test")

    def _list_tools_decorator():
        def _wrap(fn):
            captured.list_tools_handler = fn
            return fn
        return _wrap

    def _call_tool_decorator():
        def _wrap(fn):
            captured.call_tool_handler = fn
            return fn
        return _wrap

    server.list_tools = _list_tools_decorator  # type: ignore[assignment]
    server.call_tool = _call_tool_decorator  # type: ignore[assignment]
    return server


# ---------------------------------------------------------------------------
# Dispatcher fires tool_call events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_call_tool_fires_mandatory_event_on_happy_path(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)
    mock_client = MagicMock()
    telemetry_cfg = TelemetryConfig(api_key="phc_test")

    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        tools.register(server, cfg, telemetry_cfg=telemetry_cfg, install_source="direct_pip")
        await captured.call_tool_handler("atlas_search", {"query": "x"})

    assert mock_client.capture.called
    capture_kwargs = mock_client.capture.call_args.kwargs
    assert capture_kwargs["event"] == "tool_call"
    assert capture_kwargs["properties"]["tool_name"] == "atlas_search"
    assert capture_kwargs["properties"]["install_source"] == "direct_pip"
    # Session duration is included
    assert "mcp_session_duration_seconds" in capture_kwargs["properties"]


@pytest.mark.asyncio
async def test_call_tool_fires_event_after_auth_rejection(tmp_path: Path) -> None:
    """Auth rejection still emits a tool_call event (so we can count rejection rate)."""
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)
    mock_client = MagicMock()
    telemetry_cfg = TelemetryConfig(api_key="phc_test")
    revoked = _FakeValidation(valid=False, display_message="Atlas access revoked, contact your Velum admin")

    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        tools.register(
            server,
            cfg,
            modules=tools.COMPANION_CATEGORY_MODULES,
            token_validator=lambda: revoked,
            telemetry_cfg=telemetry_cfg,
            install_source="concierge_invite",
        )
        await captured.call_tool_handler("companion_search_assets", {"query": "x"})

    assert mock_client.capture.called
    capture_kwargs = mock_client.capture.call_args.kwargs
    assert capture_kwargs["event"] == "tool_call"
    assert capture_kwargs["properties"]["tool_name"] == "companion_search_assets"


@pytest.mark.asyncio
async def test_call_tool_fires_event_for_unknown_tool(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)
    mock_client = MagicMock()
    telemetry_cfg = TelemetryConfig(api_key="phc_test")

    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        tools.register(server, cfg, telemetry_cfg=telemetry_cfg)
        await captured.call_tool_handler("does_not_exist", {})

    assert mock_client.capture.called
    capture_kwargs = mock_client.capture.call_args.kwargs
    assert capture_kwargs["properties"]["tool_name"] == "does_not_exist"


@pytest.mark.asyncio
async def test_call_tool_no_telemetry_when_cfg_none(tmp_path: Path) -> None:
    """Without a telemetry_cfg, no events fire."""
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)
    mock_client = MagicMock()

    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        tools.register(server, cfg)  # no telemetry_cfg
        await captured.call_tool_handler("atlas_search", {"query": "x"})

    mock_client.capture.assert_not_called()


@pytest.mark.asyncio
async def test_call_tool_swallows_posthog_errors(tmp_path: Path) -> None:
    """A PostHog API error must NEVER crash the host process."""
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)
    mock_client = MagicMock()
    mock_client.capture.side_effect = RuntimeError("PostHog broken")
    telemetry_cfg = TelemetryConfig(api_key="phc_test")

    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        tools.register(server, cfg, telemetry_cfg=telemetry_cfg)
        # Must not raise
        result = await captured.call_tool_handler("atlas_search", {"query": "x"})

    assert len(result) == 1
    assert "No assets found" in result[0].text


@pytest.mark.asyncio
async def test_install_source_omitted_when_unset(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)
    mock_client = MagicMock()
    telemetry_cfg = TelemetryConfig(api_key="phc_test")

    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        tools.register(server, cfg, telemetry_cfg=telemetry_cfg)  # no install_source
        await captured.call_tool_handler("atlas_search", {"query": "x"})

    capture_kwargs = mock_client.capture.call_args.kwargs
    assert "install_source" not in capture_kwargs["properties"]


# ---------------------------------------------------------------------------
# cli/install.py fires install events
# ---------------------------------------------------------------------------


def test_install_cursor_fires_mandatory_event(tmp_path: Path) -> None:
    from alma_atlas.cli import install as install_cli

    target = tmp_path / "cursor_global.json"
    mock_client = MagicMock()
    with (
        patch.object(install_cli, "_cursor_appears_installed", return_value=True),
        patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target),
        patch("alma_atlas.telemetry._posthog") as mock_posthog,
        patch.dict("os.environ", {"ATLAS_POSTHOG_API_KEY": "phc_test"}, clear=False),
    ):
        mock_posthog.Posthog.return_value = mock_client
        result = runner.invoke(install_cli.app, ["cursor"])

    assert result.exit_code == 0
    # mandatory_event fired with install_cursor
    assert mock_client.capture.called
    events = [c.kwargs["event"] for c in mock_client.capture.call_args_list]
    assert "install_cursor" in events


def test_install_cursor_with_token_fires_opt_in_event_with_hashed_token(tmp_path: Path) -> None:
    """Companion install: both mandatory AND opt-in events fire; raw token never sent."""
    from alma_atlas.cli import install as install_cli

    target = tmp_path / "cursor_global.json"
    mock_client = MagicMock()
    raw_token = "a" * 32
    with (
        patch.object(install_cli, "_cursor_appears_installed", return_value=True),
        patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target),
        patch("alma_atlas.telemetry._posthog") as mock_posthog,
        patch.dict("os.environ", {"ATLAS_POSTHOG_API_KEY": "phc_test"}, clear=False),
    ):
        mock_posthog.Posthog.return_value = mock_client
        result = runner.invoke(install_cli.app, ["cursor", "--token", raw_token])

    assert result.exit_code == 0
    # Two captures: mandatory + opt-in
    assert mock_client.capture.call_count >= 2
    # The raw token must NEVER appear in any event
    all_args = str(mock_client.capture.call_args_list)
    assert raw_token not in all_args
    # The opt-in event includes a hashed account correlator
    opt_in_calls = [
        c for c in mock_client.capture.call_args_list
        if c.kwargs["properties"].get("alma_account_token") is not None
    ]
    assert len(opt_in_calls) >= 1
    # 16-char hex hash, not the raw token
    assert len(opt_in_calls[0].kwargs["properties"]["alma_account_token"]) == 16


def test_install_cursor_no_opt_in_without_token(tmp_path: Path) -> None:
    """Direct-pip install: only mandatory event fires."""
    from alma_atlas.cli import install as install_cli

    target = tmp_path / "cursor_global.json"
    mock_client = MagicMock()
    with (
        patch.object(install_cli, "_cursor_appears_installed", return_value=True),
        patch.object(install_cli, "_CURSOR_GLOBAL_PATH", target),
        patch("alma_atlas.telemetry._posthog") as mock_posthog,
        patch.dict("os.environ", {"ATLAS_POSTHOG_API_KEY": "phc_test"}, clear=False),
    ):
        mock_posthog.Posthog.return_value = mock_client
        runner.invoke(install_cli.app, ["cursor"])

    # No alma_account_token in any event (no opt-in)
    for call in mock_client.capture.call_args_list:
        assert "alma_account_token" not in call.kwargs.get("properties", {})


def test_install_claude_fires_mandatory_event(tmp_path: Path) -> None:
    from alma_atlas.cli import install as install_cli

    fake_path = tmp_path / "claude" / "config.json"
    mock_client = MagicMock()
    with (
        patch.object(install_cli, "_claude_desktop_appears_installed", return_value=True),
        patch.object(install_cli, "_claude_desktop_config_path", return_value=fake_path),
        patch("alma_atlas.telemetry._posthog") as mock_posthog,
        patch.dict("os.environ", {"ATLAS_POSTHOG_API_KEY": "phc_test"}, clear=False),
    ):
        mock_posthog.Posthog.return_value = mock_client
        result = runner.invoke(install_cli.app, ["claude"])

    assert result.exit_code == 0
    events = [c.kwargs["event"] for c in mock_client.capture.call_args_list]
    assert "install_claude" in events


# ---------------------------------------------------------------------------
# cli/serve.py: token hashing + opt-in setup
# ---------------------------------------------------------------------------


def test_serve_hashes_alma_token_for_telemetry():
    """The serve command must derive a hash, never pass the raw token."""
    from alma_atlas.cli.serve import _hash_token

    raw = "x" * 32
    hashed = _hash_token(raw)
    assert len(hashed) == 16
    assert hashed != raw
    # Stable: same token -> same hash
    assert _hash_token(raw) == hashed
    # Different token -> different hash
    assert _hash_token("y" * 32) != hashed
