"""Tests for register() Companion-mode behavior:

- Default registration exposes only atlas_* tools.
- COMPANION_CATEGORY_MODULES exposes only companion_* tools.
- A token_validator gate intercepts call_tool dispatch when validation fails,
  returning the validator's display_message verbatim instead of invoking the
  underlying handler.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from mcp.server import Server
from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp import tools
from alma_atlas_store.db import Database


@dataclass
class _FakeValidation:
    """Minimal stand-in for auth.invite_token.TokenValidation."""

    valid: bool
    display_message: str = ""


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    with Database(db_path):
        pass
    return cfg


class _CapturedHandlers:
    """Capture decorated handlers so we can call them without an MCP runtime."""

    def __init__(self) -> None:
        self.list_tools_handler = None
        self.call_tool_handler = None


def _make_capturing_server(captured: _CapturedHandlers) -> Server:
    """Build a Server stub that captures the @list_tools / @call_tool callbacks."""
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
# Tool surface depends on `modules`
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_register_exposes_atlas_surface_only(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    tools.register(server, cfg)
    listed = await captured.list_tools_handler()

    names = {t.name for t in listed}
    assert "atlas_search" in names
    assert "companion_search_assets" not in names
    assert len(names) == 20


@pytest.mark.asyncio
async def test_companion_modules_register_companion_surface_only(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    tools.register(server, cfg, modules=tools.COMPANION_CATEGORY_MODULES)
    listed = await captured.list_tools_handler()

    names = {t.name for t in listed}
    assert names == {
        "companion_search_assets",
        "companion_get_schema_and_owner",
        "companion_explain_lineage_and_contract",
    }


# ---------------------------------------------------------------------------
# token_validator gates call_tool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_call_tool_returns_display_message_when_validation_fails(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    revoked = _FakeValidation(valid=False, display_message="Atlas access revoked, contact your Velum admin")
    tools.register(
        server,
        cfg,
        modules=tools.COMPANION_CATEGORY_MODULES,
        token_validator=lambda: revoked,
    )

    result = await captured.call_tool_handler("companion_search_assets", {"query": "anything"})

    assert len(result) == 1
    assert isinstance(result[0], TextContent)
    assert result[0].text == "Atlas access revoked, contact your Velum admin"


@pytest.mark.asyncio
async def test_call_tool_invokes_handler_when_validation_succeeds(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    valid = _FakeValidation(valid=True)
    tools.register(
        server,
        cfg,
        modules=tools.COMPANION_CATEGORY_MODULES,
        token_validator=lambda: valid,
    )

    # Empty graph -> companion_search_assets returns "No assets found"
    result = await captured.call_tool_handler("companion_search_assets", {"query": "xyz"})

    assert len(result) == 1
    assert "No assets found" in result[0].text


@pytest.mark.asyncio
async def test_call_tool_skips_validation_when_no_validator(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    # Default mode (no validator). Should dispatch normally.
    tools.register(server, cfg)
    result = await captured.call_tool_handler("atlas_search", {"query": "xyz"})

    assert len(result) == 1
    assert "No assets found" in result[0].text


@pytest.mark.asyncio
async def test_call_tool_validator_invoked_each_call(tmp_path: Path) -> None:
    """Per Issue 3A: the validator runs on every call (no caching)."""
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    call_count = {"n": 0}

    def _counting_validator():
        call_count["n"] += 1
        return _FakeValidation(valid=True)

    tools.register(
        server,
        cfg,
        modules=tools.COMPANION_CATEGORY_MODULES,
        token_validator=_counting_validator,
    )

    await captured.call_tool_handler("companion_search_assets", {"query": "a"})
    await captured.call_tool_handler("companion_search_assets", {"query": "b"})
    await captured.call_tool_handler("companion_search_assets", {"query": "c"})

    assert call_count["n"] == 3, "Validator must be invoked on every call (no caching per Issue 3A)"


@pytest.mark.asyncio
async def test_call_tool_validator_returns_unreachable_message_verbatim(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    unreachable = _FakeValidation(
        valid=False, display_message="Atlas can't reach Velum right now, try again in a minute"
    )
    tools.register(
        server,
        cfg,
        modules=tools.COMPANION_CATEGORY_MODULES,
        token_validator=lambda: unreachable,
    )

    result = await captured.call_tool_handler("companion_get_schema_and_owner", {"asset_id": "x"})
    assert result[0].text == "Atlas can't reach Velum right now, try again in a minute"


@pytest.mark.asyncio
async def test_unknown_tool_name_is_rejected_after_validation(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    captured = _CapturedHandlers()
    server = _make_capturing_server(captured)

    valid = _FakeValidation(valid=True)
    tools.register(
        server,
        cfg,
        modules=tools.COMPANION_CATEGORY_MODULES,
        token_validator=lambda: valid,
    )

    result = await captured.call_tool_handler("atlas_search", {"query": "x"})
    # atlas_search isn't in the Companion surface -> Unknown tool
    assert "Unknown tool" in result[0].text


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------


def test_tools_module_exposes_companion_constants() -> None:
    assert hasattr(tools, "ATLAS_CATEGORY_MODULES")
    assert hasattr(tools, "COMPANION_CATEGORY_MODULES")
    assert len(tools.ATLAS_CATEGORY_MODULES) == 6
    assert len(tools.COMPANION_CATEGORY_MODULES) == 1
