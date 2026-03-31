"""Tests for alma_atlas.hooks — HookExecutor, HookEvent, HookResult."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from alma_atlas.config import AtlasConfig, PostScanHook, load_atlas_yml
from alma_atlas.hooks import (
    HookEvent,
    HookExecutor,
    HookResult,
    HookRuntimeConfig,
    make_drift_detected_event,
    make_hook_event,
    make_scan_result_event,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _webhook_hook(name: str = "wh", events: list[str] | None = None) -> PostScanHook:
    return PostScanHook(
        name=name,
        type="webhook",
        events=events or ["scan_complete"],
        url="http://example.com/hook",
        headers={"X-Test": "1"},
    )


def _log_hook(name: str = "lg", events: list[str] | None = None) -> PostScanHook:
    return PostScanHook(
        name=name,
        type="log",
        events=events or ["scan_complete"],
    )


def _make_event(event_type: str = "scan_complete", source_id: str = "src1") -> HookEvent:
    return HookEvent(
        event_type=event_type,
        source_id=source_id,
        timestamp="2024-01-01T00:00:00+00:00",
        data={"asset_count": 5, "edge_count": 3},
    )


# ---------------------------------------------------------------------------
# HookEvent / make_hook_event
# ---------------------------------------------------------------------------


def test_hook_event_fields() -> None:
    e = HookEvent(event_type="scan_complete", source_id="src", timestamp="t", data={"k": "v"})
    assert e.event_type == "scan_complete"
    assert e.source_id == "src"
    assert e.data == {"k": "v"}


def test_make_hook_event_stamps_timestamp() -> None:
    e = make_hook_event("scan_error", "my-source", error="oops")
    assert e.event_type == "scan_error"
    assert e.source_id == "my-source"
    assert e.data == {"error": "oops"}
    assert "T" in e.timestamp  # ISO 8601


def test_make_scan_result_event_builds_scan_complete_payload() -> None:
    event = make_scan_result_event(source_id="src", asset_count=5, edge_count=3)
    assert event.event_type == "scan_complete"
    assert event.data == {"asset_count": 5, "edge_count": 3}


def test_make_drift_detected_event_builds_payload() -> None:
    event = make_drift_detected_event(source_id="src", blocked=True, asset_count=2)
    assert event.event_type == "drift_detected"
    assert event.data == {"blocked": True, "asset_count": 2}


# ---------------------------------------------------------------------------
# HookExecutor.fire — event matching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fire_returns_empty_when_no_matching_hooks() -> None:
    hook = _log_hook(events=["scan_error"])
    executor = HookExecutor([hook])
    results = await executor.fire(_make_event("scan_complete"))
    assert results == []


@pytest.mark.asyncio
async def test_fire_returns_results_for_matching_hooks() -> None:
    hook = _log_hook(events=["scan_complete"])
    executor = HookExecutor([hook])
    results = await executor.fire(_make_event("scan_complete"))
    assert len(results) == 1
    assert results[0].hook_name == "lg"
    assert results[0].success is True


@pytest.mark.asyncio
async def test_fire_ignores_non_matching_hooks() -> None:
    hooks = [
        _log_hook(name="match", events=["scan_complete"]),
        _log_hook(name="no-match", events=["scan_error"]),
    ]
    executor = HookExecutor(hooks)
    results = await executor.fire(_make_event("scan_complete"))
    assert len(results) == 1
    assert results[0].hook_name == "match"


@pytest.mark.asyncio
async def test_fire_multiple_matching_hooks() -> None:
    hooks = [
        _log_hook(name="a", events=["scan_complete", "drift_detected"]),
        _log_hook(name="b", events=["scan_complete"]),
        _log_hook(name="c", events=["drift_detected"]),
    ]
    executor = HookExecutor(hooks)
    results = await executor.fire(_make_event("scan_complete"))
    assert len(results) == 2
    names = {r.hook_name for r in results}
    assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# Log hook
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_hook_writes_json_to_stdout(capsys) -> None:
    hook = _log_hook()
    executor = HookExecutor([hook])
    await executor.fire(_make_event())
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())
    assert payload["event_type"] == "scan_complete"
    assert payload["source_id"] == "src1"
    assert payload["hook"] == "lg"
    assert "timestamp" in payload
    assert "data" in payload


@pytest.mark.asyncio
async def test_log_hook_returns_success() -> None:
    executor = HookExecutor([_log_hook()])
    results = await executor.fire(_make_event())
    assert results[0].success is True
    assert results[0].error is None


# ---------------------------------------------------------------------------
# Webhook hook — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webhook_hook_posts_correct_payload() -> None:
    hook = _webhook_hook()
    executor = HookExecutor([hook])
    event = _make_event()

    captured_call: dict = {}

    async def fake_request_with_retry(client, **kwargs):
        captured_call.update(kwargs)
        return MagicMock()

    with patch("alma_atlas.hooks.async_request_with_retry", side_effect=fake_request_with_retry):
        results = await executor.fire(event)

    assert len(results) == 1
    assert results[0].success is True
    assert captured_call["url"] == "http://example.com/hook"
    assert captured_call["method"] == "POST"
    assert captured_call["json_body"]["event_type"] == "scan_complete"
    assert captured_call["json_body"]["source_id"] == "src1"
    assert captured_call["max_retries"] == 1
    assert captured_call["backoff_base"] == 1.0
    assert captured_call["headers"]["Content-Type"] == "application/json"
    assert captured_call["headers"]["X-Test"] == "1"


@pytest.mark.asyncio
async def test_webhook_hook_uses_runtime_settings() -> None:
    hook = _webhook_hook()
    runtime = HookRuntimeConfig(webhook_timeout_seconds=7.5, max_retries=3, backoff_base_seconds=2.0)
    executor = HookExecutor([hook], runtime=runtime)
    captured_call: dict = {}

    async def fake_request_with_retry(client, **kwargs):
        captured_call.update(kwargs)
        return MagicMock()

    with patch("alma_atlas.hooks.async_request_with_retry", side_effect=fake_request_with_retry):
        results = await executor.fire(_make_event())

    assert results[0].success is True
    assert captured_call["max_retries"] == 3
    assert captured_call["backoff_base"] == 2.0


@pytest.mark.asyncio
async def test_webhook_returns_failure_when_request_helper_raises() -> None:
    hook = _webhook_hook()
    executor = HookExecutor([hook])

    async def fake_request_with_retry(client, **kwargs):
        raise RuntimeError("connection refused")

    with patch("alma_atlas.hooks.async_request_with_retry", side_effect=fake_request_with_retry):
        results = await executor.fire(_make_event())

    assert results[0].success is False
    assert "connection refused" in (results[0].error or "")


# ---------------------------------------------------------------------------
# Error isolation — one hook failure doesn't block others
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_error_in_one_hook_does_not_block_others(capsys) -> None:
    hooks = [
        _webhook_hook(name="failing", events=["scan_complete"]),
        _log_hook(name="succeeding", events=["scan_complete"]),
    ]
    executor = HookExecutor(hooks)

    async def fake_request_with_retry(client, **kwargs):
        raise RuntimeError("connection refused")

    with patch("alma_atlas.hooks.async_request_with_retry", side_effect=fake_request_with_retry):
        results = await executor.fire(_make_event())

    assert len(results) == 2
    results_by_name = {r.hook_name: r for r in results}
    assert results_by_name["failing"].success is False
    assert results_by_name["succeeding"].success is True


@pytest.mark.asyncio
async def test_unknown_hook_type_returns_error() -> None:
    hook = PostScanHook(name="bad", type="grpc", events=["scan_complete"])
    executor = HookExecutor([hook])
    results = await executor.fire(_make_event())
    assert len(results) == 1
    assert results[0].success is False
    assert "Unknown hook type" in (results[0].error or "")


# ---------------------------------------------------------------------------
# Config parsing — hooks from atlas.yml
# ---------------------------------------------------------------------------


def test_load_atlas_yml_parses_hooks(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        """
version: 1
sources: []
hooks:
  post_scan:
    - name: slack-drift
      type: webhook
      url: https://hooks.slack.com/services/abc
      events: [drift_detected, scan_error]
      headers:
        Authorization: "Bearer token123"
    - name: ci-log
      type: log
      events: [scan_complete, drift_detected]
"""
    )
    cfg = load_atlas_yml(yml)
    assert len(cfg.hooks) == 2

    slack_hook = cfg.hooks[0]
    assert slack_hook.name == "slack-drift"
    assert slack_hook.type == "webhook"
    assert slack_hook.url == "https://hooks.slack.com/services/abc"
    assert "drift_detected" in slack_hook.events
    assert "scan_error" in slack_hook.events
    assert slack_hook.headers == {"Authorization": "Bearer token123"}

    log_hook = cfg.hooks[1]
    assert log_hook.name == "ci-log"
    assert log_hook.type == "log"
    assert log_hook.url is None
    assert "scan_complete" in log_hook.events
    assert log_hook.headers == {}


def test_load_atlas_yml_no_hooks_section(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    yml = tmp_path / "atlas.yml"
    yml.write_text("version: 1\nsources: []\n")
    cfg = load_atlas_yml(yml)
    assert cfg.hooks == []


def test_load_atlas_yml_empty_post_scan(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    yml = tmp_path / "atlas.yml"
    yml.write_text("version: 1\nsources: []\nhooks:\n  post_scan: []\n")
    cfg = load_atlas_yml(yml)
    assert cfg.hooks == []


def test_load_atlas_yml_unknown_top_level_key_rejected(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    yml = tmp_path / "atlas.yml"
    yml.write_text("version: 1\nsources: []\nunknown_key: bad\n")
    with pytest.raises(ValueError, match="Unknown top-level key"):
        load_atlas_yml(yml)


# ---------------------------------------------------------------------------
# PostScanHook dataclass
# ---------------------------------------------------------------------------


def test_post_scan_hook_defaults() -> None:
    hook = PostScanHook(name="h", type="log", events=["scan_complete"])
    assert hook.url is None
    assert hook.headers == {}


def test_post_scan_hook_with_url_and_headers() -> None:
    hook = PostScanHook(
        name="h",
        type="webhook",
        events=["scan_complete"],
        url="https://example.com",
        headers={"Authorization": "Bearer x"},
    )
    assert hook.url == "https://example.com"
    assert hook.headers["Authorization"] == "Bearer x"


# ---------------------------------------------------------------------------
# CLI — hooks list command
# ---------------------------------------------------------------------------


def test_hooks_list_with_no_hooks(tmp_path: Path) -> None:
    from unittest.mock import patch

    from typer.testing import CliRunner

    from alma_atlas.cli.hooks import app

    cfg = AtlasConfig(config_dir=tmp_path)

    runner = CliRunner()
    with patch("alma_atlas.cli.hooks.get_config", return_value=cfg):
        result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "No hooks configured" in result.output


def test_hooks_list_shows_configured_hooks(tmp_path: Path) -> None:
    from unittest.mock import patch

    from typer.testing import CliRunner

    from alma_atlas.cli.hooks import app

    cfg = AtlasConfig(
        config_dir=tmp_path,
        hooks=[
            PostScanHook(name="my-webhook", type="webhook", events=["scan_complete"], url="https://example.com"),
            PostScanHook(name="my-log", type="log", events=["drift_detected"]),
        ],
    )

    runner = CliRunner()
    with patch("alma_atlas.cli.hooks.get_config", return_value=cfg):
        result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "my-webhook" in result.output
    assert "my-log" in result.output


# ---------------------------------------------------------------------------
# CLI — hooks test command
# ---------------------------------------------------------------------------


def test_hooks_test_fires_event(tmp_path: Path) -> None:
    from unittest.mock import patch

    from typer.testing import CliRunner

    from alma_atlas.cli.hooks import app

    cfg = AtlasConfig(
        config_dir=tmp_path,
        hooks=[PostScanHook(name="log-hook", type="log", events=["scan_complete"])],
    )

    runner = CliRunner()
    with patch("alma_atlas.cli.hooks.get_config", return_value=cfg):
        result = runner.invoke(app, ["test"])
    assert result.exit_code == 0
    assert "log-hook" in result.output


def test_hooks_test_specific_hook(tmp_path: Path) -> None:
    from unittest.mock import patch

    from typer.testing import CliRunner

    from alma_atlas.cli.hooks import app

    cfg = AtlasConfig(
        config_dir=tmp_path,
        hooks=[
            PostScanHook(name="alpha", type="log", events=["scan_complete"]),
            PostScanHook(name="beta", type="log", events=["scan_error"]),
        ],
    )

    runner = CliRunner()
    with patch("alma_atlas.cli.hooks.get_config", return_value=cfg):
        result = runner.invoke(app, ["test", "alpha"])
    assert result.exit_code == 0
    assert "alpha" in result.output


def test_hooks_test_unknown_hook_exits_1(tmp_path: Path) -> None:
    from unittest.mock import patch

    from typer.testing import CliRunner

    from alma_atlas.cli.hooks import app

    cfg = AtlasConfig(
        config_dir=tmp_path,
        hooks=[PostScanHook(name="real-hook", type="log", events=["scan_complete"])],
    )

    runner = CliRunner()
    with patch("alma_atlas.cli.hooks.get_config", return_value=cfg):
        result = runner.invoke(app, ["test", "nonexistent"])
    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Integration — scan command fires hooks (mock executor)
# ---------------------------------------------------------------------------


def test_scan_fires_hooks_after_scan_complete(tmp_path: Path) -> None:
    """Scan CLI should call HookExecutor.fire after successful scan."""
    from unittest.mock import patch

    from typer.testing import CliRunner

    from alma_atlas.cli.scan import app
    from alma_atlas.config import SourceConfig
    from alma_atlas.pipeline.scan import ScanAllResult, ScanResult

    fired_events: list[HookEvent] = []

    async def mock_fire(self, event: HookEvent) -> list[HookResult]:
        fired_events.append(event)
        return [HookResult(hook_name="test-hook", success=True)]

    cfg = AtlasConfig(
        config_dir=tmp_path,
        hooks=[PostScanHook(name="test-hook", type="log", events=["scan_complete", "scan_error"])],
    )
    cfg.ensure_dir()
    cfg.save_sources([SourceConfig(id="pg-test", kind="postgres", params={})])

    mock_all_result = ScanAllResult(
        results=[ScanResult(source_id="pg-test", asset_count=10, edge_count=5)],
        cross_system_edge_count=0,
    )

    runner = CliRunner()
    with (
        patch("alma_atlas.cli.scan.get_config", return_value=cfg),
        patch("alma_atlas.pipeline.scan.run_scan_all", return_value=mock_all_result),
        patch("alma_atlas.hooks.HookExecutor.fire", mock_fire),
    ):
        result = runner.invoke(app)

    assert result.exit_code == 0
    assert len(fired_events) == 1
    assert fired_events[0].event_type == "scan_complete"
    assert fired_events[0].source_id == "pg-test"


def test_scan_fires_scan_error_hook_on_failed_source(tmp_path: Path) -> None:
    """A failed source should fire scan_error hook."""
    from unittest.mock import patch

    from typer.testing import CliRunner

    from alma_atlas.cli.scan import app
    from alma_atlas.config import SourceConfig
    from alma_atlas.pipeline.scan import ScanAllResult, ScanResult

    fired_events: list[HookEvent] = []

    async def mock_fire(self, event: HookEvent) -> list[HookResult]:
        fired_events.append(event)
        return []

    cfg = AtlasConfig(
        config_dir=tmp_path,
        hooks=[PostScanHook(name="error-hook", type="log", events=["scan_error"])],
    )
    cfg.ensure_dir()
    cfg.save_sources([SourceConfig(id="broken-src", kind="postgres", params={})])

    mock_all_result = ScanAllResult(
        results=[ScanResult(source_id="broken-src", error="Connection refused")],
        cross_system_edge_count=0,
    )

    runner = CliRunner()
    with (
        patch("alma_atlas.cli.scan.get_config", return_value=cfg),
        patch("alma_atlas.pipeline.scan.run_scan_all", return_value=mock_all_result),
        patch("alma_atlas.hooks.HookExecutor.fire", mock_fire),
    ):
        result = runner.invoke(app)

    assert result.exit_code == 1
    assert len(fired_events) == 1
    assert fired_events[0].event_type == "scan_error"


# ---------------------------------------------------------------------------
# Integration — drift_detected hook from pipeline/scan.py
# ---------------------------------------------------------------------------


def test_run_enforcement_returns_has_violations_flag() -> None:
    """_run_enforcement should return (blocked, has_violations) tuple."""
    from alma_atlas.pipeline.scan import _run_enforcement

    # Build a minimal mock snapshot with no objects (no contracts = no violations)
    snapshot = MagicMock()
    snapshot.objects = []

    db = MagicMock()
    blocked, has_violations = _run_enforcement(snapshot, "src1", db)
    assert blocked is False
    assert has_violations is False
