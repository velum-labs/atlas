"""Unit tests for ACPProvider and SimpleClient.

All tests mock ``spawn_agent_process`` so no real agent subprocess is spawned.
The mock connection records prompts and calls the client's ``write_text_file``
method to simulate what a real ACP agent would do.
"""

from __future__ import annotations

import json
import sys
import textwrap
import warnings
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alma_atlas.agents.acp_provider import MAX_RETRIES, ACPProvider, SimpleClient
from alma_atlas.agents.schemas import PipelineAnalysisResult
from alma_atlas.config import AgentConfig, AgentProcessConfig, LearningConfig, load_atlas_yml

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeSessionResp:
    session_id = "test-session-123"


class _FakeInitResp:
    pass


class _FakePromptResp:
    stop_reason = "end_turn"


def _make_fake_conn(on_prompt=None):
    """Return a fake ClientSideConnection-like object."""
    conn = MagicMock()
    conn.initialize = AsyncMock(return_value=_FakeInitResp())
    conn.new_session = AsyncMock(return_value=_FakeSessionResp())
    if on_prompt is not None:
        conn.prompt = on_prompt
    else:
        conn.prompt = AsyncMock(return_value=_FakePromptResp())
    conn.close = AsyncMock()
    return conn


def _make_spawn_cm(conn, proc=None):
    """Return an async context manager that yields (conn, proc)."""
    fake_proc = proc or MagicMock()

    @asynccontextmanager
    async def _cm(client, command, *args, env=None, **kwargs):
        yield conn, fake_proc

    return _cm


def _require_client(provider: ACPProvider) -> SimpleClient:
    assert provider._client is not None
    return provider._client


# ---------------------------------------------------------------------------
# SimpleClient
# ---------------------------------------------------------------------------


async def test_simple_client_write_text_file_stores_content(tmp_path: Path) -> None:
    client = SimpleClient()
    p = str(tmp_path / "out.json")
    await client.write_text_file(content='{"x": 1}', path=p, session_id="s1")
    assert client._written_files[p] == '{"x": 1}'


async def test_simple_client_write_text_file_writes_disk(tmp_path: Path) -> None:
    client = SimpleClient()
    p = tmp_path / "out.json"
    await client.write_text_file(content='{"ok": true}', path=str(p), session_id="s1")
    assert p.read_text() == '{"ok": true}'


async def test_simple_client_write_text_file_returns_response() -> None:
    from acp import WriteTextFileResponse

    client = SimpleClient()
    resp = await client.write_text_file(content="{}", path="/tmp/x.json", session_id="s")
    assert isinstance(resp, WriteTextFileResponse)


async def test_simple_client_request_permission_allows_allow_once() -> None:
    from acp.schema import AllowedOutcome, PermissionOption

    client = SimpleClient()
    opts = [
        PermissionOption(kind="allow_once", name="Allow once", option_id="opt-1"),
        PermissionOption(kind="reject_once", name="Reject once", option_id="opt-2"),
    ]
    resp = await client.request_permission(
        options=opts, session_id="s", tool_call=MagicMock()
    )
    assert isinstance(resp.outcome, AllowedOutcome)
    assert resp.outcome.option_id == "opt-1"


async def test_simple_client_request_permission_allows_allow_always() -> None:
    from acp.schema import AllowedOutcome, PermissionOption

    client = SimpleClient()
    opts = [
        PermissionOption(kind="reject_once", name="Reject", option_id="opt-r"),
        PermissionOption(kind="allow_always", name="Allow always", option_id="opt-a"),
    ]
    resp = await client.request_permission(
        options=opts, session_id="s", tool_call=MagicMock()
    )
    assert isinstance(resp.outcome, AllowedOutcome)
    assert resp.outcome.option_id == "opt-a"


async def test_simple_client_request_permission_falls_back_to_first() -> None:
    from acp.schema import AllowedOutcome, PermissionOption

    client = SimpleClient()
    opts = [
        PermissionOption(kind="reject_always", name="Reject always", option_id="opt-x"),
    ]
    resp = await client.request_permission(
        options=opts, session_id="s", tool_call=MagicMock()
    )
    assert isinstance(resp.outcome, AllowedOutcome)
    assert resp.outcome.option_id == "opt-x"


async def test_simple_client_request_permission_no_options_cancels() -> None:
    from acp.schema import DeniedOutcome

    client = SimpleClient()
    resp = await client.request_permission(
        options=[], session_id="s", tool_call=MagicMock()
    )
    assert isinstance(resp.outcome, DeniedOutcome)


async def test_simple_client_session_update_does_not_raise() -> None:
    client = SimpleClient()
    await client.session_update(session_id="s", update=MagicMock())


async def test_simple_client_session_update_accumulates_snapshot() -> None:
    from acp import text_block, update_agent_message

    client = SimpleClient()
    await client.session_update(
        session_id="s",
        update=update_agent_message(text_block("hello from agent")),
    )

    snapshot = client.session_snapshot
    assert snapshot.session_id == "s"
    assert len(snapshot.agent_messages) == 1
    assert snapshot.agent_messages[0].content.text == "hello from agent"


async def test_simple_client_read_text_file_returns_content(tmp_path: Path) -> None:
    p = tmp_path / "data.txt"
    p.write_text("hello")
    client = SimpleClient()
    resp = await client.read_text_file(path=str(p), session_id="s")
    assert resp.content == "hello"


async def test_simple_client_read_text_file_missing_returns_empty(tmp_path: Path) -> None:
    client = SimpleClient()
    resp = await client.read_text_file(
        path=str(tmp_path / "no_such_file.txt"), session_id="s"
    )
    assert resp.content == ""


async def test_simple_client_read_text_file_respects_line_and_limit(tmp_path: Path) -> None:
    p = tmp_path / "data.txt"
    p.write_text("line-1\nline-2\nline-3\n")
    client = SimpleClient()
    resp = await client.read_text_file(path=str(p), session_id="s", line=2, limit=1)
    assert resp.content == "line-2\n"


async def test_simple_client_terminal_lifecycle(tmp_path: Path) -> None:
    client = SimpleClient()
    created = await client.create_terminal(
        command=sys.executable,
        args=[
            "-c",
            "import sys; sys.stdout.write('hello\\n'); sys.stderr.write('err\\n')",
        ],
        cwd=str(tmp_path),
        session_id="s",
        output_byte_limit=1024,
    )

    waited = await client.wait_for_terminal_exit(session_id="s", terminal_id=created.terminal_id)
    output = await client.terminal_output(session_id="s", terminal_id=created.terminal_id)

    assert waited.exit_code == 0
    assert "hello" in output.output
    assert "err" in output.output
    assert output.exit_status is not None
    assert output.exit_status.exit_code == 0

    await client.release_terminal(session_id="s", terminal_id=created.terminal_id)
    assert created.terminal_id not in client._terminals


async def test_simple_client_terminal_disabled_raises(tmp_path: Path) -> None:
    client = SimpleClient(enable_terminal=False)
    with pytest.raises(RuntimeError, match="disabled"):
        await client.create_terminal(
            command=sys.executable,
            args=["-c", "print('nope')"],
            cwd=str(tmp_path),
            session_id="s",
        )


# ---------------------------------------------------------------------------
# ACPProvider construction
# ---------------------------------------------------------------------------


def test_acp_provider_defaults() -> None:
    p = ACPProvider()
    assert p._command == "claude-agent-acp"
    assert p._args == []
    assert p._extra_env == {}


def test_acp_provider_custom_params() -> None:
    p = ACPProvider(command="codex", args=["--model", "o3"], env={"OPENAI_API_KEY": "sk"})
    assert p._command == "codex"
    assert p._args == ["--model", "o3"]
    assert p._extra_env == {"OPENAI_API_KEY": "sk"}


# ---------------------------------------------------------------------------
# ACPProvider.analyze -- happy path
# ---------------------------------------------------------------------------


async def test_acp_provider_analyze_returns_validated_model(tmp_path: Path) -> None:
    """Agent writes valid JSON -- analyze() returns a Pydantic model."""
    valid_json = json.dumps(
        {"edges": [], "repo_summary": "All good"}
    )
    conn = _make_fake_conn()

    async def _prompt_side_effect(**kwargs):
        # Simulate agent writing the file.
        prompt_text = kwargs["prompt"][0].text
        # Extract path from the prompt.
        for line in prompt_text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = valid_json
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)):
        result = await provider.analyze("system", "user", PipelineAnalysisResult)

    assert isinstance(result, PipelineAnalysisResult)
    assert result.edges == []
    assert result.repo_summary == "All good"
    await provider.aclose()


async def test_acp_provider_analyze_session_reuse(tmp_path: Path) -> None:
    """Two analyze() calls reuse the same session (spawn_agent_process called once)."""
    valid_json = json.dumps({"edges": [], "repo_summary": "ok"})
    conn = _make_fake_conn()

    async def _prompt_side_effect(**kwargs):
        prompt_text = kwargs["prompt"][0].text
        for line in prompt_text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = valid_json
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    spawn_count = 0

    @asynccontextmanager
    async def _counting_cm(client, command, *args, **kwargs):
        nonlocal spawn_count
        spawn_count += 1
        yield conn, MagicMock()

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _counting_cm):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert spawn_count == 1, "Agent process should be spawned only once"
    assert conn.initialize.call_count == 1
    assert conn.new_session.call_count == 1
    await provider.aclose()


async def test_acp_provider_initialize_passes_capabilities_and_client_info(tmp_path: Path) -> None:
    valid_json = json.dumps({"edges": [], "repo_summary": "ok"})
    conn = _make_fake_conn()

    async def _prompt_side_effect(**kwargs):
        for line in kwargs["prompt"][0].text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = valid_json
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    initialize_kwargs = conn.initialize.call_args.kwargs
    capabilities = initialize_kwargs["client_capabilities"]
    client_info = initialize_kwargs["client_info"]

    assert capabilities.fs.read_text_file is True
    assert capabilities.fs.write_text_file is True
    assert capabilities.terminal is True
    assert client_info.name == "alma-atlas"
    assert client_info.title == "Alma Atlas"
    assert client_info.version
    await provider.aclose()


async def test_acp_provider_stores_initialize_and_session_metadata(tmp_path: Path) -> None:
    from acp.schema import (
        AgentCapabilities,
        InitializeResponse,
        NewSessionResponse,
        SessionCapabilities,
        SessionListCapabilities,
    )

    valid_json = json.dumps({"edges": []})
    conn = _make_fake_conn()
    conn.initialize = AsyncMock(
        return_value=InitializeResponse(
            protocol_version=1,
            agent_capabilities=AgentCapabilities(
                session_capabilities=SessionCapabilities(
                    list=SessionListCapabilities(),
                )
            ),
        )
    )
    conn.new_session = AsyncMock(return_value=NewSessionResponse(session_id="shared-session"))

    async def _prompt_side_effect(**kwargs):
        for line in kwargs["prompt"][0].text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = valid_json
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert provider.initialize_response is not None
    capabilities = provider.initialize_response.agent_capabilities
    assert capabilities is not None
    assert capabilities.session_capabilities is not None
    assert capabilities.session_capabilities.list is not None
    assert provider.session_response is not None
    assert provider.session_response.session_id == "shared-session"
    await provider.aclose()


async def test_acp_provider_forwards_mcp_servers_to_new_session(tmp_path: Path) -> None:
    valid_json = json.dumps({"edges": []})
    conn = _make_fake_conn()
    fake_mcp = MagicMock(name="filesystem-mcp")

    async def _prompt_side_effect(**kwargs):
        for line in kwargs["prompt"][0].text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = valid_json
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path), mcp_servers=[fake_mcp])
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert conn.new_session.call_args.kwargs["mcp_servers"] == [fake_mcp]
    await provider.aclose()


async def test_acp_provider_analyze_cleans_up_temp_file(tmp_path: Path) -> None:
    """Temp file is deleted after successful validation."""
    valid_json = json.dumps({"edges": []})
    conn = _make_fake_conn()
    written_path: list[str] = []

    async def _prompt_side_effect(**kwargs):
        prompt_text = kwargs["prompt"][0].text
        for line in prompt_text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = valid_json
                written_path.append(path)
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert written_path, "Prompt must have included a file path"
    assert str(written_path[0]) not in _require_client(provider)._written_files


# ---------------------------------------------------------------------------
# ACPProvider.analyze -- self-healing loop
# ---------------------------------------------------------------------------


def _extract_path_from_prompt(prompt_text: str) -> str | None:
    """Extract the output file path from either an initial or healing prompt."""
    for line in prompt_text.splitlines():
        if line.startswith("Write your results as valid JSON to: "):
            return line.split(": ", 1)[1].strip()
        if line.startswith("The file you wrote to ") and " has validation errors" in line:
            return line.split("The file you wrote to ", 1)[1].split(" has validation errors")[0].strip()
    return None


async def test_acp_provider_analyze_self_healing_succeeds_on_retry(tmp_path: Path) -> None:
    """First attempt writes invalid JSON; second writes valid -- succeeds on retry."""
    valid_json = json.dumps({"edges": []})
    conn = _make_fake_conn()
    call_count = 0

    async def _prompt_side_effect(**kwargs):
        nonlocal call_count
        call_count += 1
        prompt_text = kwargs["prompt"][0].text
        path = _extract_path_from_prompt(prompt_text)
        if path is not None:
            if call_count == 1:
                # First attempt: write garbage
                _require_client(provider)._written_files[path] = "not valid json"
            else:
                # Second attempt: write valid JSON
                _require_client(provider)._written_files[path] = valid_json
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)):
        result = await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert isinstance(result, PipelineAnalysisResult)
    assert call_count == 2
    await provider.aclose()


async def test_acp_provider_analyze_healing_prompt_contains_errors(tmp_path: Path) -> None:
    """Self-healing prompt includes the original validation error text."""
    conn = _make_fake_conn()
    prompts_received: list[str] = []
    call_count = 0

    valid_json = json.dumps({"edges": []})

    async def _prompt_side_effect(**kwargs):
        nonlocal call_count
        call_count += 1
        text = kwargs["prompt"][0].text
        prompts_received.append(text)
        path = _extract_path_from_prompt(text)
        if path is not None:
            if call_count == 1:
                _require_client(provider)._written_files[path] = "broken"
            else:
                _require_client(provider)._written_files[path] = valid_json
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert len(prompts_received) == 2
    # The second (healing) prompt must mention the file path and errors.
    assert "validation errors" in prompts_received[1]
    await provider.aclose()


async def test_acp_provider_analyze_raises_after_max_retries(tmp_path: Path) -> None:
    """Exhausting all retries raises ValueError."""
    conn = _make_fake_conn()

    async def _bad_prompt(**kwargs):
        for line in kwargs["prompt"][0].text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = "always bad json ]["
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_bad_prompt)

    provider = ACPProvider(cwd=str(tmp_path))
    with (
        patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)),
        pytest.raises(ValueError, match="failed to get valid response"),
    ):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert conn.prompt.call_count == MAX_RETRIES
    await provider.aclose()


async def test_acp_provider_analyze_raises_when_file_not_written(tmp_path: Path) -> None:
    """If agent never writes the file, raises ValueError after retries."""
    conn = _make_fake_conn()
    # conn.prompt does NOT write anything to _written_files.

    provider = ACPProvider(cwd=str(tmp_path))
    with (
        patch("alma_atlas.agents.acp_provider.spawn_agent_process", _make_spawn_cm(conn)),
        pytest.raises(ValueError, match="failed to get valid response"),
    ):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert conn.prompt.call_count == MAX_RETRIES
    await provider.aclose()


# ---------------------------------------------------------------------------
# ACPProvider.aclose
# ---------------------------------------------------------------------------


async def test_acp_provider_aclose_before_session_is_safe() -> None:
    provider = ACPProvider()
    # aclose before any analyze() call -- no subprocess exists, must not raise.
    await provider.aclose()


async def test_acp_provider_aclose_resets_state(tmp_path: Path) -> None:
    """After aclose(), the provider can be reopened (session reset)."""
    conn = _make_fake_conn()
    spawn_calls = 0

    @asynccontextmanager
    async def _cm(client, command, *args, **kwargs):
        nonlocal spawn_calls
        spawn_calls += 1
        yield conn, MagicMock()

    valid_json = json.dumps({"edges": []})

    async def _prompt_side_effect(**kwargs):
        for line in kwargs["prompt"][0].text.splitlines():
            if line.startswith("Write your results as valid JSON to: "):
                path = line.split(": ", 1)[1].strip()
                _require_client(provider)._written_files[path] = valid_json
                break
        return _FakePromptResp()

    conn.prompt = AsyncMock(side_effect=_prompt_side_effect)

    provider = ACPProvider(cwd=str(tmp_path))
    with patch("alma_atlas.agents.acp_provider.spawn_agent_process", _cm):
        await provider.analyze("sys", "usr", PipelineAnalysisResult)
        await provider.aclose()
        assert provider._conn is None
        # After aclose a new spawn happens on next analyze().
        await provider.analyze("sys", "usr", PipelineAnalysisResult)

    assert spawn_calls == 2
    await provider.aclose()


# ---------------------------------------------------------------------------
# make_provider factory -- acp path
# ---------------------------------------------------------------------------


def test_make_provider_acp_returns_acp_provider() -> None:
    from alma_atlas.agents.provider import make_provider

    p = make_provider("acp", model="unused")
    assert isinstance(p, ACPProvider)
    assert p._command == "claude-agent-acp"


def test_make_provider_acp_forwards_agent_params() -> None:
    from alma_atlas.agents.provider import make_provider

    p = make_provider(
        "acp",
        model="unused",
        agent_command="codex",
        agent_args=["--model", "o3"],
        agent_env={"OPENAI_API_KEY": "sk-test"},
    )
    assert isinstance(p, ACPProvider)
    assert p._command == "codex"
    assert p._args == ["--model", "o3"]
    assert p._extra_env == {"OPENAI_API_KEY": "sk-test"}


def test_make_provider_acp_unknown_still_raises() -> None:
    from alma_atlas.agents.provider import make_provider

    with pytest.raises(ValueError, match="Unknown provider"):
        make_provider("totally-unknown", model="x")


# ---------------------------------------------------------------------------
# Config parsing -- agent: section
# ---------------------------------------------------------------------------


def test_agent_process_config_defaults() -> None:
    cfg = AgentProcessConfig()
    assert cfg.command == "claude-agent-acp"
    assert cfg.args == []
    assert cfg.env == {}


def test_learning_config_agent_field_is_none_by_default() -> None:
    cfg = LearningConfig()
    assert cfg.agent is None


def test_load_atlas_yml_parses_agent_command(tmp_path: Path) -> None:
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          agent:
            command: my-custom-acp-agent
            args: ["--verbose"]
            env:
              MY_KEY: "abc123"
        """)
    )
    cfg = load_atlas_yml(yml)
    assert cfg.learning.agent is not None
    assert cfg.learning.agent.command == "my-custom-acp-agent"
    assert cfg.learning.agent.args == ["--verbose"]
    assert cfg.learning.agent.env == {"MY_KEY": "abc123"}


def test_load_atlas_yml_no_agent_key_leaves_none(tmp_path: Path) -> None:
    yml = tmp_path / "atlas.yml"
    yml.write_text("version: 1\nlearning:\n  provider: mock\n")
    cfg = load_atlas_yml(yml)
    assert cfg.learning.agent is None


def test_load_atlas_yml_per_agent_agent_key(tmp_path: Path) -> None:
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          explorer:
            provider: acp
            agent:
              command: codex
              args: []
          pipeline_analyzer:
            provider: mock
            model: claude-opus-4-6
        """)
    )
    cfg = load_atlas_yml(yml)
    assert cfg.learning.explorer.provider == "acp"
    assert cfg.learning.explorer.agent is not None
    assert cfg.learning.explorer.agent.command == "codex"
    assert cfg.learning.pipeline_analyzer.agent is None


# ---------------------------------------------------------------------------
# Phase 2 integration: config -> provider construction
# ---------------------------------------------------------------------------


def test_provider_from_agent_config_with_agent_process_config_uses_acp() -> None:
    """AgentConfig with AgentProcessConfig must produce ACPProvider automatically."""
    from alma_atlas.pipeline.learn import _provider_from_agent_config

    cfg = AgentConfig(
        provider="mock",
        model="unused",
        agent=AgentProcessConfig(command="codex", args=["--model", "o3"]),
    )
    provider = _provider_from_agent_config(cfg)
    assert isinstance(provider, ACPProvider)
    assert provider._command == "codex"
    assert provider._args == ["--model", "o3"]


def test_provider_from_agent_config_without_agent_process_config_raises() -> None:
    """AgentConfig without AgentProcessConfig and a removed provider must raise ValueError."""
    from alma_atlas.pipeline.learn import _provider_from_agent_config

    cfg = AgentConfig(provider="anthropic", model="claude-opus-4-6", agent=None)
    with pytest.raises(ValueError, match="no longer supported"):
        _provider_from_agent_config(cfg)


def test_provider_from_agent_config_agent_overrides_provider_field() -> None:
    """AgentProcessConfig takes precedence even when provider='anthropic' is set."""
    from alma_atlas.pipeline.learn import _provider_from_agent_config

    cfg = AgentConfig(
        provider="anthropic",
        model="claude-opus-4-6",
        agent=AgentProcessConfig(command="claude-agent-acp"),
    )
    provider = _provider_from_agent_config(cfg)
    assert isinstance(provider, ACPProvider)
    assert provider._command == "claude-agent-acp"


def test_load_atlas_yml_agent_command_produces_acp_provider(tmp_path: Path) -> None:
    """Flat atlas.yml with agent.command -> _provider_from_agent_config -> ACPProvider."""
    from alma_atlas.pipeline.learn import _provider_from_agent_config

    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          agent:
            command: claude-agent-acp
            args: []
        """)
    )
    cfg = load_atlas_yml(yml)
    # All three sub-agents should carry the agent process config.
    provider = _provider_from_agent_config(cfg.learning.explorer)
    assert isinstance(provider, ACPProvider)
    assert provider._command == "claude-agent-acp"


def test_load_atlas_yml_flat_unsupported_provider_raises(tmp_path: Path) -> None:
    """Flat atlas.yml with unsupported provider must fail closed."""
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          provider: anthropic
          model: claude-opus-4-6
          api_key_env: ANTHROPIC_API_KEY
        """)
    )
    with pytest.raises(ValueError, match="unsupported learning.provider"):
        load_atlas_yml(yml)


def test_load_atlas_yml_flat_openai_raises(tmp_path: Path) -> None:
    """Flat atlas.yml with unsupported provider must fail closed."""
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          provider: openai
          model: gpt-4o
        """)
    )
    with pytest.raises(ValueError, match="unsupported learning.provider"):
        load_atlas_yml(yml)


def test_load_atlas_yml_flat_mock_no_deprecation_warning(tmp_path: Path) -> None:
    """Flat atlas.yml with provider: mock must NOT emit a DeprecationWarning."""
    yml = tmp_path / "atlas.yml"
    yml.write_text("version: 1\nlearning:\n  provider: mock\n")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        load_atlas_yml(yml)

    dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert not dep_warnings


def test_load_atlas_yml_nested_unsupported_provider_raises(tmp_path: Path) -> None:
    """Nested atlas.yml with unsupported provider must fail closed."""
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          explorer:
            provider: anthropic
            model: claude-haiku-4-5-20251001
          pipeline_analyzer:
            provider: anthropic
            model: claude-opus-4-6
        """)
    )
    with pytest.raises(ValueError, match="unsupported learning provider"):
        load_atlas_yml(yml)


def test_load_atlas_yml_agent_command_no_deprecation_warning(tmp_path: Path) -> None:
    """atlas.yml using agent.command format must NOT emit a DeprecationWarning."""
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          agent:
            command: claude-agent-acp
        """)
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        load_atlas_yml(yml)

    dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert not dep_warnings
