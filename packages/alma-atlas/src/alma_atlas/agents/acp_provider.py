"""ACP runtime and provider wrappers for Atlas learning workflows.

Atlas uses ACP as the execution/runtime boundary for agent-backed learning.
This module provides two layers:

* :class:`ACPSessionRuntime`: owns one spawned ACP subprocess, negotiates
  capabilities, and creates one repo-scoped ACP session.
* :class:`ACPProvider`: thin workflow wrapper that reuses an
  :class:`ACPSessionRuntime` to run structured analyses and validate JSON
  results against Pydantic schemas.

Structured output is still obtained by instructing the agent to write JSON to a
temp file, then validating the file content with Pydantic. On validation
failure the provider sends the error details back to the agent for self-healing,
retrying up to :data:`MAX_RETRIES` times.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any, TypeVar, cast

from acp import PROTOCOL_VERSION, session_notification, spawn_agent_process, text_block
from acp.contrib.session_state import SessionAccumulator
from acp.schema import (
    ClientCapabilities,
    CreateTerminalResponse,
    FileSystemCapabilities,
    Implementation,
    InitializeResponse,
    KillTerminalResponse,
    NewSessionResponse,
    ReadTextFileResponse,
    ReleaseTerminalResponse,
    TerminalExitStatus,
    TerminalOutputResponse,
    WaitForTerminalExitResponse,
    WriteTextFileResponse,
)
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

MAX_RETRIES = 3
DEFAULT_OUTPUT_BYTE_LIMIT = 1024 * 1024
_ATLAS_CLIENT_NAME = "alma-atlas"
_ATLAS_CLIENT_TITLE = "Alma Atlas"


def _atlas_version() -> str:
    try:
        return metadata.version("alma-atlas")
    except metadata.PackageNotFoundError:
        return "0.1.0"


def _default_client_info() -> Implementation:
    return Implementation(
        name=_ATLAS_CLIENT_NAME,
        title=_ATLAS_CLIENT_TITLE,
        version=_atlas_version(),
    )


def _truncate_output(text: str, output_byte_limit: int) -> tuple[str, bool]:
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= output_byte_limit:
        return text, False

    trimmed = encoded[-output_byte_limit:]
    while trimmed:
        try:
            return trimmed.decode("utf-8"), True
        except UnicodeDecodeError:
            trimmed = trimmed[1:]
    return "", True


def _exit_status_from_returncode(returncode: int | None) -> TerminalExitStatus | None:
    if returncode is None:
        return None
    if returncode < 0:
        return TerminalExitStatus(exit_code=None, signal=str(-returncode))
    return TerminalExitStatus(exit_code=returncode, signal=None)


@dataclass
class _TerminalHandle:
    process: asyncio.subprocess.Process
    output_byte_limit: int
    output: str = ""
    truncated: bool = False
    stdout_task: asyncio.Task[None] | None = None
    stderr_task: asyncio.Task[None] | None = None

    def append_output(self, text: str) -> None:
        next_output, was_truncated = _truncate_output(
            f"{self.output}{text}",
            self.output_byte_limit,
        )
        self.output = next_output
        self.truncated = self.truncated or was_truncated

    async def wait_for_streams(self) -> None:
        tasks = [task for task in (self.stdout_task, self.stderr_task) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


class SimpleClient:
    """ACP client surface implemented by Atlas.

    Atlas currently supports:

    * file reads/writes used for temp-file structured JSON output
    * optional terminal methods for repo-native exploration
    * permission auto-approval
    * structured session-update accumulation for plans, tools, and mode/config
      changes
    """

    def __init__(self, *, enable_terminal: bool = True) -> None:
        self._enable_terminal = enable_terminal
        self._written_files: dict[str, str] = {}
        self._terminals: dict[str, _TerminalHandle] = {}
        self._next_terminal_id = 0
        self._session_updates: list[tuple[str, Any]] = []
        self._session_accumulator = SessionAccumulator(auto_reset_on_session_change=True)

    @property
    def capabilities(self) -> ClientCapabilities:
        return ClientCapabilities(
            fs=FileSystemCapabilities(
                read_text_file=True,
                write_text_file=True,
            ),
            terminal=self._enable_terminal,
        )

    @property
    def supports_direct_repo_exploration(self) -> bool:
        return self._enable_terminal

    @property
    def session_snapshot(self) -> Any:
        return self._session_accumulator.snapshot()

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    async def write_text_file(
        self, content: str, path: str, session_id: str, **kwargs: Any
    ) -> WriteTextFileResponse:
        logger.debug("SimpleClient.write_text_file: path=%s session=%s", path, session_id)
        self._written_files[path] = content
        try:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        except Exception as exc:
            logger.debug("SimpleClient.write_text_file: disk write failed: %s", exc)
        return WriteTextFileResponse()

    async def read_text_file(
        self,
        path: str,
        session_id: str,
        limit: int | None = None,
        line: int | None = None,
        **kwargs: Any,
    ) -> ReadTextFileResponse:
        try:
            content = Path(path).read_text()
        except OSError:
            content = ""
        else:
            if line is not None or limit is not None:
                lines = content.splitlines(keepends=True)
                start = max((line or 1) - 1, 0)
                stop = None if limit is None else start + max(limit, 0)
                content = "".join(lines[start:stop])
        return ReadTextFileResponse(content=content)

    # ------------------------------------------------------------------
    # Permission handling -- auto-allow everything
    # ------------------------------------------------------------------

    async def request_permission(
        self, options: list[Any], session_id: str, tool_call: Any, **kwargs: Any
    ) -> Any:
        from acp.schema import AllowedOutcome, DeniedOutcome, RequestPermissionResponse

        # Prefer allow_once or allow_always over other option kinds.
        preferred_kinds = ("allow_once", "allow_always")
        for opt in options:
            if opt.kind in preferred_kinds:
                return RequestPermissionResponse(
                    outcome=AllowedOutcome(outcome="selected", option_id=opt.option_id)
                )
        if options:
            return RequestPermissionResponse(
                outcome=AllowedOutcome(outcome="selected", option_id=options[0].option_id)
            )
        return RequestPermissionResponse(outcome=DeniedOutcome(outcome="cancelled"))

    # ------------------------------------------------------------------
    # Session updates -- log only, no state needed
    # ------------------------------------------------------------------

    async def session_update(self, session_id: str, update: Any, **kwargs: Any) -> None:
        logger.debug("SimpleClient.session_update: session=%s type=%s", session_id, type(update).__name__)
        self._session_updates.append((session_id, update))
        try:
            self._session_accumulator.apply(
                session_notification(session_id=session_id, update=update)
            )
        except Exception as exc:
            logger.debug("SimpleClient.session_update: could not accumulate update: %s", exc)

    async def _drain_stream(
        self,
        stream: asyncio.StreamReader | None,
        terminal: _TerminalHandle,
    ) -> None:
        if stream is None:
            return
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                return
            terminal.append_output(chunk.decode("utf-8", errors="replace"))

    # ------------------------------------------------------------------
    # Terminal operations
    # ------------------------------------------------------------------

    async def create_terminal(
        self,
        command: str,
        session_id: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: list[Any] | None = None,
        output_byte_limit: int | None = None,
        **kwargs: Any,
    ) -> CreateTerminalResponse:
        if not self._enable_terminal:
            raise RuntimeError("ACPProvider: terminal operations are disabled")

        terminal_id = f"term-{self._next_terminal_id}"
        self._next_terminal_id += 1

        merged_env = os.environ.copy()
        if env:
            merged_env.update({item.name: item.value for item in env})

        process = await asyncio.create_subprocess_exec(
            command,
            *(args or []),
            cwd=cwd,
            env=merged_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        terminal = _TerminalHandle(
            process=process,
            output_byte_limit=output_byte_limit or DEFAULT_OUTPUT_BYTE_LIMIT,
        )
        terminal.stdout_task = asyncio.create_task(self._drain_stream(process.stdout, terminal))
        terminal.stderr_task = asyncio.create_task(self._drain_stream(process.stderr, terminal))
        self._terminals[terminal_id] = terminal
        return CreateTerminalResponse(terminal_id=terminal_id)

    async def terminal_output(
        self,
        session_id: str,
        terminal_id: str,
        **kwargs: Any,
    ) -> TerminalOutputResponse:
        terminal = self._terminals[terminal_id]
        if terminal.process.returncode is not None:
            await terminal.wait_for_streams()
        return TerminalOutputResponse(
            output=terminal.output,
            truncated=terminal.truncated,
            exit_status=_exit_status_from_returncode(terminal.process.returncode),
        )

    async def release_terminal(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> ReleaseTerminalResponse:
        terminal = self._terminals.pop(terminal_id, None)
        if terminal is None:
            return ReleaseTerminalResponse()

        if terminal.process.returncode is None:
            terminal.process.kill()
            await terminal.process.wait()
        await terminal.wait_for_streams()
        return ReleaseTerminalResponse()

    async def wait_for_terminal_exit(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> WaitForTerminalExitResponse:
        terminal = self._terminals[terminal_id]
        await terminal.process.wait()
        await terminal.wait_for_streams()
        status = _exit_status_from_returncode(terminal.process.returncode)
        return WaitForTerminalExitResponse(
            exit_code=None if status is None else status.exit_code,
            signal=None if status is None else status.signal,
        )

    async def kill_terminal(
        self,
        session_id: str,
        terminal_id: str,
        **kwargs: Any,
    ) -> KillTerminalResponse:
        terminal = self._terminals.get(terminal_id)
        if terminal is not None and terminal.process.returncode is None:
            terminal.process.kill()
        return KillTerminalResponse()

    async def aclose(self) -> None:
        for terminal_id in list(self._terminals):
            await self.release_terminal(session_id="", terminal_id=terminal_id)

    # ------------------------------------------------------------------
    # Extension points -- no-ops
    # ------------------------------------------------------------------

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        pass

    def on_connect(self, conn: Any) -> None:
        pass


class ACPSessionRuntime:
    """Own one ACP subprocess and one repo-scoped ACP session."""

    def __init__(
        self,
        *,
        command: str = "claude-agent-acp",
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        enable_terminal: bool = True,
        mcp_servers: list[Any] | None = None,
        client_info: Implementation | None = None,
    ) -> None:
        self._command = command
        self._args: list[str] = list(args or [])
        self._extra_env: dict[str, str] = dict(env or {})
        self._cwd = cwd or os.getcwd()
        self._enable_terminal = enable_terminal
        self._mcp_servers = list(mcp_servers or [])
        self._client_info = client_info or _default_client_info()
        self._client: SimpleClient | None = None
        self._conn: Any = None  # ClientSideConnection
        self._session_id: str | None = None
        self._cm: Any = None  # async context manager holding the subprocess
        self._initialize_response: InitializeResponse | None = None
        self._session_response: NewSessionResponse | None = None

    @property
    def client(self) -> SimpleClient | None:
        return self._client

    @property
    def conn(self) -> Any:
        return self._conn

    @property
    def cm(self) -> Any:
        return self._cm

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def initialize_response(self) -> InitializeResponse | None:
        return self._initialize_response

    @property
    def session_response(self) -> NewSessionResponse | None:
        return self._session_response

    @property
    def session_snapshot(self) -> Any:
        if self._client is None:
            return None
        return self._client.session_snapshot

    @property
    def supports_direct_repo_exploration(self) -> bool:
        if self._client is None:
            return self._enable_terminal or bool(self._mcp_servers)
        return self._client.supports_direct_repo_exploration or bool(self._mcp_servers)

    async def ensure_session(self) -> None:
        """Spawn the agent and initialise the session on first call."""
        if self._conn is not None:
            return

        self._client = SimpleClient(enable_terminal=self._enable_terminal)

        merged_env: dict[str, str] | None = None
        if self._extra_env:
            merged_env = {**os.environ, **self._extra_env}

        logger.debug(
            "ACPSessionRuntime: spawning agent: command=%s args=%s",
            self._command,
            self._args,
        )
        self._cm = spawn_agent_process(
            cast(Any, self._client),
            self._command,
            *self._args,
            env=merged_env,
        )
        self._conn, _proc = await self._cm.__aenter__()
        self._initialize_response = await self._conn.initialize(
            protocol_version=PROTOCOL_VERSION,
            client_capabilities=self._client.capabilities,
            client_info=self._client_info,
        )
        self._session_response = await self._conn.new_session(
            cwd=self._cwd,
            mcp_servers=self._mcp_servers,
        )
        self._session_id = self._session_response.session_id
        logger.debug(
            "ACPSessionRuntime: session created: session_id=%s cwd=%s",
            self._session_id,
            self._cwd,
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
        if self._cm is not None:
            try:
                await self._cm.__aexit__(None, None, None)
            except Exception as exc:
                logger.debug("ACPSessionRuntime.aclose: error during cleanup: %s", exc)
        self._client = None
        self._conn = None
        self._cm = None
        self._session_id = None
        self._initialize_response = None
        self._session_response = None


class ACPProvider:
    """LLMProvider workflow wrapper backed by an ACP runtime/session."""

    def __init__(
        self,
        *,
        command: str = "claude-agent-acp",
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        enable_terminal: bool = True,
        mcp_servers: list[Any] | None = None,
        runtime: ACPSessionRuntime | None = None,
    ) -> None:
        self._command = command
        self._args: list[str] = list(args or [])
        self._extra_env: dict[str, str] = dict(env or {})
        self._cwd = cwd or os.getcwd()
        self._enable_terminal = enable_terminal
        self._mcp_servers = list(mcp_servers or [])
        self._runtime = runtime or ACPSessionRuntime(
            command=self._command,
            args=self._args,
            env=self._extra_env,
            cwd=self._cwd,
            enable_terminal=self._enable_terminal,
            mcp_servers=self._mcp_servers,
        )
        self._owns_runtime = runtime is None
        self._client: SimpleClient | None = None
        self._conn: Any = None  # ClientSideConnection
        self._session_id: str | None = None
        self._cm: Any = None  # async context manager holding the subprocess
        self._initialize_response: InitializeResponse | None = None
        self._session_response: NewSessionResponse | None = None

    def _sync_runtime_state(self) -> None:
        self._client = self._runtime.client
        self._conn = self._runtime.conn
        self._session_id = self._runtime.session_id
        self._cm = self._runtime.cm
        self._initialize_response = self._runtime.initialize_response
        self._session_response = self._runtime.session_response

    @property
    def initialize_response(self) -> InitializeResponse | None:
        return self._runtime.initialize_response

    @property
    def session_response(self) -> NewSessionResponse | None:
        return self._runtime.session_response

    @property
    def session_snapshot(self) -> Any:
        return self._runtime.session_snapshot

    @property
    def supports_direct_repo_exploration(self) -> bool:
        return self._runtime.supports_direct_repo_exploration

    # ------------------------------------------------------------------
    # LLMProvider interface
    # ------------------------------------------------------------------

    async def analyze(
        self,
        system_prompt: str,
        user_prompt: str,
        response_schema: type[T],
    ) -> T:
        """Send prompts to the ACP agent and return a validated Pydantic model.

        The agent is instructed to write its response as JSON to a temp file.
        The file is read and validated with Pydantic after each turn.  On
        validation failure the errors are sent back to the agent for
        self-healing (up to :data:`MAX_RETRIES` attempts).

        Args:
            system_prompt:   Instructions for the agent (role, task context).
            user_prompt:     User-facing content (edges, file snippets, etc.).
            response_schema: Pydantic model class the agent output must match.

        Returns:
            A validated instance of *response_schema*.

        Raises:
            ValueError: If the agent fails to produce valid output after all
                        retries.
        """
        await self._runtime.ensure_session()
        self._sync_runtime_state()
        assert self._conn is not None
        assert self._client is not None
        assert self._session_id is not None

        tmp_path = Path(tempfile.gettempdir()) / f"atlas-learn-{uuid.uuid4().hex}.json"
        schema_json = json.dumps(response_schema.model_json_schema(), indent=2)

        initial_prompt = (
            f"{system_prompt}\n\n"
            f"{user_prompt}\n\n"
            f"Write your results as valid JSON to: {tmp_path}\n\n"
            f"The JSON must conform to this schema:\n{schema_json}\n\n"
            "Write ONLY valid JSON to the file. No markdown, no comments, no wrapping."
        )

        last_error: Exception | None = None

        for attempt in range(MAX_RETRIES):
            if attempt == 0:
                prompt_text = initial_prompt
            else:
                prompt_text = (
                    f"The file you wrote to {tmp_path} has validation errors:\n"
                    f"{last_error}\n\n"
                    "Fix the file so it passes validation. "
                    "Write the corrected JSON to the same path."
                )

            # Clear any previously written content for this path.
            self._client._written_files.pop(str(tmp_path), None)

            logger.debug(
                "ACPProvider: sending prompt (attempt %d/%d)",
                attempt + 1,
                MAX_RETRIES,
            )
            await self._conn.prompt(
                session_id=self._session_id,
                prompt=[text_block(prompt_text)],
            )

            # Prefer content captured by write_text_file callback; fall back to
            # reading from disk in case the agent wrote directly without the callback.
            content: str | None = self._client._written_files.get(str(tmp_path))
            if content is None and tmp_path.exists():
                content = tmp_path.read_text()

            if content is None:
                last_error = ValueError(
                    f"Agent did not write output to {tmp_path}"
                )
                logger.warning(
                    "ACPProvider: attempt %d: agent did not write output file",
                    attempt + 1,
                )
                continue

            try:
                result = response_schema.model_validate_json(content)
                # Clean up.
                self._client._written_files.pop(str(tmp_path), None)
                tmp_path.unlink(missing_ok=True)
                logger.debug(
                    "ACPProvider: attempt %d: validation succeeded",
                    attempt + 1,
                )
                return result
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "ACPProvider: attempt %d: validation failed: %s",
                    attempt + 1,
                    exc,
                )

        # All retries exhausted.
        # Clean up temp file if present.
        self._client._written_files.pop(str(tmp_path), None)
        tmp_path.unlink(missing_ok=True)
        raise ValueError(
            f"ACPProvider: failed to get valid response after {MAX_RETRIES} attempts. "
            f"Last error: {last_error}"
        ) from last_error

    async def aclose(self) -> None:
        """Terminate the agent subprocess and release resources."""
        if self._owns_runtime:
            await self._runtime.aclose()
        self._sync_runtime_state()
        logger.debug("ACPProvider: agent subprocess terminated")

    async def __aenter__(self) -> ACPProvider:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()
