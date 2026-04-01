"""ACP client implementation used by Atlas."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from acp import session_notification
from acp.contrib.session_state import SessionAccumulator
from acp.schema import (
    ClientCapabilities,
    CreateTerminalResponse,
    FileSystemCapabilities,
    KillTerminalResponse,
    ReadTextFileResponse,
    ReleaseTerminalResponse,
    TerminalOutputResponse,
    WaitForTerminalExitResponse,
    WriteTextFileResponse,
)

from .common import DEFAULT_OUTPUT_BYTE_LIMIT, exit_status_from_returncode, truncate_output

logger = logging.getLogger(__name__)


@dataclass
class _TerminalHandle:
    process: asyncio.subprocess.Process
    output_byte_limit: int
    output: str = ""
    truncated: bool = False
    stdout_task: asyncio.Task[None] | None = None
    stderr_task: asyncio.Task[None] | None = None

    def append_output(self, text: str) -> None:
        next_output, was_truncated = truncate_output(
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
    """ACP client surface implemented by Atlas."""

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

    def get_written_file(self, path: str) -> str | None:
        return self._written_files.get(path)

    def clear_written_file(self, path: str) -> None:
        self._written_files.pop(path, None)

    async def write_text_file(
        self,
        content: str,
        path: str,
        session_id: str,
        **kwargs: Any,
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

    async def request_permission(
        self,
        options: list[Any],
        session_id: str,
        tool_call: Any,
        **kwargs: Any,
    ) -> Any:
        from acp.schema import AllowedOutcome, DeniedOutcome, RequestPermissionResponse

        preferred_kinds = ("allow_once", "allow_always")
        for option in options:
            if option.kind in preferred_kinds:
                return RequestPermissionResponse(
                    outcome=AllowedOutcome(outcome="selected", option_id=option.option_id)
                )
        if options:
            return RequestPermissionResponse(
                outcome=AllowedOutcome(outcome="selected", option_id=options[0].option_id)
            )
        return RequestPermissionResponse(outcome=DeniedOutcome(outcome="cancelled"))

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
            exit_status=exit_status_from_returncode(terminal.process.returncode),
        )

    async def release_terminal(
        self,
        session_id: str,
        terminal_id: str,
        **kwargs: Any,
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
        self,
        session_id: str,
        terminal_id: str,
        **kwargs: Any,
    ) -> WaitForTerminalExitResponse:
        terminal = self._terminals[terminal_id]
        await terminal.process.wait()
        await terminal.wait_for_streams()
        status = exit_status_from_returncode(terminal.process.returncode)
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

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        pass

    def on_connect(self, conn: Any) -> None:
        pass
