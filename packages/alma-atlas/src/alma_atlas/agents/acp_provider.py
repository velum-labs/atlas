"""ACP (Agent Client Protocol) LLM provider for Atlas learning pipeline.

Spawns an ACP-compatible agent subprocess (e.g. ``claude-agent-acp``) and
communicates with it via the ACP stdio JSON-RPC protocol.  Structured output
is obtained by instructing the agent to write JSON to a temp file, then
validating the file content with Pydantic.

On validation failure the provider sends the error details back to the agent
for self-healing, retrying up to :data:`MAX_RETRIES` times.

Session reuse
-------------
The agent subprocess is spawned on the first :meth:`ACPProvider.analyze` call
and kept alive for the lifetime of the provider instance.  Multiple
``analyze()`` calls (explorer -> pipeline_analyzer -> annotator) share the
same session, preserving conversation context.

Call :meth:`ACPProvider.aclose` (or use the provider as an async context
manager) to terminate the subprocess cleanly.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, TypeVar

from acp import PROTOCOL_VERSION, spawn_agent_process, text_block
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

MAX_RETRIES = 3


class SimpleClient:
    """Minimal ACP client that handles write_text_file and auto-approves permissions.

    Stores file content written by the agent so :class:`ACPProvider` can
    retrieve it after each :meth:`~acp.ClientSideConnection.prompt` call.
    """

    def __init__(self) -> None:
        self._written_files: dict[str, str] = {}

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    async def write_text_file(
        self, content: str, path: str, session_id: str, **kwargs: Any
    ) -> Any:
        from acp import WriteTextFileResponse

        logger.debug("SimpleClient.write_text_file: path=%s session=%s", path, session_id)
        self._written_files[path] = content
        # Also write to disk so the agent can verify its own output if needed.
        try:
            Path(path).write_text(content)
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
    ) -> Any:
        from acp import ReadTextFileResponse

        try:
            content = Path(path).read_text()
        except OSError:
            content = ""
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

    # ------------------------------------------------------------------
    # Terminal stubs -- not used by Atlas
    # ------------------------------------------------------------------

    async def create_terminal(self, command: str, session_id: str, **kwargs: Any) -> Any:
        from acp.schema import CreateTerminalResponse

        raise RuntimeError("ACPProvider: terminal operations are not supported")

    async def terminal_output(self, session_id: str, terminal_id: str, **kwargs: Any) -> Any:
        from acp.schema import TerminalOutputResponse

        raise RuntimeError("ACPProvider: terminal operations are not supported")

    async def release_terminal(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> Any:
        return None

    async def wait_for_terminal_exit(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> Any:
        raise RuntimeError("ACPProvider: terminal operations are not supported")

    async def kill_terminal(self, session_id: str, terminal_id: str, **kwargs: Any) -> Any:
        return None

    # ------------------------------------------------------------------
    # Extension points -- no-ops
    # ------------------------------------------------------------------

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        pass

    def on_connect(self, conn: Any) -> None:
        pass


class ACPProvider:
    """LLMProvider implementation backed by an ACP agent subprocess.

    Spawns the configured agent binary on the first :meth:`analyze` call and
    keeps the session alive for subsequent calls within the same run.

    Args:
        command: ACP-compatible binary to spawn (default: ``claude-agent-acp``).
        args:    Extra CLI arguments passed to the binary.
        env:     Additional environment variables merged with the current
                 process environment before spawning the subprocess.
        cwd:     Working directory for the agent session.  Defaults to the
                 current working directory at construction time.
    """

    def __init__(
        self,
        *,
        command: str = "claude-agent-acp",
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
    ) -> None:
        self._command = command
        self._args: list[str] = list(args or [])
        self._extra_env: dict[str, str] = dict(env or {})
        self._cwd = cwd or os.getcwd()
        self._client: SimpleClient | None = None
        self._conn: Any = None  # ClientSideConnection
        self._session_id: str | None = None
        self._cm: Any = None  # async context manager holding the subprocess

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> None:
        """Spawn the agent and initialise the session on first call."""
        if self._conn is not None:
            return

        self._client = SimpleClient()

        # Merge extra env vars on top of current process environment.
        merged_env: dict[str, str] | None = None
        if self._extra_env:
            merged_env = {**os.environ, **self._extra_env}

        logger.debug(
            "ACPProvider: spawning agent: command=%s args=%s",
            self._command,
            self._args,
        )
        self._cm = spawn_agent_process(
            self._client,
            self._command,
            *self._args,
            env=merged_env,
        )
        self._conn, _proc = await self._cm.__aenter__()
        await self._conn.initialize(protocol_version=PROTOCOL_VERSION)
        resp = await self._conn.new_session(cwd=self._cwd, mcp_servers=[])
        self._session_id = resp.session_id
        logger.debug("ACPProvider: session created: session_id=%s", self._session_id)

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
        await self._ensure_session()
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
        if self._cm is not None:
            try:
                await self._cm.__aexit__(None, None, None)
            except Exception as exc:
                logger.debug("ACPProvider.aclose: error during cleanup: %s", exc)
            self._conn = None
            self._cm = None
            self._client = None
            self._session_id = None
            logger.debug("ACPProvider: agent subprocess terminated")

    async def __aenter__(self) -> "ACPProvider":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()
