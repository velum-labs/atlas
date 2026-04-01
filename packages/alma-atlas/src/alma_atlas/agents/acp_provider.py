"""Compatibility facade for ACP-backed Atlas learning workflows."""

from __future__ import annotations

import logging
import os
from typing import Any, TypeVar

from acp.schema import InitializeResponse, NewSessionResponse
from pydantic import BaseModel

from alma_atlas.agents.acp.client import SimpleClient
from alma_atlas.agents.acp.common import MAX_RETRIES as _MAX_RETRIES
from alma_atlas.agents.acp.session_runtime import ACPSessionRuntime
from alma_atlas.agents.acp.structured_output import analyze_structured_output

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)
MAX_RETRIES = _MAX_RETRIES


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
        self._conn: Any = None
        self._session_id: str | None = None
        self._cm: Any = None
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

    async def analyze(
        self,
        system_prompt: str,
        user_prompt: str,
        response_schema: type[T],
    ) -> T:
        await self._runtime.ensure_session()
        self._sync_runtime_state()
        assert self._conn is not None
        assert self._client is not None
        assert self._session_id is not None

        return await analyze_structured_output(
            conn=self._conn,
            client=self._client,
            session_id=self._session_id,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_schema=response_schema,
        )

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
