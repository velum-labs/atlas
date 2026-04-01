"""ACP subprocess and session lifecycle management."""

from __future__ import annotations

import logging
import os
from typing import Any, cast

from acp import PROTOCOL_VERSION, spawn_agent_process
from acp.schema import Implementation, InitializeResponse, NewSessionResponse

from .client import SimpleClient
from .common import default_client_info

logger = logging.getLogger(__name__)


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
        self._client_info = client_info or default_client_info()
        self._client: SimpleClient | None = None
        self._conn: Any = None
        self._session_id: str | None = None
        self._cm: Any = None
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
