"""CLI command for starting the Alma Atlas MCP server.

Two modes:

    alma-atlas serve                                 # Default: 20 atlas_* tools
    alma-atlas serve --alma-token <invite>           # Companion: 3 companion_* tools

Companion mode validates the invite token against the Alma deployment
endpoint on every MCP tool call (per eng review Issue 3A — no caching,
instant revocation). The token can be passed via `--alma-token` or via the
`ALMA_INVITE_TOKEN` env var; the endpoint defaults to `https://app.alma.dev`
and can be overridden via `--alma-endpoint` or `ALMA_ENDPOINT`.

Transport flags (`--transport`, `--port`, `--host`) apply to both modes.
"""

from __future__ import annotations

from typing import Annotated, Any

import typer
from rich import print as rprint

app = typer.Typer(help="Start the Alma Atlas MCP server.")

DEFAULT_ALMA_ENDPOINT = "https://app.alma.dev"


@app.callback(invoke_without_command=True)
def serve(
    ctx: typer.Context,
    transport: Annotated[
        str, typer.Option("--transport", "-t", help="MCP transport: stdio or sse.")
    ] = "stdio",
    port: Annotated[int, typer.Option("--port", "-p", help="Port for SSE transport.")] = 8080,
    host: Annotated[str, typer.Option("--host", help="Host for SSE transport.")] = "127.0.0.1",
    alma_token: Annotated[
        str | None,
        typer.Option(
            "--alma-token",
            envvar="ALMA_INVITE_TOKEN",
            help="Run in Atlas Companion mode with the given invite token. "
            "Validates the token on every MCP tool call.",
        ),
    ] = None,
    alma_endpoint: Annotated[
        str,
        typer.Option(
            "--alma-endpoint",
            envvar="ALMA_ENDPOINT",
            help="Alma deployment endpoint to validate invite tokens against.",
        ),
    ] = DEFAULT_ALMA_ENDPOINT,
) -> None:
    """Start the Alma Atlas MCP server for AI agent integration."""
    if ctx.invoked_subcommand is not None:
        return

    from alma_atlas.bootstrap import load_config as get_config
    from alma_atlas.mcp import tools
    from alma_atlas.mcp.server import create_server

    cfg = get_config()

    if alma_token is not None:
        token_validator = _build_token_validator(alma_token, alma_endpoint)
        modules = tools.COMPANION_CATEGORY_MODULES
        rprint(
            f"[bold]Atlas Companion[/bold] — transport: [cyan]{transport}[/cyan], "
            f"alma: [cyan]{alma_endpoint}[/cyan]"
        )
    else:
        token_validator = None
        modules = None
        rprint(f"[bold]Alma Atlas MCP Server[/bold] — transport: [cyan]{transport}[/cyan]")

    server = create_server(cfg, modules=modules, token_validator=token_validator)

    if transport == "stdio":
        import asyncio

        from mcp.server.stdio import stdio_server

        async def _run() -> None:
            async with stdio_server() as (read_stream, write_stream):
                await server.run(read_stream, write_stream, server.create_initialization_options())

        asyncio.run(_run())
    elif transport == "sse":
        rprint(f"Listening on [cyan]http://{host}:{port}[/cyan]")
        import uvicorn  # type: ignore[import-untyped]
        from mcp.server.sse import SseServerTransport
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request: Any) -> object:
            async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
                await server.run(streams[0], streams[1], server.create_initialization_options())

        starlette_app = Starlette(
            routes=[Mount("/sse", app=sse.handle_post_message), Route("/sse", endpoint=handle_sse)]
        )
        uvicorn.run(starlette_app, host=host, port=port)
    else:
        rprint(f"[red]Unknown transport:[/red] {transport}. Use 'stdio' or 'sse'.")
        raise typer.Exit(1)


def _build_token_validator(token: str, endpoint: str):
    """Build the per-call token validator closure for Companion mode.

    The validator captures `token` and `endpoint` so the dispatcher can call
    it with no arguments. Imported lazily so non-Companion serve sessions
    don't pay the import cost.
    """
    from alma_atlas.auth.invite_token import validate_token

    def _validate():
        return validate_token(token, endpoint)

    return _validate
