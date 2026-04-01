"""CLI command for starting the Alma Atlas MCP server.

Usage:
    alma-atlas serve                        # Start MCP server on stdio
    alma-atlas serve --transport sse        # Start MCP server with SSE transport
    alma-atlas serve --port 8080            # SSE server on a specific port
"""

from __future__ import annotations

from typing import Annotated, Any

import typer
from rich import print as rprint

app = typer.Typer(help="Start the Alma Atlas MCP server.")


@app.callback(invoke_without_command=True)
def serve(
    ctx: typer.Context,
    transport: Annotated[str, typer.Option("--transport", "-t", help="MCP transport: stdio or sse.")] = "stdio",
    port: Annotated[int, typer.Option("--port", "-p", help="Port for SSE transport.")] = 8080,
    host: Annotated[str, typer.Option("--host", help="Host for SSE transport.")] = "127.0.0.1",
) -> None:
    """Start the Alma Atlas MCP server for AI agent integration."""
    if ctx.invoked_subcommand is not None:
        return

    from alma_atlas.bootstrap import load_config as get_config
    from alma_atlas.mcp.server import create_server

    cfg = get_config()

    rprint(f"[bold]Alma Atlas MCP Server[/bold] — transport: [cyan]{transport}[/cyan]")

    server = create_server(cfg)

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
