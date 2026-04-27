"""Alma Atlas CLI — main entry point.

Registers all subcommands and defines the root app. This module is the
target of the ``alma-atlas`` script entry point defined in pyproject.toml.
"""

from __future__ import annotations

import typer
from rich import print as rprint
from rich.console import Console

import alma_atlas
from alma_atlas.cli import (
    connect,
    enforce,
    export,
    hooks,
    install,
    learn,
    lineage,
    scan,
    search,
    serve,
    status,
    team,
    uninstall,
)

app = typer.Typer(
    name="alma-atlas",
    help="Alma Atlas — open-source data stack discovery CLI + MCP server.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

# Register subcommand modules
app.add_typer(connect.app, name="connect")
app.add_typer(scan.app, name="scan")
app.add_typer(serve.app, name="serve")
app.add_typer(status.app, name="status")
app.add_typer(search.app, name="search")
app.add_typer(lineage.app, name="lineage")
app.add_typer(export.app, name="export")
app.add_typer(enforce.app, name="enforce")
app.add_typer(learn.app, name="learn")
app.add_typer(team.app, name="team")
app.add_typer(hooks.app, name="hooks")
app.add_typer(install.app, name="install")
app.add_typer(uninstall.app, name="uninstall")

console = Console()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(False, "--version", "-v", help="Show version and exit."),
) -> None:
    """Alma Atlas — discover, map, and understand your data stack."""
    if version:
        rprint(f"[bold]alma-atlas[/bold] [green]{alma_atlas.__version__}[/green]")
        raise typer.Exit()
    if ctx.invoked_subcommand is None:
        rprint(ctx.get_help())
