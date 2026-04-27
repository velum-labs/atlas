"""CLI: delete the local Atlas data directory entirely.

Per design doc Security section: this is the documented uninstall path that
guarantees full credential + graph + telemetry-id removal. The directory at
~/.alma-atlas/ stores everything Atlas writes locally; removing it returns
the machine to a pristine pre-Atlas state.

Idempotent: re-running on a machine that's already clean prints a friendly
message and exits 0.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated

import typer
from rich import print as rprint

app = typer.Typer(help="Uninstall Atlas: removes the local Atlas data directory.")


@app.callback(invoke_without_command=True)
def uninstall(
    ctx: typer.Context,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip the confirmation prompt"),
    ] = False,
) -> None:
    """Delete ~/.alma-atlas/ entirely (graph, secrets, telemetry id)."""
    if ctx.invoked_subcommand is not None:
        return

    target = Path.home() / ".alma-atlas"
    if not target.exists():
        rprint("[dim]Nothing to uninstall — ~/.alma-atlas/ does not exist.[/dim]")
        return

    if not yes:
        confirmed = typer.confirm(f"Remove {target} and everything inside?")
        if not confirmed:
            rprint("[dim]Aborted.[/dim]")
            raise typer.Exit(0)

    shutil.rmtree(target)
    rprint(f"[green]Removed:[/green] {target}")
