"""CLI commands for team sync.

Usage:
    alma-atlas team init --server <url> --key <key> [--team-id <id>]
    alma-atlas team sync
    alma-atlas team status
"""

from __future__ import annotations

import os
from typing import Annotated

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from alma_atlas.bootstrap import load_config as get_config

app = typer.Typer(help="Manage team sync — share Atlas graphs with your team.")
console = Console()


@app.command("init")
def team_init(
    server: Annotated[str, typer.Option("--server", help="Team server URL.")] = "",
    key: Annotated[str, typer.Option("--key", help="Team API key.")] = "",
    key_env: Annotated[
        str | None,
        typer.Option("--key-env", help="Env var containing the team API key."),
    ] = None,
    team_id: Annotated[str | None, typer.Option("--team-id", help="Team ID (defaults to 'default').")] = None,
) -> None:
    """Configure team sync — store server URL, API key, and team ID."""
    if not server:
        rprint("[red]Error:[/red] --server is required")
        raise typer.Exit(1)
    if key and key_env:
        rprint("[red]Error:[/red] Use either --key or --key-env, not both")
        raise typer.Exit(1)
    resolved_key = key or (os.environ.get(key_env or "") if key_env else "")
    if not resolved_key:
        rprint("[red]Error:[/red] --key or --key-env is required")
        raise typer.Exit(1)

    cfg = get_config()
    cfg.team_server_url = server
    cfg.team_api_key = resolved_key
    cfg.team_id = team_id or "default"
    cfg.save_team_config()

    rprint("[green]Team sync configured[/green]")
    rprint(f"  Server:  {server}")
    rprint(f"  Team ID: {cfg.team_id}")


@app.command("sync")
def team_sync() -> None:
    """Push local changes to the team server and pull team contracts."""
    from alma_atlas.application.query.service import require_db_path
    from alma_atlas.application.sync.use_cases import run_team_sync
    from alma_atlas.async_utils import run_sync

    cfg = get_config()
    cfg.load_team_config()

    if not cfg.team_server_url or not cfg.team_api_key:
        rprint("[yellow]Team sync not configured. Run [bold]alma-atlas team init[/bold] first.[/yellow]")
        raise typer.Exit(1)

    try:
        require_db_path(cfg)
    except ValueError as exc:
        rprint(f"[yellow]{exc}[/yellow]")
        raise typer.Exit(1) from exc

    with console.status("[bold]Syncing with team server…"):
        try:
            response = run_sync(run_team_sync(cfg))
            rprint(
                f"[green]Sync complete[/green] — "
                f"{response.accepted_count} record(s) accepted, "
                f"{len(response.rejected)} rejected"
            )
            if response.rejected:
                for r in response.rejected:
                    rprint(f"  [yellow]rejected[/yellow] {r.id}: {r.reason}")
        except Exception as exc:
            from alma_ports.errors import SyncError

            sync_err = SyncError(str(exc))
            rprint(f"[red]Sync failed:[/red] {sync_err}")
            raise typer.Exit(1) from sync_err


@app.command("status")
def team_status() -> None:
    """Show sync state — last sync cursor and pending change counts."""
    from alma_atlas.application.team.status import get_team_sync_status

    cfg = get_config()
    status = get_team_sync_status(cfg)

    table = Table(title="Team Sync Status", show_header=False, box=None, padding=(0, 2))
    table.add_column("Key", style="bold")
    table.add_column("Value")

    if status.server_url:
        table.add_row("Server", status.server_url)
        table.add_row("Team ID", status.team_id or "default")
        table.add_row("Last sync cursor", status.cursor or "[dim]never synced[/dim]")

        if status.pending_asset_changes is not None:
            table.add_row("Pending asset changes", str(status.pending_asset_changes))
        if status.asset_count is not None:
            table.add_row("Total assets", str(status.asset_count))
        if status.edge_count is not None:
            table.add_row("Total edges", str(status.edge_count))
        if status.contract_count is not None:
            table.add_row("Total contracts", str(status.contract_count))
    else:
        table.add_row("Status", "[dim]not configured — run alma-atlas team init[/dim]")

    console.print(table)
