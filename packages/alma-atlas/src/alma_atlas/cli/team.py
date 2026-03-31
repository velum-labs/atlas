"""CLI commands for team sync.

Usage:
    alma-atlas team init --server <url> --key <key> [--team-id <id>]
    alma-atlas team sync
    alma-atlas team status
"""

from __future__ import annotations

import asyncio
from typing import Annotated

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from alma_atlas.config import get_config

app = typer.Typer(help="Manage team sync — share Atlas graphs with your team.")
console = Console()


@app.command("init")
def team_init(
    server: Annotated[str, typer.Option("--server", help="Team server URL.")] = "",
    key: Annotated[str, typer.Option("--key", help="Team API key.")] = "",
    team_id: Annotated[str | None, typer.Option("--team-id", help="Team ID (defaults to 'default').")] = None,
) -> None:
    """Configure team sync — store server URL, API key, and team ID."""
    if not server:
        rprint("[red]Error:[/red] --server is required")
        raise typer.Exit(1)
    if not key:
        rprint("[red]Error:[/red] --key is required")
        raise typer.Exit(1)

    cfg = get_config()
    cfg.team_server_url = server
    cfg.team_api_key = key
    cfg.team_id = team_id or "default"
    cfg.save_team_config()

    rprint("[green]Team sync configured[/green]")
    rprint(f"  Server:  {server}")
    rprint(f"  Team ID: {cfg.team_id}")


@app.command("sync")
def team_sync() -> None:
    """Push local changes to the team server and pull team contracts."""
    from alma_atlas.graph_service import run_team_sync

    cfg = get_config()
    cfg.load_team_config()

    if not cfg.team_server_url or not cfg.team_api_key:
        rprint("[yellow]Team sync not configured. Run [bold]alma-atlas team init[/bold] first.[/yellow]")
        raise typer.Exit(1)

    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[yellow]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/yellow]")
        raise typer.Exit(1)

    with console.status("[bold]Syncing with team server…"):
        try:
            response = asyncio.run(run_team_sync(cfg))
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
    cfg = get_config()
    cfg.load_team_config()

    table = Table(title="Team Sync Status", show_header=False, box=None, padding=(0, 2))
    table.add_column("Key", style="bold")
    table.add_column("Value")

    if cfg.team_server_url:
        table.add_row("Server", cfg.team_server_url)
        table.add_row("Team ID", cfg.team_id or "default")
        cursor = cfg.load_sync_cursor()
        table.add_row("Last sync cursor", cursor or "[dim]never synced[/dim]")

        if cfg.db_path and cfg.db_path.exists():
            from alma_atlas.sync.client import _parse_ts
            from alma_atlas_store.asset_repository import AssetRepository
            from alma_atlas_store.contract_repository import ContractRepository
            from alma_atlas_store.db import Database
            from alma_atlas_store.edge_repository import EdgeRepository

            with Database(cfg.db_path) as db:
                asset_count = len(AssetRepository(db).list_all())
                edge_count = len(EdgeRepository(db).list_all())
                contract_count = len(ContractRepository(db).list_all())

            if cursor:
                from alma_atlas_store.asset_repository import AssetRepository
                from alma_atlas_store.db import Database

                with Database(cfg.db_path) as db:
                    pending = len(
                        [
                            asset
                            for asset in AssetRepository(db).list_all()
                            if _parse_ts(asset.last_seen) >= _parse_ts(cursor)
                        ]
                    )
                table.add_row("Pending asset changes", str(pending))
            table.add_row("Total assets", str(asset_count))
            table.add_row("Total edges", str(edge_count))
            table.add_row("Total contracts", str(contract_count))
    else:
        table.add_row("Status", "[dim]not configured — run alma-atlas team init[/dim]")

    console.print(table)
