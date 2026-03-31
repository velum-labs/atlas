"""CLI command for searching the Atlas asset graph.

Usage:
    alma-atlas search <query>
    alma-atlas search orders --limit 20
"""

from __future__ import annotations

from typing import Annotated

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Search for assets in the Atlas graph.")
console = Console()


@app.callback(invoke_without_command=True)
def search(
    ctx: typer.Context,
    query: Annotated[str | None, typer.Argument(help="Search query string.")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Maximum results.")] = 50,
) -> None:
    """Search for assets by name, ID, or description."""
    if ctx.invoked_subcommand is not None:
        return

    if not query:
        rprint("[yellow]Provide a search query. Example: [bold]alma-atlas search orders[/bold][/yellow]")
        raise typer.Exit(1)

    from alma_atlas.config import get_config
    from alma_atlas.graph_service import search_assets

    cfg = get_config()
    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[yellow]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/yellow]")
        return

    results = search_assets(cfg.db_path, query, limit=limit)

    if not results:
        rprint(f"[dim]No assets found matching [bold]{query!r}[/bold][/dim]")
        return

    table = Table(title=f"Search results for {query!r}")
    table.add_column("ID", style="cyan")
    table.add_column("Kind", style="magenta")
    table.add_column("Source", style="blue")
    for asset in results:
        table.add_row(asset.id, asset.kind, asset.source)
    console.print(table)
