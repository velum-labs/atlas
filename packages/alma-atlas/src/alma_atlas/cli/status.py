"""CLI command for displaying Atlas graph status.

Usage:
    alma-atlas status
"""

from __future__ import annotations

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Show the current state of the Atlas asset graph.")
console = Console()


@app.callback(invoke_without_command=True)
def status(ctx: typer.Context) -> None:
    """Display a summary of the current Atlas asset graph."""
    if ctx.invoked_subcommand is not None:
        return

    from alma_atlas.config import get_config
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.query_repository import QueryRepository

    cfg = get_config()

    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[yellow]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/yellow]")
        return

    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).list_all()
        edges = EdgeRepository(db).list_all()
        queries = QueryRepository(db).list_all()

    table = Table(title="Alma Atlas Status")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green", justify="right")

    table.add_row("Assets", str(len(assets)))
    table.add_row("Edges", str(len(edges)))
    table.add_row("Query fingerprints", str(len(queries)))

    # Asset breakdown by kind
    kind_counts: dict[str, int] = {}
    for asset in assets:
        kind_counts[asset.kind] = kind_counts.get(asset.kind, 0) + 1
    for kind, count in sorted(kind_counts.items()):
        table.add_row(f"  {kind}", str(count))

    console.print(table)
