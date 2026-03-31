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
    from alma_atlas.graph_service import get_graph_status

    cfg = get_config()

    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[yellow]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/yellow]")
        return

    summary = get_graph_status(cfg.db_path)

    table = Table(title="Alma Atlas Status")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green", justify="right")

    table.add_row("Assets", str(summary.asset_count))
    table.add_row("Edges", str(summary.edge_count))
    table.add_row("Query fingerprints", str(summary.query_count))

    for kind, count in sorted(summary.kind_counts.items()):
        table.add_row(f"  {kind}", str(count))

    console.print(table)
