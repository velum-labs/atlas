"""CLI commands for exploring asset lineage.

Usage:
    alma-atlas lineage upstream <asset-id>
    alma-atlas lineage downstream <asset-id>
    alma-atlas lineage upstream <asset-id> --depth 3
"""

from __future__ import annotations

from typing import Annotated, Literal

import typer
from rich import print as rprint
from rich.console import Console
from rich.tree import Tree

app = typer.Typer(help="Trace upstream and downstream lineage for an asset.")
console = Console()


@app.command("upstream")
def upstream(
    asset_id: Annotated[str, typer.Argument(help="Asset ID to trace upstream from.")],
    depth: Annotated[int | None, typer.Option("--depth", "-d", help="Maximum traversal depth.")] = None,
) -> None:
    """Show all upstream dependencies of an asset."""
    _show_lineage(asset_id, direction="upstream", depth=depth)


@app.command("downstream")
def downstream(
    asset_id: Annotated[str, typer.Argument(help="Asset ID to trace downstream from.")],
    depth: Annotated[int | None, typer.Option("--depth", "-d", help="Maximum traversal depth.")] = None,
) -> None:
    """Show all downstream dependents of an asset."""
    _show_lineage(asset_id, direction="downstream", depth=depth)


def _show_lineage(asset_id: str, direction: Literal["upstream", "downstream"], depth: int | None) -> None:
    from alma_atlas.cli.common import require_db_path_or_exit
    from alma_atlas.graph_service import get_lineage_summary

    summary = get_lineage_summary(require_db_path_or_exit(), asset_id, direction=direction, depth=depth)

    if not summary.asset_exists:
        rprint(f"[yellow]Asset not found in lineage graph:[/yellow] [bold]{asset_id}[/bold]")
        return

    tree = Tree(f"[bold cyan]{asset_id}[/bold cyan]")
    for node in summary.related:
        tree.add(f"[dim]{node}[/dim]")

    direction_label = "Upstream" if direction == "upstream" else "Downstream"
    rprint(f"[bold]{direction_label} lineage[/bold] ({len(summary.related)} nodes):")
    console.print(tree)
