"""CLI commands for exploring asset lineage.

Usage:
    alma-atlas lineage upstream <asset-id>
    alma-atlas lineage downstream <asset-id>
    alma-atlas lineage upstream <asset-id> --depth 3
"""

from __future__ import annotations

from typing import Annotated

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


def _show_lineage(asset_id: str, direction: str, depth: int | None) -> None:
    from alma_analysis.edges import Edge
    from alma_analysis.lineage import compute_lineage
    from alma_atlas.config import get_config
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    cfg = get_config()
    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[yellow]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/yellow]")
        return

    with Database(cfg.db_path) as db:
        raw_edges = EdgeRepository(db).list_all()

    edges = [Edge(upstream_id=e.upstream_id, downstream_id=e.downstream_id, kind=e.kind) for e in raw_edges]
    graph = compute_lineage(edges)

    if not graph.has_asset(asset_id):
        rprint(f"[yellow]Asset not found in lineage graph:[/yellow] [bold]{asset_id}[/bold]")
        return

    if direction == "upstream":
        related = graph.upstream(asset_id, depth=depth)
    else:
        related = graph.downstream(asset_id, depth=depth)

    tree = Tree(f"[bold cyan]{asset_id}[/bold cyan]")
    for node in related:
        tree.add(f"[dim]{node}[/dim]")

    direction_label = "Upstream" if direction == "upstream" else "Downstream"
    rprint(f"[bold]{direction_label} lineage[/bold] ({len(related)} nodes):")
    console.print(tree)
