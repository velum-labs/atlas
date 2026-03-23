"""CLI commands for exporting the Atlas asset graph.

Usage:
    alma-atlas export json                  # Export full graph as JSON
    alma-atlas export json --output out.json
"""

from __future__ import annotations

import json
from typing import Annotated

import typer
from rich import print as rprint

app = typer.Typer(help="Export the Atlas asset graph in various formats.")


@app.command("json")
def export_json(
    output: Annotated[str | None, typer.Option("--output", "-o", help="Output file path (stdout if omitted).")] = None,
) -> None:
    """Export the full asset graph as JSON."""
    from alma_atlas.config import get_config
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    cfg = get_config()
    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[yellow]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/yellow]")
        raise typer.Exit(1)

    with Database(cfg.db_path) as db:
        assets = AssetRepository(db).list_all()
        edges = EdgeRepository(db).list_all()

    data = {
        "assets": [
            {
                "id": a.id,
                "source": a.source,
                "kind": a.kind,
                "name": a.name,
                "description": a.description,
                "tags": a.tags,
            }
            for a in assets
        ],
        "edges": [
            {
                "upstream_id": e.upstream_id,
                "downstream_id": e.downstream_id,
                "kind": e.kind,
            }
            for e in edges
        ],
    }

    serialized = json.dumps(data, indent=2)

    if output:
        with open(output, "w") as f:
            f.write(serialized)
        rprint(f"[green]Exported[/green] {len(assets)} assets and {len(edges)} edges to [bold]{output}[/bold]")
    else:
        print(serialized)
