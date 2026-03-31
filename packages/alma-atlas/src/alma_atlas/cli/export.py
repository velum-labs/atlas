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
    from alma_atlas.graph_service import export_graph, require_db_path

    cfg = get_config()
    try:
        db_path = require_db_path(cfg)
    except ValueError as exc:
        rprint(f"[yellow]{exc}[/yellow]")
        raise typer.Exit(1) from exc

    data = export_graph(db_path)

    serialized = json.dumps(data, indent=2)

    if output:
        with open(output, "w") as f:
            f.write(serialized)
        rprint(
            f"[green]Exported[/green] {len(data['assets'])} assets and {len(data['edges'])} edges to [bold]{output}[/bold]"
        )
    else:
        print(serialized)
