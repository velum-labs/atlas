"""CLI commands for scanning registered data sources.

Usage:
    alma-atlas scan                  # Scan all registered sources
    alma-atlas scan --source <id>    # Scan a specific source
"""

from __future__ import annotations

from typing import Annotated

import typer
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from alma_atlas.config import get_config

app = typer.Typer(help="Scan registered data sources to discover assets and lineage.")
console = Console()


@app.callback(invoke_without_command=True)
def scan(
    ctx: typer.Context,
    source: Annotated[str | None, typer.Option("--source", "-s", help="Scan a specific source ID.")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Print what would be scanned without writing.")] = False,
) -> None:
    """Scan data sources and populate the Atlas asset graph."""
    if ctx.invoked_subcommand is not None:
        return

    cfg = get_config()
    sources = cfg.load_sources()

    if not sources:
        rprint("[yellow]No sources registered. Run [bold]alma-atlas connect[/bold] first.[/yellow]")
        raise typer.Exit(1)

    if source:
        sources = [s for s in sources if s.id == source]
        if not sources:
            rprint(f"[red]Source not found:[/red] {source}")
            raise typer.Exit(1)

    if dry_run:
        rprint("[dim]Dry run — no changes will be written.[/dim]")
        for s in sources:
            rprint(f"  Would scan: [cyan]{s.id}[/cyan] ([magenta]{s.kind}[/magenta])")
        return

    from alma_atlas.pipeline.scan import run_scan

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        for src in sources:
            task = progress.add_task(f"Scanning [cyan]{src.id}[/cyan]...", total=None)
            try:
                result = run_scan(src, cfg)
                progress.update(
                    task,
                    description=f"[green]Done:[/green] {src.id} — {result.asset_count} assets, {result.edge_count} edges",
                )
            except Exception as e:
                progress.update(task, description=f"[red]Failed:[/red] {src.id} — {e}")
            finally:
                progress.stop_task(task)
