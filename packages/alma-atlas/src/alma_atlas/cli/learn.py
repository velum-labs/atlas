"""CLI commands for pipeline learning.

Usage:
    atlas learn --repo /path/to/repo   # Learn edges using an LLM
    atlas learn --dry-run              # List unlearned edges without calling LLM
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from alma_atlas.config import get_config

app = typer.Typer(help="Learn edges and assets with agent-inferred metadata.")
console = Console()
logger = logging.getLogger(__name__)


@app.callback(invoke_without_command=True)
def learn(
    ctx: typer.Context,
    repo: Annotated[
        Path | None,
        typer.Option(
            "--repo",
            help="Path to the code repository to scan for pipeline code.",
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
        ),
    ] = None,
    assets: Annotated[
        bool,
        typer.Option(
            "--assets",
            help="Annotate assets (business metadata) instead of edges.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show unlearned items without calling the LLM."),
    ] = False,
) -> None:
    """Learn cross-system edges with pipeline transport metadata.

    Reads pipeline code from --repo and uses an LLM to fill in transport kind,
    schedule, copy strategy, and other metadata for unlearned edges.
    Use --dry-run to preview which edges need learning without calling the LLM.
    """
    if ctx.invoked_subcommand is not None:
        return

    from alma_atlas_store.db import Database

    cfg = get_config()
    assert cfg.db_path is not None, "db_path must be configured"

    if assets:
        from alma_atlas.pipeline.learn import get_unannotated_assets

        with Database(cfg.db_path) as db:
            unannotated = get_unannotated_assets(db)

        if not unannotated:
            console.print("[green]No unannotated assets found.[/green]")
            return

        if dry_run:
            table = Table(title=f"Unannotated assets ({len(unannotated)})")
            table.add_column("Asset ID", style="cyan")
            for asset_id in unannotated:
                table.add_row(asset_id)
            console.print(table)
            return

        if repo is None:
            console.print(
                "[red]Error:[/red] --repo is required when annotating assets.\n"
                "Run [bold]atlas learn --help[/bold] for usage."
            )
            raise typer.Exit(code=1)

        _run_asset_annotation(cfg, repo)
        return

    # Default: edge learning
    from alma_atlas.pipeline.learn import get_unlearned_edges

    with Database(cfg.db_path) as db:
        unlearned = get_unlearned_edges(db)

    if not unlearned:
        console.print("[green]No unlearned edges found.[/green]")
        return

    if dry_run:
        table = Table(title=f"Unlearned edges ({len(unlearned)})")
        table.add_column("Upstream", style="cyan")
        table.add_column("Downstream", style="magenta")
        table.add_column("Kind", style="yellow")
        for edge in unlearned:
            table.add_row(edge.upstream_id, edge.downstream_id, edge.kind)
        console.print(table)
        return

    if repo is None:
        console.print(
            "[red]Error:[/red] --repo is required when not using --dry-run.\n"
            "Run [bold]atlas learn --help[/bold] for usage."
        )
        raise typer.Exit(code=1)

    _run_edge_learning(cfg, repo)


def _run_edge_learning(cfg, repo_path: Path) -> None:
    """Synchronous wrapper — runs edge learning using the per-agent config."""
    from alma_atlas.pipeline.learn import edge_learning_is_enabled, run_edge_learning
    from alma_atlas_store.db import Database

    if not edge_learning_is_enabled(cfg.learning):
        console.print(
            "[red]Error:[/red] No real LLM provider configured for learning.\n"
            "Configure non-mock [bold]learning.explorer[/bold] and "
            "[bold]learning.pipeline_analyzer[/bold] agent settings in atlas.yml."
        )
        raise typer.Exit(code=1)

    with Database(cfg.db_path) as db:
        count = asyncio.run(run_edge_learning(db, repo_path, config=cfg.learning))

    console.print(f"[green]Learned {count} edge(s).[/green]")


def _run_asset_annotation(cfg, repo_path: Path) -> None:
    """Synchronous wrapper -- runs asset annotation using the per-agent config."""
    from alma_atlas.pipeline.learn import asset_annotation_is_enabled, run_asset_annotation
    from alma_atlas_store.db import Database

    if not asset_annotation_is_enabled(cfg.learning):
        console.print(
            "[red]Error:[/red] No real LLM provider configured for learning.\n"
            "Configure non-mock [bold]learning.explorer[/bold] and "
            "[bold]learning.annotator[/bold] agent settings in atlas.yml."
        )
        raise typer.Exit(code=1)

    with Database(cfg.db_path) as db:
        count = asyncio.run(run_asset_annotation(db, repo_path, config=cfg.learning))

    console.print(f"[green]Annotated {count} asset(s).[/green]")
