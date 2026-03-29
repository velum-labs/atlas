"""CLI commands for pipeline enrichment.

Usage:
    atlas enrich --repo /path/to/repo   # Enrich edges using an LLM
    atlas enrich --dry-run              # List unenriched edges without calling LLM
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from alma_atlas.config import get_config

app = typer.Typer(help="Enrich edges and assets with agent-inferred metadata.")
console = Console()
logger = logging.getLogger(__name__)


@app.callback(invoke_without_command=True)
def enrich(
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
            help="Enrich assets (business metadata annotations) instead of edges.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show unenriched items without calling the LLM."),
    ] = False,
) -> None:
    """Enrich cross-system edges with pipeline transport metadata.

    Reads pipeline code from --repo and uses an LLM to fill in transport kind,
    schedule, copy strategy, and other metadata for unenriched edges.
    Use --dry-run to preview which edges need enrichment without calling the LLM.
    """
    if ctx.invoked_subcommand is not None:
        return

    from alma_atlas_store.db import Database

    cfg = get_config()
    assert cfg.db_path is not None, "db_path must be configured"

    if assets:
        from alma_atlas.pipeline.enrich import get_unannotated_assets

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
                "[red]Error:[/red] --repo is required when enriching assets.\n"
                "Run [bold]atlas enrich --help[/bold] for usage."
            )
            raise typer.Exit(code=1)

        _run_asset_enrichment(cfg, repo)
        return

    # Default: edge enrichment
    from alma_atlas.pipeline.enrich import get_unenriched_edges

    with Database(cfg.db_path) as db:
        unenriched = get_unenriched_edges(db)

    if not unenriched:
        console.print("[green]No unenriched edges found.[/green]")
        return

    if dry_run:
        table = Table(title=f"Unenriched edges ({len(unenriched)})")
        table.add_column("Upstream", style="cyan")
        table.add_column("Downstream", style="magenta")
        table.add_column("Kind", style="yellow")
        for edge in unenriched:
            table.add_row(edge.upstream_id, edge.downstream_id, edge.kind)
        console.print(table)
        return

    if repo is None:
        console.print(
            "[red]Error:[/red] --repo is required when not using --dry-run.\n"
            "Run [bold]atlas enrich --help[/bold] for usage."
        )
        raise typer.Exit(code=1)

    _run_enrichment(cfg, repo)


def _make_provider_from_cfg(cfg):
    from alma_atlas.agents.provider import make_provider

    enrichment_cfg = cfg.enrichment
    api_key: str | None = None
    if enrichment_cfg.api_key_env:
        api_key = os.environ.get(enrichment_cfg.api_key_env)

    provider = make_provider(
        enrichment_cfg.provider,
        model=enrichment_cfg.model,
        api_key=api_key,
        timeout=float(enrichment_cfg.timeout),
        max_tokens=enrichment_cfg.max_tokens,
    )
    return provider


def _run_enrichment(cfg, repo_path: Path) -> None:
    """Synchronous wrapper — resolves the provider and runs the async edge enrichment."""
    from alma_atlas.pipeline.enrich import run_enrichment
    from alma_atlas_store.db import Database

    provider = _make_provider_from_cfg(cfg)

    with Database(cfg.db_path) as db:
        count = asyncio.run(run_enrichment(db, repo_path, provider))

    console.print(f"[green]Enriched {count} edge(s).[/green]")


def _run_asset_enrichment(cfg, repo_path: Path) -> None:
    """Synchronous wrapper — resolves the provider and runs the async asset enrichment."""
    from alma_atlas.pipeline.enrich import run_asset_enrichment
    from alma_atlas_store.db import Database

    provider = _make_provider_from_cfg(cfg)

    with Database(cfg.db_path) as db:
        count = asyncio.run(
            run_asset_enrichment(
                db,
                repo_path,
                provider,
                provider_name=cfg.enrichment.provider,
                model=cfg.enrichment.model,
            )
        )

    console.print(f"[green]Annotated {count} asset(s).[/green]")
