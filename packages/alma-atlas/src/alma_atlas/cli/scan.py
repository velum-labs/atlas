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
    no_sync: Annotated[bool, typer.Option("--no-sync", help="Skip automatic team sync after scan.")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Show verbose output including warnings.")] = False,
) -> None:
    """Scan data sources and populate the Atlas asset graph."""
    if ctx.invoked_subcommand is not None:
        return

    import logging as _logging

    if not verbose:
        # Suppress noisy sqlglot parse warnings (e.g. TRUNCATE unsupported syntax)
        _logging.getLogger("sqlglot").setLevel(_logging.ERROR)

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

    from alma_atlas.pipeline.scan import run_scan_all

    failed_sources: list[str] = []
    scan_error: str | None = None

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task(f"Scanning {len(sources)} source(s)...", total=None)
        try:
            all_result = run_scan_all(sources, cfg)
            for result in all_result.results:
                if result.error:
                    rprint(f"  [red]Failed:[/red] {result.source_id} — {result.error}")
                    failed_sources.append(result.source_id)
                else:
                    rprint(
                        f"  [green]Done:[/green] {result.source_id} — "
                        f"{result.asset_count} assets, {result.edge_count} edges"
                    )
            progress.update(
                task,
                description=(
                    f"[green]Scan complete[/green] — "
                    f"{all_result.cross_system_edge_count} cross-system edge(s) discovered"
                ),
            )
        except Exception as e:
            scan_error = str(e)
            progress.update(task, description=f"[red]Scan failed:[/red] {e}")
        finally:
            progress.stop_task(task)

    # Auto-sync if team is configured and --no-sync not passed
    if not no_sync:
        cfg.load_team_config()
        if cfg.team_server_url and cfg.team_api_key:
            import asyncio

            from alma_atlas.sync.auth import TeamAuth
            from alma_atlas.sync.client import SyncClient
            from alma_atlas_store.db import Database

            try:
                auth = TeamAuth(cfg.team_api_key)
                client = SyncClient(cfg.team_server_url, auth, cfg.team_id or "default")
                with Database(cfg.db_path) as db:
                    asyncio.run(client.full_sync(db, cfg))
                rprint("[dim]Team sync complete.[/dim]")
            except Exception as exc:
                if verbose:
                    rprint(f"[yellow]Team sync failed (continuing):[/yellow] {exc}")

    # Exit codes: 0 = all succeeded, 1 = partial (some sources failed), 3 = complete failure
    if scan_error is not None:
        raise typer.Exit(3)
    if failed_sources:
        raise typer.Exit(1)
