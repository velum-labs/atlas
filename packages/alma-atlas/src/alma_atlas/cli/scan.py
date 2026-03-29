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

from alma_atlas.ci_support import (
    resolve_runtime_sources,
    serialize_dry_run_sources,
    serialize_scan_result,
    write_payload,
)
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
    output_format: Annotated[str, typer.Option("--format", help="Output format: text or json.")] = "text",
    output: Annotated[str | None, typer.Option("--output", "-o", help="Write JSON output to a file.")] = None,
    config_file: Annotated[
        str | None,
        typer.Option("--config-file", help="Optional path to atlas.yml for runtime source loading."),
    ] = None,
    connections: Annotated[
        str | None,
        typer.Option(
            "--connections",
            help="Inline JSON/YAML or a path to JSON/YAML defining runtime source configs.",
        ),
    ] = None,
) -> None:
    """Scan data sources and populate the Atlas asset graph."""
    if ctx.invoked_subcommand is not None:
        return

    normalized_output_format = output_format.strip().lower()
    if normalized_output_format not in {"json", "text"}:
        rprint("[red]Invalid format.[/red] Must be one of: text, json")
        raise typer.Exit(1)

    import logging as _logging

    if not verbose:
        # Suppress noisy sqlglot parse warnings (e.g. TRUNCATE unsupported syntax)
        _logging.getLogger("sqlglot").setLevel(_logging.ERROR)

    if config_file is None and connections is None:
        cfg = get_config()
        sources = cfg.load_sources()
    else:
        cfg, sources = resolve_runtime_sources(
            config_file=config_file,
            connections=connections,
        )

    if not sources:
        if normalized_output_format == "json":
            write_payload(
                {
                    "status": "failed",
                    "error": "No sources registered. Run `alma-atlas connect` first.",
                },
                output=output,
            )
        else:
            rprint("[yellow]No sources registered. Run [bold]alma-atlas connect[/bold] first.[/yellow]")
        raise typer.Exit(1)

    if source:
        sources = [s for s in sources if s.id == source]
        if not sources:
            if normalized_output_format == "json":
                write_payload(
                    {
                        "status": "failed",
                        "error": f"Source not found: {source}",
                    },
                    output=output,
                )
            else:
                rprint(f"[red]Source not found:[/red] {source}")
            raise typer.Exit(1)

    if dry_run:
        if normalized_output_format == "json":
            write_payload(serialize_dry_run_sources(sources), output=output)
        else:
            rprint("[dim]Dry run — no changes will be written.[/dim]")
            for s in sources:
                rprint(f"  Would scan: [cyan]{s.id}[/cyan] ([magenta]{s.kind}[/magenta])")
        return

    from alma_atlas.pipeline.scan import run_scan_all

    cfg.ensure_dir()
    failed_sources: list[str] = []
    scan_error: str | None = None
    all_result = None

    if normalized_output_format == "json":
        try:
            all_result = run_scan_all(sources, cfg)
            for result in all_result.results:
                if result.error:
                    failed_sources.append(result.source_id)
            write_payload(serialize_scan_result(all_result), output=output)
        except Exception as e:
            scan_error = str(e)
            write_payload({"status": "failed", "error": str(e)}, output=output)
    else:
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

    # Fire post-scan hooks for each source result.
    if cfg.hooks and all_result is not None:
        import asyncio as _asyncio
        from datetime import UTC, datetime

        from alma_atlas.hooks import HookEvent, HookExecutor

        executor = HookExecutor(cfg.hooks)

        async def _fire_all_hooks() -> None:
            for result in all_result.results:  # type: ignore[union-attr]
                event_type = "scan_error" if result.error else "scan_complete"
                data: dict = {"asset_count": result.asset_count, "edge_count": result.edge_count}
                if result.error:
                    data["error"] = result.error
                if result.warnings:
                    data["warnings"] = list(result.warnings)
                event = HookEvent(
                    event_type=event_type,
                    source_id=result.source_id,
                    timestamp=datetime.now(UTC).isoformat(),
                    data=data,
                )
                hook_results = await executor.fire(event)
                for hr in hook_results:
                    if not hr.success and normalized_output_format != "json":
                        rprint(f"[yellow]Hook {hr.hook_name!r} failed (continuing):[/yellow] {hr.error}")

        _asyncio.run(_fire_all_hooks())

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
                if normalized_output_format != "json":
                    rprint("[dim]Team sync complete.[/dim]")
            except Exception as exc:
                if verbose and normalized_output_format != "json":
                    rprint(f"[yellow]Team sync failed (continuing):[/yellow] {exc}")

    # Exit codes: 0 = all succeeded, 1 = partial (some sources failed), 3 = complete failure
    if scan_error is not None:
        raise typer.Exit(3)
    if failed_sources:
        raise typer.Exit(1)
