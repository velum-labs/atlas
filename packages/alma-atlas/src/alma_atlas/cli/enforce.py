"""CLI commands for Alma enforcement — drift detection and contract mode management.

Usage:
    alma-atlas enforce check [--asset <id>]   # Run drift detection, print violations
    alma-atlas enforce set <asset_id> --mode shadow|warn|enforce
    alma-atlas enforce status                 # Show all contracts with enforcement modes
"""

from __future__ import annotations

from typing import Annotated

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from alma_atlas.config import get_config

app = typer.Typer(help="Manage enforcement modes and inspect drift violations.")
console = Console()


@app.command("check")
def check(
    asset: Annotated[str | None, typer.Option("--asset", "-a", help="Check a specific asset ID.")] = None,
) -> None:
    """Run drift detection against the current store and print any violations."""
    cfg = get_config()
    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[red]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/red]")
        raise typer.Exit(1)

    from alma_atlas.enforcement.drift import DriftDetector
    from alma_atlas.enforcement.engine import EnforcementEngine
    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.schema_repository import SchemaRepository

    detector = DriftDetector()

    with Database(cfg.db_path) as db:  # type: ignore[arg-type]
        contract_repo = ContractRepository(db)
        schema_repo = SchemaRepository(db)
        engine = EnforcementEngine(db)

        contracts = contract_repo.list_for_asset(asset) if asset else contract_repo.list_all()
        if not contracts:
            msg = f"No contracts found for asset {asset!r}." if asset else "No contracts registered."
            rprint(f"[yellow]{msg}[/yellow]")
            raise typer.Exit(0)

        # Deduplicate by asset_id so we only compare once per asset.
        seen: set[str] = set()
        total_violations = 0

        for contract in contracts:
            aid = contract.asset_id
            if aid in seen:
                continue
            seen.add(aid)

            history = schema_repo.list_history(aid)
            if len(history) < 2:
                rprint(f"  [dim]{aid}[/dim] — not enough history to diff (need ≥2 snapshots)")
                continue

            previous, current = history[1], history[0]
            report = detector.detect(aid, previous, current)
            if not report.has_violations:
                rprint(f"  [green]✓[/green] {aid} — no drift detected")
                continue

            result = engine.enforce(report, contract.mode)
            total_violations += len(result.violations)
            status = "[red]BLOCKED[/red]" if result.blocked else f"[yellow]{contract.mode.upper()}[/yellow]"
            rprint(f"\n  {status} {aid} — {len(result.violations)} violation(s):")
            for v in result.violations:
                icon = "✗" if v.severity == "error" else "⚠" if v.severity == "warning" else "ℹ"
                rprint(f"    {icon} [{v.severity}] {v.violation_type}: {v.details.get('message', '')}")

    if total_violations:
        rprint(f"\n[bold]{total_violations} violation(s) found and logged.[/bold]")
        raise typer.Exit(1)
    else:
        rprint("\n[green]All checked assets are drift-free.[/green]")


@app.command("set")
def set_mode(
    asset_id: Annotated[str, typer.Argument(help="Asset ID to update enforcement mode for.")],
    mode: Annotated[str, typer.Option("--mode", "-m", help="Enforcement mode: shadow, warn, or enforce.")],
) -> None:
    """Set the enforcement mode for all contracts on an asset."""
    if mode not in ("shadow", "warn", "enforce"):
        rprint(f"[red]Invalid mode {mode!r}. Must be one of: shadow, warn, enforce.[/red]")
        raise typer.Exit(1)

    cfg = get_config()
    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[red]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/red]")
        raise typer.Exit(1)

    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.db import Database

    with Database(cfg.db_path) as db:  # type: ignore[arg-type]
        repo = ContractRepository(db)
        contracts = repo.list_for_asset(asset_id)
        if not contracts:
            rprint(f"[yellow]No contracts found for asset {asset_id!r}.[/yellow]")
            raise typer.Exit(1)

        for contract in contracts:
            contract.mode = mode  # type: ignore[assignment]
            repo.upsert(contract)

    rprint(
        f"[green]Updated {len(contracts)} contract(s) for [bold]{asset_id}[/bold] "
        f"to mode [bold]{mode}[/bold].[/green]"
    )


@app.command("status")
def status() -> None:
    """Show all registered contracts with their enforcement modes."""
    cfg = get_config()
    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[red]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/red]")
        raise typer.Exit(1)

    from alma_atlas_store.contract_repository import ContractRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.violation_repository import ViolationRepository

    with Database(cfg.db_path) as db:  # type: ignore[arg-type]
        contracts = ContractRepository(db).list_all()
        if not contracts:
            rprint("[yellow]No contracts registered.[/yellow]")
            raise typer.Exit(0)

        violation_repo = ViolationRepository(db)
        violation_counts: dict[str, int] = {}
        for contract in contracts:
            aid = contract.asset_id
            if aid not in violation_counts:
                violation_counts[aid] = len(violation_repo.list_for_asset(aid))

    table = Table(title="Contract Enforcement Status", show_lines=False)
    table.add_column("Asset ID", style="cyan", no_wrap=True)
    table.add_column("Contract ID", style="dim")
    table.add_column("Version")
    table.add_column("Status")
    table.add_column("Mode", style="bold")
    table.add_column("Open Violations", justify="right")

    mode_styles = {"shadow": "dim", "warn": "yellow", "enforce": "red"}

    for contract in contracts:
        vcount = violation_counts.get(contract.asset_id, 0)
        vstr = f"[red]{vcount}[/red]" if vcount else "[green]0[/green]"
        mode_style = mode_styles.get(contract.mode, "")
        table.add_row(
            contract.asset_id,
            contract.id,
            contract.version,
            contract.status,
            f"[{mode_style}]{contract.mode}[/{mode_style}]",
            vstr,
        )

    console.print(table)
