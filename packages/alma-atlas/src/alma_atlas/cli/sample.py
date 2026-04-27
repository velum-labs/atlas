"""CLI: install the bundled sample-data SQLite snapshot.

Per design doc Track 3b: gives a cold dev a working data stack to explore in
60 seconds without scanning a real warehouse. After install, the user can:

    alma-atlas serve                        # Cursor / Claude Desktop sees a real graph
    alma-atlas search orders                # CLI search works against sample
    alma-atlas lineage <asset_id>           # Lineage walks the sample chain

The snapshot is fictitious mid-stage analytics company data: Snowflake +
dbt + Looker with cross-system lineage chains. Refreshed on major releases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich import print as rprint

from alma_atlas import sample_data

app = typer.Typer(help="Install or preview the bundled sample data snapshot.")


def _default_target() -> Path:
    """Default install path: the same db Atlas writes to during a real scan."""
    return Path.home() / ".alma-atlas" / "atlas.db"


@app.command("install")
def install(
    target: Annotated[
        Path | None,
        typer.Option("--target", help="Where to write the SQLite db (default: ~/.alma-atlas/atlas.db)"),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Overwrite the target if it already exists"),
    ] = False,
) -> None:
    """Install the bundled sample data into ~/.alma-atlas/atlas.db (or --target)."""
    target_path = target or _default_target()
    try:
        written = sample_data.install_sample(target_path, overwrite=force)
    except FileExistsError as exc:
        rprint(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc
    except FileNotFoundError as exc:
        rprint(f"[red]Bundled sample missing:[/red] {exc}")
        raise typer.Exit(1) from exc
    rprint(f"[green]Sample data installed at:[/green] {written}")
    rprint(
        "[dim]Try: [/dim]"
        "[cyan]alma-atlas search orders[/cyan][dim] or "
        "[/dim][cyan]alma-atlas lineage 'snowflake:demo::analytics.orders' --direction downstream[/cyan]"
    )


@app.command("preview")
def preview() -> None:
    """Show what the bundled sample data contains without installing it."""
    import gzip
    import tempfile

    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository

    bundled = sample_data.bundled_snapshot_path()
    if not bundled.exists():
        rprint(f"[red]Bundled sample missing at {bundled}[/red]")
        raise typer.Exit(1)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_db = Path(tmpdir) / "atlas-sample.db"
        with gzip.open(bundled, "rb") as src, open(tmp_db, "wb") as dst:
            dst.write(src.read())

        with Database(tmp_db) as db:
            assets = AssetRepository(db).list_all()
            edges = EdgeRepository(db).list_all() if hasattr(EdgeRepository(db), "list_all") else []

    rprint(f"[bold]Bundled snapshot:[/bold] {bundled.name} ({bundled.stat().st_size / 1024:.1f} KB compressed)")
    rprint(f"[bold]Assets:[/bold] {len(assets)}")

    by_source: dict[str, list[str]] = {}
    for asset in assets:
        by_source.setdefault(asset.source, []).append(f"{asset.kind}: {asset.name}")
    for source in sorted(by_source.keys()):
        rprint(f"  [cyan]{source}[/cyan] ({len(by_source[source])} assets)")
        for entry in sorted(by_source[source]):
            rprint(f"    {entry}")

    if edges:
        rprint(f"[bold]Edges:[/bold] {len(edges)}")
