"""CLI commands for Atlas analysis snapshots and workload analysis."""

from __future__ import annotations

from typing import Annotated

import typer
from rich import print as rprint

from alma_atlas.analysis import (
    build_analysis_snapshot,
    build_analysis_summary,
    run_analysis,
)
from alma_atlas.config import get_config
from alma_atlas_store.db import Database

app = typer.Typer(help="Export and analyze Atlas traffic-aware workloads.")


@app.command("export")
def export_analysis(
    output: Annotated[
        str | None,
        typer.Option("--output", "-o", help="Output file path (stdout if omitted)."),
    ] = None,
    source: Annotated[
        str | None,
        typer.Option("--source", help="Filter to a specific source ID."),
    ] = None,
    top_n: Annotated[
        int,
        typer.Option("--top-n", help="Number of top tables and fingerprints to include."),
    ] = 10,
) -> None:
    """Export a machine-readable Atlas analysis snapshot as JSON."""

    cfg = get_config()
    _ensure_database_exists(cfg)

    with Database(cfg.db_path) as db:
        snapshot = build_analysis_snapshot(db, source=source, top_n=top_n)

    payload = snapshot.to_json()
    if output:
        snapshot.write_json(output)
        rprint(
            "[green]Exported[/green] analysis snapshot with "
            f"{len(snapshot.graph.assets)} assets and "
            f"{snapshot.traffic_summary.query_fingerprint_count} query fingerprints "
            f"to [bold]{output}[/bold]"
        )
        return

    print(payload)


@app.command("summary")
def analysis_summary(
    output: Annotated[
        str | None,
        typer.Option("--output", "-o", help="Output file path (stdout if omitted)."),
    ] = None,
    source: Annotated[
        str | None,
        typer.Option("--source", help="Filter to a specific source ID."),
    ] = None,
    top_n: Annotated[
        int,
        typer.Option("--top-n", help="Number of top tables and fingerprints to include."),
    ] = 10,
) -> None:
    """Print a JSON traffic summary for the current Atlas store."""

    cfg = get_config()
    _ensure_database_exists(cfg)

    with Database(cfg.db_path) as db:
        summary = build_analysis_summary(db, source=source, top_n=top_n)

    payload = summary.to_json()
    if output:
        from pathlib import Path

        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
        rprint(f"[green]Wrote[/green] analysis summary to [bold]{output}[/bold]")
        return

    print(payload)


@app.command("run")
def run_live_analysis(
    output: Annotated[
        str | None,
        typer.Option("--output", "-o", help="Output file path (stdout if omitted)."),
    ] = None,
    source: Annotated[
        str | None,
        typer.Option("--source", help="Filter to a specific source ID."),
    ] = None,
    similarity_threshold: Annotated[
        float | None,
        typer.Option("--similarity-threshold", help="Override the clustering similarity threshold."),
    ] = None,
    min_cluster_size: Annotated[
        int | None,
        typer.Option("--min-cluster-size", help="Override the minimum cluster size."),
    ] = None,
) -> None:
    """Run Atlas-native clustering and candidate derivation."""

    cfg = get_config()
    _ensure_database_exists(cfg)

    with Database(cfg.db_path) as db:
        snapshot = build_analysis_snapshot(db, source=source)
        result = run_analysis(
            snapshot,
            similarity_threshold=similarity_threshold,
            min_cluster_size=min_cluster_size,
        )

    payload = result.to_json()
    if output:
        from pathlib import Path

        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
        rprint(
            "[green]Wrote[/green] analysis result with "
            f"{result.cluster_count} clusters and {result.candidate_count} candidates "
            f"to [bold]{output}[/bold]"
        )
        return

    print(payload)


def _ensure_database_exists(cfg) -> None:
    if not cfg.db_path or not cfg.db_path.exists():
        rprint("[yellow]No Atlas database found. Run [bold]alma-atlas scan[/bold] first.[/yellow]")
        raise typer.Exit(1)
