"""CLI commands for registering and managing data source connections.

Usage:
    alma-atlas connect bigquery --project my-project
    alma-atlas connect postgres --dsn postgresql://user:pass@host/db
    alma-atlas connect snowflake --account xy12345 --user admin
    alma-atlas connect dbt --manifest ./target/manifest.json
    alma-atlas connect list
    alma-atlas connect remove <source-id>
"""

from __future__ import annotations

from typing import Annotated

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from alma_atlas.config import SourceConfig, get_config

app = typer.Typer(help="Register and manage data source connections.")
console = Console()


@app.command("bigquery")
def connect_bigquery(
    project: Annotated[str, typer.Option("--project", "-p", help="GCP project ID.")],
    credentials: Annotated[str | None, typer.Option(help="Path to service account JSON.")] = None,
) -> None:
    """Register a Google BigQuery data source."""
    cfg = get_config()
    source = SourceConfig(
        id=f"bigquery:{project}",
        kind="bigquery",
        params={"project": project, **({"credentials": credentials} if credentials else {})},
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] BigQuery project [bold]{project}[/bold]")


@app.command("postgres")
def connect_postgres(
    dsn: Annotated[str, typer.Option("--dsn", help="PostgreSQL connection string.")],
    schema: Annotated[str, typer.Option("--schema", help="Schema to scan.")] = "public",
) -> None:
    """Register a PostgreSQL data source."""
    cfg = get_config()
    db_name = dsn.rsplit("/", 1)[-1].split("?")[0]
    source = SourceConfig(
        id=f"postgres:{db_name}",
        kind="postgres",
        params={"dsn": dsn, "schema": schema},
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Postgres database [bold]{db_name}[/bold]")


@app.command("snowflake")
def connect_snowflake(
    account: Annotated[str, typer.Option("--account", help="Snowflake account identifier.")],
    user: Annotated[str, typer.Option("--user", help="Snowflake username.")],
    warehouse: Annotated[str | None, typer.Option("--warehouse")] = None,
    database: Annotated[str | None, typer.Option("--database")] = None,
    schema: Annotated[str, typer.Option("--schema")] = "PUBLIC",
) -> None:
    """Register a Snowflake data source."""
    cfg = get_config()
    source = SourceConfig(
        id=f"snowflake:{account}",
        kind="snowflake",
        params={
            "account": account,
            "user": user,
            **({} if warehouse is None else {"warehouse": warehouse}),
            **({} if database is None else {"database": database}),
            "schema": schema,
        },
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Snowflake account [bold]{account}[/bold]")


@app.command("dbt")
def connect_dbt(
    manifest: Annotated[str, typer.Option("--manifest", "-m", help="Path to manifest.json.")],
    project: Annotated[str | None, typer.Option("--project", help="dbt project name override.")] = None,
) -> None:
    """Register a dbt project source."""
    cfg = get_config()
    source = SourceConfig(
        id=f"dbt:{project or 'project'}",
        kind="dbt",
        params={"manifest_path": manifest, **({"project_name": project} if project else {})},
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] dbt project from [bold]{manifest}[/bold]")


@app.command("list")
def list_sources() -> None:
    """List all registered data sources."""
    cfg = get_config()
    sources = cfg.load_sources()

    if not sources:
        rprint("[dim]No sources registered. Use [bold]alma-atlas connect <type>[/bold] to add one.[/dim]")
        return

    table = Table(title="Registered Sources")
    table.add_column("ID", style="cyan")
    table.add_column("Kind", style="magenta")
    for source in sources:
        table.add_row(source.id, source.kind)
    console.print(table)


@app.command("remove")
def remove_source(
    source_id: Annotated[str, typer.Argument(help="Source ID to remove.")],
) -> None:
    """Remove a registered data source."""
    cfg = get_config()
    if cfg.remove_source(source_id):
        rprint(f"[green]Removed:[/green] {source_id}")
    else:
        rprint(f"[red]Not found:[/red] {source_id}")
        raise typer.Exit(1)
