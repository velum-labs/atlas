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

import json
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from alma_atlas.config import SourceConfig, get_config

app = typer.Typer(help="Register and manage data source connections.")
console = Console()


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or url
    return host.replace(".", "-")


def _read_dbt_project_name(manifest_path: str) -> str | None:
    try:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict):
        return None
    project_name = metadata.get("project_name")
    return project_name.strip() if isinstance(project_name, str) and project_name.strip() else None


@app.command("bigquery")
def connect_bigquery(
    project: Annotated[str, typer.Option("--project", "-p", help="GCP project ID.")],
    credentials: Annotated[str | None, typer.Option(help="Path to service account JSON.")] = None,
    service_account_env: Annotated[
        str | None,
        typer.Option("--service-account-env", help="Env var containing the raw service account JSON payload."),
    ] = None,
    location: Annotated[str, typer.Option("--location", help="BigQuery location / region.")] = "us",
) -> None:
    """Register a Google BigQuery data source."""
    if credentials and service_account_env:
        rprint("[red]Error:[/red] Use either --credentials or --service-account-env, not both.")
        raise typer.Exit(1)
    cfg = get_config()
    params: dict[str, str] = {"project_id": project, "location": location}
    if credentials:
        params["credentials"] = credentials
    if service_account_env:
        params["service_account_env"] = service_account_env
    source = SourceConfig(
        id=f"bigquery:{project}",
        kind="bigquery",
        params=params,
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
    # Include schema in ID to allow multiple schemas from same database
    source_id = f"postgres:{db_name}" if schema == "public" else f"postgres:{db_name}:{schema}"
    source = SourceConfig(
        id=source_id,
        kind="postgres",
        params={"dsn": dsn, "include_schemas": [schema]},
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Postgres database [bold]{db_name}[/bold] (schema: {schema})")


@app.command("snowflake")
def connect_snowflake(
    account: Annotated[str, typer.Option("--account", help="Snowflake account identifier.")],
    account_secret_env: Annotated[
        str,
        typer.Option(
            "--account-secret-env",
            help="Env var containing the Snowflake connection JSON payload.",
        ),
    ] = "SNOWFLAKE_CONNECTION_JSON",
    warehouse: Annotated[str | None, typer.Option("--warehouse")] = None,
    database: Annotated[str | None, typer.Option("--database")] = None,
    role: Annotated[str | None, typer.Option("--role")] = None,
    schema: Annotated[str | None, typer.Option("--schema")] = None,
) -> None:
    """Register a Snowflake data source."""
    cfg = get_config()
    params: dict[str, object] = {
        "account": account,
        "account_secret_env": account_secret_env,
    }
    if warehouse is not None:
        params["warehouse"] = warehouse
    if database is not None:
        params["database"] = database
    if role is not None:
        params["role"] = role
    if schema is not None:
        params["include_schemas"] = [schema]
    source = SourceConfig(
        id=f"snowflake:{account}",
        kind="snowflake",
        params=params,
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Snowflake account [bold]{account}[/bold]")


@app.command("dbt")
def connect_dbt(
    manifest: Annotated[str | None, typer.Option("--manifest", "-m", help="Path to manifest.json.")] = None,
    project_dir: Annotated[
        str | None,
        typer.Option("--project-dir", help="Path to dbt project directory (looks for target/manifest.json)."),
    ] = None,
    project: Annotated[str | None, typer.Option("--project", help="dbt project name override.")] = None,
) -> None:
    """Register a dbt project source."""
    if project_dir is not None:
        project_path = Path(project_dir).resolve()
        if manifest is not None:
            manifest_path = str(Path(manifest).resolve())
        else:
            target_manifest = project_path / "target" / "manifest.json"
            root_manifest = project_path / "manifest.json"
            if target_manifest.exists():
                manifest_path = str(target_manifest)
            elif root_manifest.exists():
                manifest_path = str(root_manifest)
            else:
                rprint(f"[red]Error:[/red] No manifest.json found in {project_dir}/target/ or {project_dir}/")
                raise typer.Exit(1)
    elif manifest is not None:
        manifest_path = str(Path(manifest).resolve())
    else:
        rprint("[red]Error:[/red] Provide --manifest or --project-dir")
        raise typer.Exit(1)

    detected_project = project or _read_dbt_project_name(manifest_path)
    if detected_project is None:
        detected_project = Path(manifest_path).resolve().parent.parent.name

    project_dir_path = Path(manifest_path).resolve().parent
    catalog_path = project_dir_path / "catalog.json"
    run_results_path = project_dir_path / "run_results.json"

    cfg = get_config()
    source = SourceConfig(
        id=f"dbt:{detected_project}",
        kind="dbt",
        params={
            "manifest_path": manifest_path,
            **({"catalog_path": str(catalog_path)} if catalog_path.exists() else {}),
            **({"run_results_path": str(run_results_path)} if run_results_path.exists() else {}),
            "project_name": detected_project,
        },
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] dbt project from [bold]{manifest_path}[/bold]")


@app.command("airflow")
def connect_airflow(
    base_url: Annotated[str, typer.Option("--base-url", help="Airflow base URL.")],
    auth_token_env: Annotated[
        str | None,
        typer.Option("--auth-token-env", help="Env var containing the Airflow auth token."),
    ] = "AIRFLOW_AUTH_TOKEN",
    username: Annotated[str | None, typer.Option("--username")] = None,
    password: Annotated[str | None, typer.Option("--password", hide_input=True)] = None,
) -> None:
    """Register an Apache Airflow source."""
    cfg = get_config()
    params: dict[str, object] = {"base_url": base_url}
    if auth_token_env:
        params["auth_token_env"] = auth_token_env
    if username is not None:
        params["username"] = username
    if password is not None:
        params["password"] = password
    source = SourceConfig(
        id=f"airflow:{_slug_from_url(base_url)}",
        kind="airflow",
        params=params,
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Airflow instance [bold]{base_url}[/bold]")


@app.command("looker")
def connect_looker(
    instance_url: Annotated[str, typer.Option("--instance-url", help="Looker instance URL.")],
    client_id_env: Annotated[
        str,
        typer.Option("--client-id-env", help="Env var containing the Looker client ID."),
    ] = "LOOKER_CLIENT_ID",
    client_secret_env: Annotated[
        str,
        typer.Option("--client-secret-env", help="Env var containing the Looker client secret."),
    ] = "LOOKER_CLIENT_SECRET",
    port: Annotated[int, typer.Option("--port", help="Looker API port.")] = 19999,
) -> None:
    """Register a Looker source."""
    cfg = get_config()
    source = SourceConfig(
        id=f"looker:{_slug_from_url(instance_url)}",
        kind="looker",
        params={
            "instance_url": instance_url,
            "client_id_env": client_id_env,
            "client_secret_env": client_secret_env,
            "port": port,
        },
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Looker instance [bold]{instance_url}[/bold]")


@app.command("fivetran")
def connect_fivetran(
    api_key_env: Annotated[
        str,
        typer.Option("--api-key-env", help="Env var containing the Fivetran API key."),
    ] = "FIVETRAN_API_KEY",
    api_secret_env: Annotated[
        str,
        typer.Option("--api-secret-env", help="Env var containing the Fivetran API secret."),
    ] = "FIVETRAN_API_SECRET",
) -> None:
    """Register a Fivetran source."""
    cfg = get_config()
    source = SourceConfig(
        id="fivetran:default",
        kind="fivetran",
        params={
            "api_key_env": api_key_env,
            "api_secret_env": api_secret_env,
        },
    )
    cfg.add_source(source)
    rprint("[green]Connected:[/green] Fivetran account")


@app.command("metabase")
def connect_metabase(
    instance_url: Annotated[str, typer.Option("--instance-url", help="Metabase instance URL.")],
    api_key_env: Annotated[
        str | None,
        typer.Option("--api-key-env", help="Env var containing the Metabase API key."),
    ] = None,
    username: Annotated[str | None, typer.Option("--username")] = None,
    password: Annotated[str | None, typer.Option("--password", hide_input=True)] = None,
) -> None:
    """Register a Metabase source."""
    if api_key_env is None and (username is None or password is None):
        rprint(
            "[red]Error:[/red] Provide either --api-key-env or both --username and --password."
        )
        raise typer.Exit(1)

    cfg = get_config()
    params: dict[str, object] = {"instance_url": instance_url}
    if api_key_env is not None:
        params["api_key_env"] = api_key_env
    if username is not None:
        params["username"] = username
    if password is not None:
        params["password"] = password
    source = SourceConfig(
        id=f"metabase:{_slug_from_url(instance_url)}",
        kind="metabase",
        params=params,
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Metabase instance [bold]{instance_url}[/bold]")


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
