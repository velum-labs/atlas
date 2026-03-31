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

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from alma_atlas.config import get_config
from alma_atlas.source_specs import (
    DEFAULT_AIRFLOW_AUTH_TOKEN_ENV,
    DEFAULT_BIGQUERY_LOCATION,
    DEFAULT_FIVETRAN_API_KEY_ENV,
    DEFAULT_FIVETRAN_API_SECRET_ENV,
    DEFAULT_LOOKER_CLIENT_ID_ENV,
    DEFAULT_LOOKER_CLIENT_SECRET_ENV,
    DEFAULT_LOOKER_PORT,
    DEFAULT_POSTGRES_SCHEMA,
    DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV,
    make_airflow_source,
    make_bigquery_source,
    make_dbt_source,
    make_fivetran_source,
    make_looker_source,
    make_metabase_source,
    make_postgres_source,
    make_snowflake_source,
    resolve_dbt_auxiliary_paths,
)

app = typer.Typer(help="Register and manage data source connections.")
console = Console()


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
    location: Annotated[
        str,
        typer.Option("--location", help="BigQuery location / region."),
    ] = DEFAULT_BIGQUERY_LOCATION,
) -> None:
    """Register a Google BigQuery data source."""
    cfg = get_config()
    try:
        source = make_bigquery_source(
            project=project,
            credentials=credentials,
            service_account_env=service_account_env,
            location=location,
        )
    except ValueError as exc:
        rprint(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] BigQuery project [bold]{project}[/bold]")


@app.command("postgres")
def connect_postgres(
    dsn: Annotated[str | None, typer.Option("--dsn", help="PostgreSQL connection string.")] = None,
    dsn_env: Annotated[
        str | None,
        typer.Option("--dsn-env", help="Env var containing the PostgreSQL connection string."),
    ] = None,
    schema: Annotated[str, typer.Option("--schema", help="Schema to scan.")] = DEFAULT_POSTGRES_SCHEMA,
) -> None:
    """Register a PostgreSQL data source."""
    cfg = get_config()
    try:
        source = make_postgres_source(dsn=dsn, dsn_env=dsn_env, schema=schema)
    except ValueError as exc:
        rprint(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    cfg.add_source(source)
    db_locator = dsn or dsn_env or "postgres"
    db_name = db_locator.rsplit("/", 1)[-1].split("?")[0]
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
    ] = DEFAULT_SNOWFLAKE_ACCOUNT_SECRET_ENV,
    warehouse: Annotated[str | None, typer.Option("--warehouse")] = None,
    database: Annotated[str | None, typer.Option("--database")] = None,
    role: Annotated[str | None, typer.Option("--role")] = None,
    schema: Annotated[str | None, typer.Option("--schema")] = None,
) -> None:
    """Register a Snowflake data source."""
    cfg = get_config()
    source = make_snowflake_source(
        account=account,
        account_secret_env=account_secret_env,
        warehouse=warehouse,
        database=database,
        role=role,
        schema=schema,
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

    cfg = get_config()
    catalog_path, run_results_path = resolve_dbt_auxiliary_paths(manifest_path)
    source = make_dbt_source(
        manifest_path=manifest_path,
        project_name=detected_project,
        catalog_path=catalog_path,
        run_results_path=run_results_path,
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] dbt project from [bold]{manifest_path}[/bold]")


@app.command("airflow")
def connect_airflow(
    base_url: Annotated[str, typer.Option("--base-url", help="Airflow base URL.")],
    auth_token_env: Annotated[
        str | None,
        typer.Option("--auth-token-env", help="Env var containing the Airflow auth token."),
    ] = DEFAULT_AIRFLOW_AUTH_TOKEN_ENV,
    username: Annotated[str | None, typer.Option("--username")] = None,
    password: Annotated[str | None, typer.Option("--password", hide_input=True)] = None,
    password_env: Annotated[
        str | None,
        typer.Option("--password-env", help="Env var containing the Airflow password."),
    ] = None,
) -> None:
    """Register an Apache Airflow source."""
    cfg = get_config()
    source = make_airflow_source(
        base_url=base_url,
        auth_token_env=auth_token_env,
        username=username,
        password=password,
        password_env=password_env,
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Airflow instance [bold]{base_url}[/bold]")


@app.command("looker")
def connect_looker(
    instance_url: Annotated[str, typer.Option("--instance-url", help="Looker instance URL.")],
    client_id_env: Annotated[
        str,
        typer.Option("--client-id-env", help="Env var containing the Looker client ID."),
    ] = DEFAULT_LOOKER_CLIENT_ID_ENV,
    client_secret_env: Annotated[
        str,
        typer.Option("--client-secret-env", help="Env var containing the Looker client secret."),
    ] = DEFAULT_LOOKER_CLIENT_SECRET_ENV,
    port: Annotated[int, typer.Option("--port", help="Looker API port.")] = DEFAULT_LOOKER_PORT,
) -> None:
    """Register a Looker source."""
    cfg = get_config()
    source = make_looker_source(
        instance_url=instance_url,
        client_id_env=client_id_env,
        client_secret_env=client_secret_env,
        port=port,
    )
    cfg.add_source(source)
    rprint(f"[green]Connected:[/green] Looker instance [bold]{instance_url}[/bold]")


@app.command("fivetran")
def connect_fivetran(
    api_key_env: Annotated[
        str,
        typer.Option("--api-key-env", help="Env var containing the Fivetran API key."),
    ] = DEFAULT_FIVETRAN_API_KEY_ENV,
    api_secret_env: Annotated[
        str,
        typer.Option("--api-secret-env", help="Env var containing the Fivetran API secret."),
    ] = DEFAULT_FIVETRAN_API_SECRET_ENV,
) -> None:
    """Register a Fivetran source."""
    cfg = get_config()
    source = make_fivetran_source(api_key_env=api_key_env, api_secret_env=api_secret_env)
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
    password_env: Annotated[
        str | None,
        typer.Option("--password-env", help="Env var containing the Metabase password."),
    ] = None,
) -> None:
    """Register a Metabase source."""
    cfg = get_config()
    try:
        source = make_metabase_source(
            instance_url=instance_url,
            api_key_env=api_key_env,
            username=username,
            password=password,
            password_env=password_env,
        )
    except ValueError as exc:
        rprint(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
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
