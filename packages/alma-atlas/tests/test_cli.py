"""Tests for alma_atlas CLI commands."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from alma_atlas.cli.main import app
from alma_atlas.config import AtlasConfig, SourceConfig

runner = CliRunner()


def _cfg(tmp_path: Path) -> AtlasConfig:
    return AtlasConfig(config_dir=tmp_path / "alma")


def _create_sqlite_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    connection.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    connection.commit()
    connection.close()


# ---------------------------------------------------------------------------
# --version flag
# ---------------------------------------------------------------------------


def test_version_flag() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "alma-atlas" in result.output


# ---------------------------------------------------------------------------
# connect bigquery
# ---------------------------------------------------------------------------


def test_connect_bigquery(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "bigquery", "--project", "my-project"])
    assert result.exit_code == 0
    assert "Application Default Credentials" in result.output
    sources = cfg.load_sources()
    assert len(sources) == 1
    assert sources[0].kind == "bigquery"
    assert sources[0].params["project_id"] == "my-project"
    assert "credentials" not in sources[0].params
    assert "service_account_env" not in sources[0].params


def test_connect_bigquery_with_credentials(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app, ["connect", "bigquery", "--project", "proj", "--credentials", "/path/to/creds.json"]
        )
    assert result.exit_code == 0
    assert "explicit credentials" in result.output
    sources = cfg.load_sources()
    assert sources[0].params.get("credentials") == "/path/to/creds.json"


def test_connect_bigquery_with_service_account_env(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "connect",
                "bigquery",
                "--project",
                "proj",
                "--service-account-env",
                "BQ_SERVICE_ACCOUNT_JSON",
            ],
        )
    assert result.exit_code == 0
    assert "explicit credentials" in result.output
    sources = cfg.load_sources()
    assert sources[0].params.get("service_account_env") == "BQ_SERVICE_ACCOUNT_JSON"


# ---------------------------------------------------------------------------
# connect postgres
# ---------------------------------------------------------------------------


def test_connect_postgres(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "postgres", "--dsn", "postgresql://user:pass@localhost/mydb"])
    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].id == "postgres:mydb"
    assert sources[0].kind == "postgres"
    assert sources[0].params["include_schemas"] == ["public"]


# ---------------------------------------------------------------------------
# connect sqlite
# ---------------------------------------------------------------------------


def test_connect_sqlite(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    db_path = tmp_path / "sample.sqlite"
    _create_sqlite_db(db_path)

    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "sqlite", "--path", str(db_path)])

    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].id == "sqlite:sample"
    assert sources[0].kind == "sqlite"
    assert sources[0].params["path"] == str(db_path.resolve())


def test_connect_sqlite_with_custom_id(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    db_path = tmp_path / "sample.sqlite"
    _create_sqlite_db(db_path)

    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            ["connect", "sqlite", "--path", str(db_path), "--id", "sqlite:custom"],
        )

    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].id == "sqlite:custom"


def test_connect_sqlite_directory_mode(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    database_dir = tmp_path / "databases"
    alpha_path = database_dir / "alpha" / "alpha.sqlite"
    beta_path = database_dir / "beta" / "beta.sqlite"
    _create_sqlite_db(alpha_path)
    _create_sqlite_db(beta_path)

    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            ["connect", "sqlite", "--dir", str(database_dir), "--glob", "*.sqlite"],
        )

    assert result.exit_code == 0
    assert "2 SQLite database(s)" in result.output
    sources = sorted(cfg.load_sources(), key=lambda source: source.id)
    assert [source.id for source in sources] == ["sqlite:alpha", "sqlite:beta"]
    assert [source.params["path"] for source in sources] == [
        str(alpha_path.resolve()),
        str(beta_path.resolve()),
    ]


# ---------------------------------------------------------------------------
# connect dbt
# ---------------------------------------------------------------------------


def test_connect_dbt(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    manifest = str(tmp_path / "manifest.json")
    Path(manifest).write_text('{"metadata":{"project_name":"analytics"}}')
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "dbt", "--manifest", manifest])
    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].kind == "dbt"
    assert sources[0].params["manifest_path"] == manifest
    assert sources[0].id == "dbt:analytics"
    assert sources[0].params["project_name"] == "analytics"


def test_connect_snowflake(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "connect",
                "snowflake",
                "--account",
                "xy12345.us-east-1",
                "--account-secret-env",
                "SNOWFLAKE_CONNECTION_JSON",
                "--role",
                "ANALYST",
                "--schema",
                "ANALYTICS",
            ],
        )
    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert sources[0].kind == "snowflake"
    assert sources[0].params["account_secret_env"] == "SNOWFLAKE_CONNECTION_JSON"
    assert sources[0].params["role"] == "ANALYST"
    assert sources[0].params["include_schemas"] == ["ANALYTICS"]


def test_connect_airflow(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            ["connect", "airflow", "--base-url", "https://airflow.example.com", "--auth-token-env", "AIRFLOW_AUTH_TOKEN"],
        )
    assert result.exit_code == 0
    source = cfg.load_sources()[0]
    assert source.kind == "airflow"
    assert source.params["base_url"] == "https://airflow.example.com"
    assert source.params["auth_token_env"] == "AIRFLOW_AUTH_TOKEN"


def test_connect_looker(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "connect",
                "looker",
                "--instance-url",
                "https://looker.example.com",
                "--client-id-env",
                "LOOKER_CLIENT_ID",
                "--client-secret-env",
                "LOOKER_CLIENT_SECRET",
            ],
        )
    assert result.exit_code == 0
    source = cfg.load_sources()[0]
    assert source.kind == "looker"
    assert source.params["instance_url"] == "https://looker.example.com"


def test_connect_fivetran(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "connect",
                "fivetran",
                "--api-key-env",
                "FIVETRAN_API_KEY",
                "--api-secret-env",
                "FIVETRAN_API_SECRET",
            ],
        )
    assert result.exit_code == 0
    source = cfg.load_sources()[0]
    assert source.kind == "fivetran"
    assert source.params["api_key_env"] == "FIVETRAN_API_KEY"


def test_connect_metabase(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            ["connect", "metabase", "--instance-url", "https://metabase.example.com", "--api-key-env", "METABASE_API_KEY"],
        )
    assert result.exit_code == 0
    source = cfg.load_sources()[0]
    assert source.kind == "metabase"
    assert source.params["api_key_env"] == "METABASE_API_KEY"


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# connect github
# ---------------------------------------------------------------------------


def test_connect_github_app(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "connect",
                "github",
                "--app-id",
                "123",
                "--installation-id",
                "456",
                "--private-key-env",
                "GITHUB_APP_PK",
                "--repo",
                "velum-labs/atlas",
            ],
        )

    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert len(sources) == 1
    assert sources[0].kind == "github"
    assert sources[0].params["app_id"] == "123"
    assert sources[0].params["installation_id"] == "456"
    assert sources[0].params["private_key_env"] == "GITHUB_APP_PK"
    assert sources[0].params["repos"] == ["velum-labs/atlas"]


def test_connect_github_token(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "connect",
                "github",
                "--token-env",
                "GITHUB_TOKEN",
                "--repo",
                "velum-labs/atlas",
            ],
        )

    assert result.exit_code == 0
    sources = cfg.load_sources()
    assert len(sources) == 1
    assert sources[0].kind == "github"
    assert sources[0].params["token_env"] == "GITHUB_TOKEN"
    assert sources[0].params["repos"] == ["velum-labs/atlas"]


# connect list
# ---------------------------------------------------------------------------


def test_connect_list_no_sources(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "list"])
    assert result.exit_code == 0
    assert "No sources" in result.output


def test_connect_list_with_sources(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "list"])
    assert result.exit_code == 0
    assert "pg:mydb" in result.output


# ---------------------------------------------------------------------------
# connect remove
# ---------------------------------------------------------------------------


def test_connect_remove_existing(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "remove", "pg:mydb"])
    assert result.exit_code == 0
    assert cfg.load_sources() == []


def test_connect_remove_nonexistent(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.connect.get_config", return_value=cfg):
        result = runner.invoke(app, ["connect", "remove", "nonexistent"])
    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# scan — no sources
# ---------------------------------------------------------------------------


def test_scan_no_sources(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    with patch("alma_atlas.cli.scan.get_config", return_value=cfg):
        result = runner.invoke(app, ["scan"])
    assert result.exit_code == 1
    assert "No sources" in result.output


def test_scan_dry_run(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.scan.get_config", return_value=cfg):
        result = runner.invoke(app, ["scan", "--dry-run"])
    assert result.exit_code == 0
    assert "Dry run" in result.output
    assert "pg:mydb" in result.output


def test_scan_source_not_found(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.add_source(SourceConfig(id="pg:mydb", kind="postgres", params={}))
    with patch("alma_atlas.cli.scan.get_config", return_value=cfg):
        result = runner.invoke(app, ["scan", "--source", "nonexistent"])
    assert result.exit_code == 1


def test_scan_uses_runtime_sources_without_persisting_them(tmp_path: Path) -> None:
    from alma_atlas.pipeline.scan import ScanAllResult, ScanResult

    cfg = AtlasConfig(
        config_dir=tmp_path / "alma",
        sources=[SourceConfig(id="runtime", kind="postgres", params={"dsn_env": "PG_URL"})],
    )
    mock_result = ScanAllResult(results=[ScanResult(source_id="runtime")], cross_system_edge_count=0)

    with (
        patch("alma_atlas.cli.scan.get_config", return_value=cfg),
        patch("alma_atlas.pipeline.scan.run_scan_all", return_value=mock_result),
        patch.object(cfg, "save_sources") as mock_save_sources,
    ):
        result = runner.invoke(app, ["scan", "--no-sync"])

    assert result.exit_code == 0
    mock_save_sources.assert_not_called()


# ---------------------------------------------------------------------------
# learn --source
# ---------------------------------------------------------------------------


def _seed_learn_assets(cfg, assets: list[tuple[str, str]]) -> None:
    """Create the atlas DB and seed it with (asset_id, source) pairs."""
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_ports.asset import Asset

    db_path = cfg.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with Database(db_path) as db:
        repo = AssetRepository(db)
        for asset_id, source in assets:
            repo.upsert(Asset(id=asset_id, source=source, kind="table", name=asset_id))


def test_learn_assets_source_filter_dry_run(tmp_path: Path) -> None:
    """learn --assets --source <id> --dry-run shows only assets from that source."""
    cfg = _cfg(tmp_path)
    _seed_learn_assets(cfg, [
        ("pg:henkel::analytics.revenue", "pg:henkel"),
        ("sqlite:benchmark::frpm", "sqlite:benchmark"),
    ])

    with patch("alma_atlas.cli.learn.get_config", return_value=cfg):
        result = runner.invoke(app, ["learn", "--assets", "--source", "pg:henkel", "--dry-run"])

    assert result.exit_code == 0
    assert "pg:henkel::analytics.revenue" in result.output
    assert "sqlite:benchmark::frpm" not in result.output


def test_learn_assets_no_source_filter_dry_run(tmp_path: Path) -> None:
    """learn --assets --dry-run without --source shows all unannotated assets."""
    cfg = _cfg(tmp_path)
    _seed_learn_assets(cfg, [
        ("pg:henkel::analytics.revenue", "pg:henkel"),
        ("sqlite:benchmark::frpm", "sqlite:benchmark"),
    ])

    with patch("alma_atlas.cli.learn.get_config", return_value=cfg):
        result = runner.invoke(app, ["learn", "--assets", "--dry-run"])

    assert result.exit_code == 0
    assert "pg:henkel::analytics.revenue" in result.output
    assert "sqlite:benchmark::frpm" in result.output
