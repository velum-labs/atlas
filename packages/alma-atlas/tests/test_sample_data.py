"""Tests for the bundled sample-data snapshot and `alma-atlas sample` CLI.

Verifies:
- The bundled snapshot exists in the wheel and is reasonably small.
- install_sample() decompresses to a valid SQLite db Atlas can read.
- The installed sample contains the assets, edges, schemas, annotations the
  generator wrote (smoke).
- The sample snapshot supports the existing MCP tool surface end-to-end (so a
  `pip install alma-atlas && alma-atlas sample install` flow really gives
  Cursor / Claude Desktop a working data stack).
- CLI install respects --target, --force, and the FileExistsError default.
"""

from __future__ import annotations

import gzip
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from alma_atlas import sample_data
from alma_atlas.cli import sample as sample_cli
from alma_atlas.config import AtlasConfig
from alma_atlas.mcp.tools_companion import (
    _handle_companion_explain_lineage_and_contract,
    _handle_companion_get_schema_and_owner,
    _handle_companion_search,
)
from alma_atlas.mcp.tools_lineage import _handle_lineage
from alma_atlas.mcp.tools_search import _handle_search
from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.db import Database

runner = CliRunner()


# ---------------------------------------------------------------------------
# Bundled snapshot integrity
# ---------------------------------------------------------------------------


def test_bundled_snapshot_exists_in_wheel() -> None:
    path = sample_data.bundled_snapshot_path()
    assert path.exists(), (
        f"Bundled sample snapshot missing at {path}. "
        "Did the wheel build skip src/alma_atlas/data/?"
    )


def test_bundled_snapshot_under_size_budget() -> None:
    """Design doc spec: <10 MB compressed."""
    path = sample_data.bundled_snapshot_path()
    size_mb = path.stat().st_size / (1024 * 1024)
    assert size_mb < 10, f"Snapshot is {size_mb:.2f} MB; budget is 10 MB."


def test_bundled_snapshot_is_valid_gzip() -> None:
    path = sample_data.bundled_snapshot_path()
    with gzip.open(path, "rb") as f:
        head = f.read(16)
    assert head.startswith(b"SQLite format 3"), (
        "Decompressed snapshot does not look like a SQLite database."
    )


# ---------------------------------------------------------------------------
# install_sample() decompression
# ---------------------------------------------------------------------------


def test_install_sample_writes_valid_sqlite(tmp_path: Path) -> None:
    target = tmp_path / "atlas.db"
    written = sample_data.install_sample(target)
    assert written == target
    assert target.exists()
    # First bytes should be the SQLite magic
    assert target.read_bytes()[:16].startswith(b"SQLite format 3")


def test_install_sample_creates_parent_dir(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "dirs" / "atlas.db"
    sample_data.install_sample(target)
    assert target.exists()


def test_install_sample_refuses_overwrite_by_default(tmp_path: Path) -> None:
    target = tmp_path / "atlas.db"
    sample_data.install_sample(target)
    with pytest.raises(FileExistsError):
        sample_data.install_sample(target)


def test_install_sample_overwrites_with_overwrite_true(tmp_path: Path) -> None:
    target = tmp_path / "atlas.db"
    sample_data.install_sample(target)
    target.write_bytes(b"clobbered")  # corrupt it
    sample_data.install_sample(target, overwrite=True)
    # Restored to a real SQLite db
    assert target.read_bytes()[:16].startswith(b"SQLite format 3")


def test_install_sample_raises_on_missing_bundle(tmp_path: Path) -> None:
    fake_missing = tmp_path / "nonexistent.db.gz"
    with patch("alma_atlas.sample_data._BUNDLED_DATA_PATH", fake_missing), pytest.raises(FileNotFoundError):
        sample_data.install_sample(tmp_path / "atlas.db")


# ---------------------------------------------------------------------------
# Snapshot content (smoke — what the generator put in)
# ---------------------------------------------------------------------------


def _installed_snapshot(tmp_path: Path) -> Path:
    target = tmp_path / "atlas.db"
    sample_data.install_sample(target)
    return target


def test_installed_snapshot_has_three_sources(tmp_path: Path) -> None:
    target = _installed_snapshot(tmp_path)
    with Database(target) as db:
        assets = AssetRepository(db).list_all()
    sources = {a.source for a in assets}
    # Generator ships Snowflake + dbt + Looker
    assert "snowflake:demo" in sources
    assert "dbt:demo" in sources
    assert "looker:demo" in sources


def test_installed_snapshot_has_realistic_asset_count(tmp_path: Path) -> None:
    target = _installed_snapshot(tmp_path)
    with Database(target) as db:
        assets = AssetRepository(db).list_all()
    # Generator ships 5 + 5 + 5 = 15 assets
    assert len(assets) == 15


def test_installed_snapshot_supports_atlas_search(tmp_path: Path) -> None:
    target = _installed_snapshot(tmp_path)
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=target)
    result = _handle_search(cfg, {"query": "orders"})
    assert "analytics.orders" in result[0].text or "stg_orders" in result[0].text


def test_installed_snapshot_supports_atlas_lineage(tmp_path: Path) -> None:
    """The cross-system lineage chain must walk from Snowflake -> dbt -> Looker."""
    target = _installed_snapshot(tmp_path)
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=target)
    # analytics.orders -> stg_orders -> fct_revenue -> explore.revenue -> dashboards
    result = _handle_lineage(
        cfg,
        {"asset_id": "snowflake:demo::analytics.orders", "direction": "downstream"},
    )
    text = result[0].text
    assert "stg_orders" in text
    assert "fct_revenue" in text


def test_installed_snapshot_supports_companion_search(tmp_path: Path) -> None:
    target = _installed_snapshot(tmp_path)
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=target)
    result = _handle_companion_search(cfg, {"query": "users"})
    text = result[0].text
    # Search hits across Snowflake users + dbt stg_users + dbt dim_users
    assert "users" in text


def test_installed_snapshot_supports_companion_get_schema_and_owner(tmp_path: Path) -> None:
    target = _installed_snapshot(tmp_path)
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=target)
    result = _handle_companion_get_schema_and_owner(
        cfg, {"asset_id": "snowflake:demo::analytics.payments"}
    )
    text = result[0].text
    assert "analytics.payments" in text
    # Generator annotates payments with finance ops as owner
    assert "finance-ops@example.com" in text
    # Schema columns we put in
    assert "payment_id" in text


def test_installed_snapshot_supports_companion_explain_lineage(tmp_path: Path) -> None:
    target = _installed_snapshot(tmp_path)
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=target)
    result = _handle_companion_explain_lineage_and_contract(
        cfg, {"asset_id": "dbt:demo::marts.fct_revenue"}
    )
    text = result[0].text
    # Upstream + downstream: payments + stg_orders -> fct_revenue -> explore.revenue + fct_user_ltv
    assert "payments" in text or "stg_orders" in text
    assert "explore.revenue" in text or "fct_user_ltv" in text


# ---------------------------------------------------------------------------
# CLI: alma-atlas sample install / preview
# ---------------------------------------------------------------------------


def test_cli_install_writes_sample_to_target(tmp_path: Path) -> None:
    target = tmp_path / "atlas.db"
    result = runner.invoke(sample_cli.app, ["install", "--target", str(target)])
    assert result.exit_code == 0, result.stdout
    assert "installed" in result.stdout.lower()
    assert target.exists()


def test_cli_install_refuses_overwrite_without_force(tmp_path: Path) -> None:
    target = tmp_path / "atlas.db"
    sample_data.install_sample(target)  # pre-existing
    result = runner.invoke(sample_cli.app, ["install", "--target", str(target)])
    assert result.exit_code == 1
    assert "already exists" in result.stdout


def test_cli_install_force_overwrites(tmp_path: Path) -> None:
    target = tmp_path / "atlas.db"
    sample_data.install_sample(target)
    target.write_bytes(b"clobbered")
    result = runner.invoke(sample_cli.app, ["install", "--target", str(target), "--force"])
    assert result.exit_code == 0
    assert target.read_bytes()[:16].startswith(b"SQLite format 3")


def test_cli_install_default_target_is_home_alma_atlas(tmp_path: Path) -> None:
    """Without --target, install lands at ~/.alma-atlas/atlas.db."""
    fake_home = tmp_path
    with patch("alma_atlas.cli.sample.Path.home", return_value=fake_home):
        result = runner.invoke(sample_cli.app, ["install"])
    assert result.exit_code == 0
    assert (fake_home / ".alma-atlas" / "atlas.db").exists()


def test_cli_preview_lists_sources_and_assets() -> None:
    result = runner.invoke(sample_cli.app, ["preview"])
    assert result.exit_code == 0, result.stdout
    assert "snowflake:demo" in result.stdout
    assert "dbt:demo" in result.stdout
    assert "looker:demo" in result.stdout
    assert "Assets:" in result.stdout
