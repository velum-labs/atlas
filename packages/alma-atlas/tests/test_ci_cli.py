"""CLI tests for Atlas machine-readable CI workflows."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from alma_atlas.cli.main import app
from alma_atlas.config import AtlasConfig
from alma_atlas.pipeline.scan import ScanAllResult, ScanResult
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot

runner = CliRunner()


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    with Database(db_path):
        pass
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)


def test_scan_json_output_writes_machine_readable_payload(tmp_path: Path) -> None:
    output_path = tmp_path / "scan-results.json"
    with patch(
        "alma_atlas.pipeline.scan.run_scan_all",
        return_value=ScanAllResult(
            results=[ScanResult(source_id="pg:warehouse", asset_count=3, edge_count=2)],
            cross_system_edge_count=1,
        ),
    ):
        result = runner.invoke(
            app,
            [
                "scan",
                "--format",
                "json",
                "--output",
                str(output_path),
                "--connections",
                '[{"id":"pg:warehouse","kind":"postgres","params":{"dsn_env":"PG_DATABASE_URL"}}]',
            ],
        )

    assert result.exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert payload["source_count"] == 1
    assert payload["results"][0]["source_id"] == "pg:warehouse"


def test_enforce_validate_warn_mode_does_not_fail_process(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: contract.orders
asset_id: pg:warehouse::public.orders
columns:
  - name: id
    type: INTEGER
    nullable: false
""".strip(),
        encoding="utf-8",
    )

    with patch("alma_atlas.cli.enforce.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "enforce",
                "validate",
                "--contracts",
                str(contract_file),
                "--mode",
                "warn",
            ],
        )

    assert result.exit_code == 0
    assert "FAIL" in result.output


def test_enforce_validate_enforce_mode_returns_non_zero_on_failures(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: contract.orders
asset_id: pg:warehouse::public.orders
columns:
  - name: id
    type: INTEGER
    nullable: false
""".strip(),
        encoding="utf-8",
    )

    with patch("alma_atlas.cli.enforce.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "enforce",
                "validate",
                "--contracts",
                str(contract_file),
                "--mode",
                "enforce",
                "--format",
                "json",
            ],
        )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "failed"
    assert payload["blocked"] == 1


def test_enforce_validate_passes_with_matching_snapshot(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: contract.orders
asset_id: pg:warehouse::public.orders
columns:
  - name: id
    type: INTEGER
    nullable: false
""".strip(),
        encoding="utf-8",
    )

    with Database(cfg.db_path) as db:
        AssetRepository(db).upsert(
            Asset(
                id="pg:warehouse::public.orders",
                source="pg:warehouse",
                kind="table",
                name="public.orders",
            )
        )
        SchemaRepository(db).upsert(
            SchemaSnapshot(
                asset_id="pg:warehouse::public.orders",
                columns=[ColumnInfo(name="id", type="INTEGER", nullable=False)],
            )
        )

    with patch("alma_atlas.cli.enforce.get_config", return_value=cfg):
        result = runner.invoke(
            app,
            [
                "enforce",
                "validate",
                "--contracts",
                str(contract_file),
                "--mode",
                "enforce",
                "--format",
                "json",
            ],
        )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "passed"
    assert payload["passed"] == 1
