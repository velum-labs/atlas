"""CLI tests for Atlas analysis snapshot commands."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from alma_atlas.cli.main import app
from alma_atlas.config import AtlasConfig
from alma_atlas.testing.analysis_seed import seed_analysis_data

runner = CliRunner()


def test_analysis_export_writes_snapshot_json(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    seed_analysis_data(cfg.db_path)
    output = tmp_path / "analysis-snapshot.json"

    with patch("alma_atlas.cli.analysis.get_config", return_value=cfg):
        result = runner.invoke(app, ["analysis", "export", "--output", str(output)])

    assert result.exit_code == 0
    payload = json.loads(output.read_text())
    assert payload["snapshot_version"] == "1"
    assert payload["traffic_summary"]["query_fingerprint_count"] == 3
    assert len(payload["graph"]["queries"]) == 3


def test_analysis_summary_prints_json(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    seed_analysis_data(cfg.db_path)

    with patch("alma_atlas.cli.analysis.get_config", return_value=cfg):
        result = runner.invoke(app, ["analysis", "summary", "--source", "postgres:demo"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["query_fingerprint_count"] == 2
    assert payload["total_query_executions"] == 3
    assert payload["asset_count"] == 3


def _cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
