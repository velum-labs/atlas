"""CLI tests for Atlas analysis execution."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from alma_atlas.cli.main import app
from alma_atlas.config import AtlasConfig
from alma_atlas.testing.analysis_seed import seed_analysis_data

runner = CliRunner()


def test_analysis_run_outputs_clusters_and_candidates(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    seed_analysis_data(cfg.db_path)

    with patch("alma_atlas.cli.analysis.get_config", return_value=cfg):
        result = runner.invoke(app, ["analysis", "run", "--source", "postgres:demo"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["parsed_query_count"] == 2
    assert payload["cluster_count"] >= 1
    assert payload["candidate_count"] >= 1


def _cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
