"""Tests for alma_atlas.config — AtlasConfig and SourceConfig."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from alma_atlas.config import AtlasConfig, SourceConfig, default_config_dir, get_config

# ---------------------------------------------------------------------------
# default_config_dir
# ---------------------------------------------------------------------------


def test_default_config_dir_uses_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ALMA_CONFIG_DIR", str(tmp_path))
    assert default_config_dir() == tmp_path


def test_default_config_dir_falls_back_to_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALMA_CONFIG_DIR", raising=False)
    result = default_config_dir()
    assert result == Path.home() / ".alma"


# ---------------------------------------------------------------------------
# AtlasConfig — construction
# ---------------------------------------------------------------------------


def test_db_path_defaults_to_config_dir(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    assert cfg.db_path == tmp_path / "atlas.db"


def test_db_path_can_be_overridden(tmp_path: Path) -> None:
    custom = tmp_path / "custom.db"
    cfg = AtlasConfig(config_dir=tmp_path, db_path=custom)
    assert cfg.db_path == custom


def test_sources_file_property(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    assert cfg.sources_file == tmp_path / "sources.json"


# ---------------------------------------------------------------------------
# AtlasConfig — ensure_dir
# ---------------------------------------------------------------------------


def test_ensure_dir_creates_directory(tmp_path: Path) -> None:
    config_dir = tmp_path / "nested" / "alma"
    cfg = AtlasConfig(config_dir=config_dir)
    assert not config_dir.exists()
    cfg.ensure_dir()
    assert config_dir.is_dir()


def test_ensure_dir_is_idempotent(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    cfg.ensure_dir()
    cfg.ensure_dir()  # should not raise


# ---------------------------------------------------------------------------
# AtlasConfig — load_sources / save_sources
# ---------------------------------------------------------------------------


def test_load_sources_returns_empty_when_file_missing(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    assert cfg.load_sources() == []


def test_save_and_load_sources_roundtrip(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    sources = [
        SourceConfig(id="pg:mydb", kind="postgres", params={"dsn": "postgresql://localhost/mydb"}),
        SourceConfig(id="bq:proj", kind="bigquery", params={"project_id": "proj"}),
    ]
    cfg.save_sources(sources)
    loaded = cfg.load_sources()
    assert len(loaded) == 2
    assert loaded[0].id == "pg:mydb"
    assert loaded[0].kind == "postgres"
    assert loaded[1].id == "bq:proj"


def test_save_sources_creates_valid_json(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    cfg.save_sources([SourceConfig(id="x", kind="postgres", params={"dsn": "pg://localhost/x"})])
    raw = json.loads(cfg.sources_file.read_text())
    assert isinstance(raw, list)
    assert raw[0]["id"] == "x"


def test_save_sources_encrypts_literal_secrets(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    secret_dsn = "postgresql://user:secret@localhost/db"
    cfg.save_sources([SourceConfig(id="x", kind="postgres", params={"dsn": secret_dsn})])

    raw = json.loads(cfg.sources_file.read_text())
    assert secret_dsn not in cfg.sources_file.read_text()
    assert raw[0]["params"]["dsn"]["__alma_secret_id__"] == "source.x.dsn"

    loaded = cfg.load_sources()
    assert loaded[0].params["dsn"] == secret_dsn


# ---------------------------------------------------------------------------
# AtlasConfig — add_source
# ---------------------------------------------------------------------------


def test_add_source_appends(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    cfg.add_source(SourceConfig(id="src1", kind="postgres", params={}))
    cfg.add_source(SourceConfig(id="src2", kind="bigquery", params={}))
    sources = cfg.load_sources()
    assert len(sources) == 2


def test_add_source_updates_existing(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    cfg.add_source(SourceConfig(id="src1", kind="postgres", params={"dsn": "old"}))
    cfg.add_source(SourceConfig(id="src1", kind="postgres", params={"dsn": "new"}))
    sources = cfg.load_sources()
    assert len(sources) == 1
    assert sources[0].params["dsn"] == "new"


# ---------------------------------------------------------------------------
# AtlasConfig — remove_source
# ---------------------------------------------------------------------------


def test_remove_source_returns_true_when_found(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    cfg.add_source(SourceConfig(id="src1", kind="postgres", params={}))
    assert cfg.remove_source("src1") is True
    assert cfg.load_sources() == []


def test_remove_source_returns_false_when_not_found(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    assert cfg.remove_source("nonexistent") is False


def test_remove_source_only_removes_target(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    cfg.add_source(SourceConfig(id="keep", kind="postgres", params={}))
    cfg.add_source(SourceConfig(id="drop", kind="bigquery", params={}))
    cfg.remove_source("drop")
    sources = cfg.load_sources()
    assert len(sources) == 1
    assert sources[0].id == "keep"


def test_team_config_encrypts_api_key(tmp_path: Path) -> None:
    cfg = AtlasConfig(config_dir=tmp_path)
    cfg.team_server_url = "https://atlas.example.com"
    cfg.team_api_key = "super-secret-team-key"
    cfg.team_id = "default"

    cfg.save_team_config()
    raw = cfg.config_file.read_text()
    assert "super-secret-team-key" not in raw

    cfg.team_server_url = None
    cfg.team_api_key = None
    cfg.team_id = None
    cfg.load_team_config()
    assert cfg.team_server_url == "https://atlas.example.com"
    assert cfg.team_api_key == "super-secret-team-key"
    assert cfg.team_id == "default"


# ---------------------------------------------------------------------------
# get_config singleton
# ---------------------------------------------------------------------------


def test_get_config_returns_singleton(monkeypatch: pytest.MonkeyPatch) -> None:
    import alma_atlas.config as config_module

    monkeypatch.setattr(config_module, "_config", None)
    c1 = get_config()
    c2 = get_config()
    assert c1 is c2
    # cleanup
    monkeypatch.setattr(config_module, "_config", None)
