"""Tests for machine-readable Atlas CI helpers."""

from __future__ import annotations

from pathlib import Path

from alma_atlas.ci_support import (
    render_contract_summary_markdown,
    resolve_runtime_sources,
    serialize_scan_result,
    validate_contracts,
)
from alma_atlas.config import AtlasConfig
from alma_atlas.pipeline.scan import ScanAllResult, ScanResult
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    with Database(db_path):
        pass
    return AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)


def test_resolve_runtime_sources_accepts_mapping_connections() -> None:
    cfg, sources = resolve_runtime_sources(
        connections='{"pg:warehouse":{"kind":"postgres","dsn_env":"PG_DATABASE_URL"}}'
    )

    assert cfg is not None
    assert len(sources) == 1
    assert sources[0].id == "pg:warehouse"
    assert sources[0].kind == "postgres"
    assert sources[0].params["dsn_env"] == "PG_DATABASE_URL"


def test_resolve_runtime_sources_supports_atlas_yml(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ALMA_CONFIG_DIR", str(tmp_path / "alma-runtime"))
    atlas_yml = tmp_path / "atlas.yml"
    atlas_yml.write_text(
        """
version: 1
sources:
  - id: pg:warehouse
    kind: postgres
    params:
      dsn_env: PG_DATABASE_URL
""".strip(),
        encoding="utf-8",
    )

    cfg, sources = resolve_runtime_sources(config_file=str(atlas_yml))

    assert cfg.config_dir == tmp_path / "alma-runtime"
    assert len(sources) == 1
    assert sources[0].id == "pg:warehouse"


def test_resolve_runtime_sources_prefers_auto_discovered_runtime_sources(tmp_path: Path, monkeypatch) -> None:
    runtime_dir = tmp_path / "alma-runtime"
    runtime_dir.mkdir()
    monkeypatch.setenv("ALMA_CONFIG_DIR", str(runtime_dir))
    (runtime_dir / "atlas.yml").write_text(
        """
version: 1
sources:
  - id: runtime
    kind: postgres
    params:
      dsn_env: PG_DATABASE_URL
""".strip(),
        encoding="utf-8",
    )

    cfg, sources = resolve_runtime_sources()

    assert cfg.config_dir == runtime_dir
    assert [source.id for source in sources] == ["runtime"]


def test_serialize_scan_result_is_machine_readable() -> None:
    payload = serialize_scan_result(
        ScanAllResult(
            results=[
                ScanResult(source_id="pg:warehouse", asset_count=4, edge_count=2),
                ScanResult(source_id="bq:raw", error="connection refused"),
            ],
            cross_system_edge_count=3,
        )
    )

    assert payload["status"] == "failed"
    assert payload["source_count"] == 2
    assert payload["sources_succeeded"] == 1
    assert payload["sources_failed"] == 1
    assert payload["cross_system_edge_count"] == 3


def test_validate_contracts_passes_when_snapshot_matches(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    contract_file = contracts_dir / "orders.yaml"
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

    payload = validate_contracts(
        cfg=cfg,
        contract_patterns=[str(contracts_dir / "*.yaml")],
        mode="warn",
    )

    assert payload["status"] == "passed"
    assert payload["passed"] == 1
    assert payload["failed"] == 0


def test_validate_contracts_marks_invalid_documents_as_failed(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    bad_contract = contracts_dir / "broken.yaml"
    bad_contract.write_text(
        """
columns:
  - name: id
    type: INTEGER
""".strip(),
        encoding="utf-8",
    )

    payload = validate_contracts(
        cfg=cfg,
        contract_patterns=[str(contracts_dir / "*.yaml")],
        mode="enforce",
    )

    assert payload["status"] == "failed"
    assert payload["blocked"] == 1
    detail = payload["details"][0]
    assert detail["status"] == "failed"
    assert detail["blocking"] is True
    assert detail["issues"][0]["code"] == "invalid_contract"


def test_render_contract_summary_markdown_includes_table() -> None:
    markdown = render_contract_summary_markdown(
        {
            "status": "failed",
            "mode": "warn",
            "total": 1,
            "passed": 0,
            "failed": 1,
            "details": [
                {
                    "contract_id": "contract.orders",
                    "asset_id": "pg:warehouse::public.orders",
                    "status": "failed",
                    "issue_count": 2,
                    "blocking": False,
                }
            ],
        }
    )

    assert "Alma Contract CI" in markdown
    assert "| Contract | Asset | Status | Issues | Blocking |" in markdown
    assert "`contract.orders`" in markdown
