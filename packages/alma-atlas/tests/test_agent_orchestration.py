"""Tests for the Atlas lead/specialist agent orchestration pattern.

Covers:
- AgentConfig and per-agent LearningConfig (defaults + YAML parsing)
- Codebase explorer (two-pass file selection, fallback on failure)
- Updated orchestrator with config-based path
- Backward compatibility (old flat config + old provider argument)
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from alma_atlas.agents.provider import MockProvider
from alma_atlas.agents.schemas import (
    AnnotationResult,
    AssetAnnotation,
    EdgeEnrichment,
    ExplorerResult,
    FileRelevance,
    PipelineAnalysisResult,
)
from alma_atlas.config import AgentConfig, LearningConfig, load_atlas_yml
from alma_atlas.pipeline.learn import (
    run_asset_annotation,
    run_edge_learning,
)
from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_edge(
    upstream: str,
    downstream: str,
    kind: str = "schema_match",
    metadata: dict | None = None,
) -> Edge:
    return Edge(upstream_id=upstream, downstream_id=downstream, kind=kind, metadata=metadata or {})


def _seed_edge(db: Database, edge: Edge) -> None:
    asset_repo = AssetRepository(db)
    for asset_id in (edge.upstream_id, edge.downstream_id):
        name = asset_id.split("::", 1)[-1] if "::" in asset_id else asset_id
        asset_repo.upsert(Asset(id=asset_id, source=asset_id.split("::")[0], kind="table", name=name))
    EdgeRepository(db).upsert(edge)


# ---------------------------------------------------------------------------
# AgentConfig defaults
# ---------------------------------------------------------------------------


def test_agent_config_defaults() -> None:
    cfg = AgentConfig()
    assert cfg.provider == "anthropic"
    assert cfg.model == "claude-opus-4-6"
    assert cfg.api_key_env == "ANTHROPIC_API_KEY"
    assert cfg.timeout == 120
    assert cfg.max_tokens == 4096


def test_agent_config_custom_values() -> None:
    cfg = AgentConfig(provider="openai", model="gpt-4o", timeout=60, max_tokens=2048)
    assert cfg.provider == "openai"
    assert cfg.model == "gpt-4o"
    assert cfg.timeout == 60
    assert cfg.max_tokens == 2048


# ---------------------------------------------------------------------------
# LearningConfig — per-agent sub-configs
# ---------------------------------------------------------------------------


def test_learning_config_per_agent_defaults() -> None:
    cfg = LearningConfig()
    # Per-agent configs default to mock.
    assert isinstance(cfg.explorer, AgentConfig)
    assert cfg.explorer.provider == "mock"
    assert cfg.explorer.model == "claude-haiku-4-5-20251001"
    assert isinstance(cfg.pipeline_analyzer, AgentConfig)
    assert cfg.pipeline_analyzer.provider == "mock"
    assert isinstance(cfg.annotator, AgentConfig)
    assert cfg.annotator.provider == "mock"


def test_learning_config_flat_fields_still_work() -> None:
    cfg = LearningConfig()
    # Flat (legacy) fields are preserved.
    assert cfg.provider == "mock"
    assert cfg.model == "claude-opus-4-6"
    assert cfg.api_key_env == "ANTHROPIC_API_KEY"
    assert cfg.timeout == 120
    assert cfg.max_tokens == 4096


def test_learning_config_per_agent_fields_are_independent() -> None:
    explorer_cfg = AgentConfig(provider="openai", model="gpt-4o-mini")
    analyzer_cfg = AgentConfig(provider="anthropic", model="claude-haiku-4-5-20251001")
    cfg = LearningConfig(explorer=explorer_cfg, pipeline_analyzer=analyzer_cfg)
    assert cfg.explorer.provider == "openai"
    assert cfg.pipeline_analyzer.provider == "anthropic"
    assert cfg.annotator.provider == "mock"  # default


# ---------------------------------------------------------------------------
# load_atlas_yml — flat (legacy) format
# ---------------------------------------------------------------------------


def test_load_atlas_yml_flat_format_sets_per_agent_configs(tmp_path: Path) -> None:
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        enrichment:
          provider: anthropic
          model: claude-opus-4-6
          api_key_env: MY_KEY
          timeout: 60
          max_tokens: 2048
        """)
    )
    cfg = load_atlas_yml(yml)
    # Flat fields preserved.
    assert cfg.learning.provider == "anthropic"
    assert cfg.learning.model == "claude-opus-4-6"
    # Flat format propagates to all per-agent configs.
    assert cfg.learning.explorer.provider == "anthropic"
    assert cfg.learning.explorer.model == "claude-opus-4-6"
    assert cfg.learning.pipeline_analyzer.provider == "anthropic"
    assert cfg.learning.pipeline_analyzer.model == "claude-opus-4-6"
    assert cfg.learning.annotator.provider == "anthropic"
    assert cfg.learning.annotator.model == "claude-opus-4-6"


# ---------------------------------------------------------------------------
# load_atlas_yml — nested (per-agent) format
# ---------------------------------------------------------------------------


def test_load_atlas_yml_nested_per_agent_format(tmp_path: Path) -> None:
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        enrichment:
          explorer:
            provider: anthropic
            model: claude-haiku-4-20250514
            timeout: 30
          pipeline_analyzer:
            provider: anthropic
            model: claude-sonnet-4-20250514
          annotator:
            provider: openai
            model: gpt-4o
        """)
    )
    cfg = load_atlas_yml(yml)
    assert cfg.learning.explorer.provider == "anthropic"
    assert cfg.learning.explorer.model == "claude-haiku-4-20250514"
    assert cfg.learning.explorer.timeout == 30
    assert cfg.learning.pipeline_analyzer.provider == "anthropic"
    assert cfg.learning.pipeline_analyzer.model == "claude-sonnet-4-20250514"
    assert cfg.learning.annotator.provider == "openai"
    assert cfg.learning.annotator.model == "gpt-4o"


def test_load_atlas_yml_nested_partial_uses_defaults(tmp_path: Path) -> None:
    """Omitted per-agent keys use AgentConfig defaults."""
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        enrichment:
          explorer:
            provider: mock
        """)
    )
    cfg = load_atlas_yml(yml)
    assert cfg.learning.explorer.provider == "mock"
    # pipeline_analyzer and annotator fall back to AgentConfig defaults.
    assert cfg.learning.pipeline_analyzer.provider == "anthropic"
    assert cfg.learning.annotator.provider == "anthropic"


# ---------------------------------------------------------------------------
# FileRelevance + ExplorerResult schemas
# ---------------------------------------------------------------------------


def test_file_relevance_valid() -> None:
    fr = FileRelevance(path="dags/load.py", relevance_score=0.9, reason="Contains DAG")
    assert fr.path == "dags/load.py"
    assert fr.relevance_score == 0.9


def test_explorer_result_valid() -> None:
    result = ExplorerResult(
        files=[FileRelevance(path="models/stg.sql", relevance_score=0.8, reason="SQL model")],
        repo_structure_summary="2 dirs, 3 files",
    )
    assert len(result.files) == 1
    assert result.repo_structure_summary == "2 dirs, 3 files"


# ---------------------------------------------------------------------------
# Codebase explorer — MockProvider path (falls back to glob)
# ---------------------------------------------------------------------------


async def test_explorer_fallback_on_empty_result(tmp_path: Path) -> None:
    """MockProvider returns empty files list → explorer falls back to glob."""
    from alma_atlas.agents.codebase_explorer import explore_for_edges

    (tmp_path / "dag.py").write_text("# pipeline dag")
    edge = _make_edge("src::raw.users", "dst::stg.users")
    provider = MockProvider()

    files = await explore_for_edges([edge], tmp_path, provider)
    # Falls back to glob, which finds the .py file.
    names = {p.name for p, _ in files}
    assert "dag.py" in names


async def test_explorer_fallback_on_lm_failure(tmp_path: Path) -> None:
    """LLM exception → explorer falls back to glob."""
    from alma_atlas.agents.codebase_explorer import explore_for_edges
    from alma_atlas.agents.provider import LLMProvider

    class FailProvider(LLMProvider):
        async def analyze(self, system_prompt, user_prompt, response_schema):
            raise RuntimeError("network error")

    (tmp_path / "etl.py").write_text("# etl script")
    edge = _make_edge("a::x.y", "b::x.y")
    files = await explore_for_edges([edge], tmp_path, FailProvider())
    names = {p.name for p, _ in files}
    assert "etl.py" in names


async def test_explorer_uses_ranked_files(tmp_path: Path) -> None:
    """When the provider returns ranked files, only those are loaded."""
    from alma_atlas.agents.codebase_explorer import explore_for_edges

    (tmp_path / "relevant.py").write_text("# relevant pipeline")
    (tmp_path / "unrelated.sql").write_text("SELECT 1")

    fixed_result = ExplorerResult(
        files=[FileRelevance(path="relevant.py", relevance_score=0.95, reason="pipeline code")],
        repo_structure_summary="2 files",
    )
    provider = MockProvider(fixed_result=fixed_result)
    edge = _make_edge("src::raw.a", "dst::stg.a")

    files = await explore_for_edges([edge], tmp_path, provider)
    names = {p.name for p, _ in files}
    assert "relevant.py" in names
    assert "unrelated.sql" not in names


async def test_explorer_for_assets_fallback(tmp_path: Path) -> None:
    """explore_for_assets with MockProvider falls back to glob."""
    from alma_atlas.agents.codebase_explorer import explore_for_assets

    (tmp_path / "model.sql").write_text("SELECT * FROM raw.orders")
    assets = [{"asset_id": "pg::public.orders", "name": "public.orders", "kind": "table"}]
    provider = MockProvider()

    files = await explore_for_assets(assets, tmp_path, provider)
    names = {p.name for p, _ in files}
    assert "model.sql" in names


async def test_explorer_empty_edges_returns_empty(tmp_path: Path) -> None:
    from alma_atlas.agents.codebase_explorer import explore_for_edges

    provider = MockProvider()
    files = await explore_for_edges([], tmp_path, provider)
    assert files == []


async def test_explorer_empty_assets_returns_empty(tmp_path: Path) -> None:
    from alma_atlas.agents.codebase_explorer import explore_for_assets

    provider = MockProvider()
    files = await explore_for_assets([], tmp_path, provider)
    assert files == []


# ---------------------------------------------------------------------------
# repo_scanner
# ---------------------------------------------------------------------------


def test_build_file_index_finds_py_and_sql(tmp_path: Path) -> None:
    from alma_atlas.agents.repo_scanner import build_file_index

    (tmp_path / "dag.py").write_text("# dag")
    (tmp_path / "model.sql").write_text("SELECT 1")
    entries = build_file_index(tmp_path)
    rel_paths = [rel for rel, _ in entries]
    assert "dag.py" in rel_paths
    assert "model.sql" in rel_paths


def test_build_file_index_skips_hidden_dirs(tmp_path: Path) -> None:
    from alma_atlas.agents.repo_scanner import build_file_index

    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / "config").write_text("bogus")
    (tmp_path / "pipeline.py").write_text("# real")
    entries = build_file_index(tmp_path)
    rel_paths = [rel for rel, _ in entries]
    assert not any(".git" in r for r in rel_paths)
    assert "pipeline.py" in rel_paths


# ---------------------------------------------------------------------------
# run_edge_learning — new config-based path
# ---------------------------------------------------------------------------


async def test_run_edge_learning_with_config(db: Database, tmp_path: Path) -> None:
    """run_edge_learning(config=...) uses explorer + analyzer pattern."""
    _seed_edge(db, _make_edge("src::raw.users", "dst::staging.stg_users", kind="schema_match"))

    fixed = PipelineAnalysisResult(
        edges=[
            EdgeEnrichment(
                source_table="raw.users",
                dest_table="staging.stg_users",
                transport_kind="CUSTOM_SCRIPT",
                confidence_note="Found via config path",
            )
        ]
    )
    provider = MockProvider(fixed_result=fixed)

    with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=provider):
        count = await run_edge_learning(db, tmp_path, config=LearningConfig())

    assert count == 1
    edges = EdgeRepository(db).list_all()
    assert edges[0].metadata["learning_status"] == "learned"
    assert edges[0].metadata["transport_kind"] == "CUSTOM_SCRIPT"


async def test_run_edge_learning_no_provider_no_config_raises(db: Database, tmp_path: Path) -> None:
    _seed_edge(db, _make_edge("src::raw.x", "dst::stg.x"))
    with pytest.raises(ValueError, match="requires either"):
        await run_edge_learning(db, tmp_path)


async def test_run_edge_learning_legacy_provider_still_works(db: Database, tmp_path: Path) -> None:
    """Old calling convention: run_edge_learning(db, path, provider) still works."""
    _seed_edge(db, _make_edge("src::raw.orders", "dst::stg.orders", kind="schema_match"))

    fixed = PipelineAnalysisResult(
        edges=[
            EdgeEnrichment(
                source_table="raw.orders",
                dest_table="stg.orders",
                transport_kind="DBT_SEED",
                confidence_note="dbt seed",
            )
        ]
    )
    provider = MockProvider(fixed_result=fixed)
    count = await run_edge_learning(db, tmp_path, provider)
    assert count == 1


# ---------------------------------------------------------------------------
# run_asset_annotation — new config-based path
# ---------------------------------------------------------------------------


async def test_run_asset_annotation_with_config(db: Database, tmp_path: Path) -> None:
    """run_asset_annotation(config=...) uses explorer + enricher pattern."""
    asset_id = "pg::public.items"
    AssetRepository(db).upsert(Asset(id=asset_id, source="pg:test", kind="table", name="public.items"))
    SchemaRepository(db).upsert(
        SchemaSnapshot(
            asset_id=asset_id,
            columns=[ColumnInfo(name="item_id", type="int")],
        )
    )

    fixed = AnnotationResult(
        annotations=[
            AssetAnnotation(
                asset_id=asset_id,
                ownership="analytics",
                granularity="one row per item",
                join_keys=["item_id"],
            )
        ]
    )
    provider = MockProvider(fixed_result=fixed)

    with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=provider):
        count = await run_asset_annotation(db, tmp_path, config=LearningConfig())

    assert count == 1
    record = AnnotationRepository(db).get(asset_id)
    assert record is not None
    assert record.ownership == "analytics"


async def test_run_asset_annotation_no_provider_no_config_raises(db: Database, tmp_path: Path) -> None:
    AssetRepository(db).upsert(Asset(id="pg::public.x", source="pg:test", kind="table", name="public.x"))
    with pytest.raises(ValueError, match="requires either"):
        await run_asset_annotation(db, tmp_path)


async def test_run_asset_annotation_legacy_provider_still_works(db: Database, tmp_path: Path) -> None:
    """Old calling convention still works."""
    asset_id = "pg::public.orders"
    AssetRepository(db).upsert(Asset(id=asset_id, source="pg:test", kind="table", name="public.orders"))

    fixed = AnnotationResult(
        annotations=[
            AssetAnnotation(
                asset_id=asset_id,
                ownership="data-team",
                join_keys=["order_id"],
            )
        ]
    )
    provider = MockProvider(fixed_result=fixed)
    count = await run_asset_annotation(
        db,
        tmp_path,
        provider,
        provider_name="mock",
        model="test",
    )
    assert count == 1
