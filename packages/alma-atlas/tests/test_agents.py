"""Tests for the P1 pipeline analysis agent system.

Covers:
- Schema validation (EdgeEnrichment, PipelineAnalysisResult)
- MockProvider behaviour
- Pipeline analyzer prompt construction
- Enrichment orchestrator (get_unlearned_edges, run_edge_learning)
- CLI dry-run
- Config parsing for the enrichment section
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from alma_atlas.agents.pipeline_analyzer import (
    _build_user_prompt,
    _collect_repo_files,
    analyze_edges,
)
from alma_atlas.agents.provider import MockProvider, make_provider
from alma_atlas.agents.schemas import AnnotationResult, AssetAnnotation, EdgeEnrichment, PipelineAnalysisResult
from alma_atlas.config import AtlasConfig, LearningConfig, load_atlas_yml
from alma_atlas.pipeline.learn import (
    get_unannotated_assets,
    get_unlearned_edges,
    run_asset_annotation,
    run_edge_learning,
)
from alma_atlas_store.annotation_repository import AnnotationRecord, AnnotationRepository
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
    """Seed an edge, pre-creating asset stubs to satisfy FK constraints."""
    asset_repo = AssetRepository(db)
    for asset_id in (edge.upstream_id, edge.downstream_id):
        # Derive a human-readable name from the asset ID (strip source prefix).
        name = asset_id.split("::", 1)[-1] if "::" in asset_id else asset_id
        asset_repo.upsert(Asset(id=asset_id, source=asset_id.split("::")[0], kind="table", name=name))
    EdgeRepository(db).upsert(edge)


# ---------------------------------------------------------------------------
# Schema validation — EdgeEnrichment
# ---------------------------------------------------------------------------


def test_edge_enrichment_valid() -> None:
    e = EdgeEnrichment(
        source_table="raw.users",
        dest_table="staging.stg_users",
        transport_kind="CUSTOM_SCRIPT",
        schedule="0 2 * * *",
        strategy="INCREMENTAL",
        write_disposition="APPEND",
        watermark_column="updated_at",
        owner="data-team",
        confidence_note="Found incremental load script in pipelines/load_users.py.",
    )
    assert e.source_table == "raw.users"
    assert e.dest_table == "staging.stg_users"
    assert e.transport_kind == "CUSTOM_SCRIPT"
    assert e.watermark_column == "updated_at"


def test_edge_enrichment_optional_fields_default_to_none() -> None:
    e = EdgeEnrichment(
        source_table="raw.events",
        dest_table="prod.events",
        transport_kind="UNKNOWN",
        confidence_note="No pipeline code found.",
    )
    assert e.schedule is None
    assert e.strategy is None
    assert e.write_disposition is None
    assert e.watermark_column is None
    assert e.owner is None


def test_edge_enrichment_missing_required_fields_raises() -> None:
    with pytest.raises(ValidationError):
        EdgeEnrichment()  # type: ignore[call-arg]


def test_edge_enrichment_missing_confidence_note_raises() -> None:
    with pytest.raises(ValidationError):
        EdgeEnrichment(
            source_table="a.b",
            dest_table="c.d",
            transport_kind="UNKNOWN",
            # confidence_note is required
        )  # type: ignore[call-arg]


def test_edge_enrichment_missing_source_table_raises() -> None:
    with pytest.raises(ValidationError):
        EdgeEnrichment(
            dest_table="c.d",
            transport_kind="UNKNOWN",
            confidence_note="x",
        )  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Schema validation — PipelineAnalysisResult
# ---------------------------------------------------------------------------


def test_pipeline_analysis_result_empty_edges() -> None:
    r = PipelineAnalysisResult(edges=[])
    assert r.edges == []
    assert r.repo_summary is None


def test_pipeline_analysis_result_with_summary() -> None:
    r = PipelineAnalysisResult(edges=[], repo_summary="Scanned 10 files.")
    assert r.repo_summary == "Scanned 10 files."


def test_pipeline_analysis_result_with_edges() -> None:
    e = EdgeEnrichment(
        source_table="raw.orders",
        dest_table="staging.stg_orders",
        transport_kind="DBT_SEED",
        confidence_note="dbt seed found.",
    )
    r = PipelineAnalysisResult(edges=[e])
    assert len(r.edges) == 1
    assert r.edges[0].source_table == "raw.orders"


def test_pipeline_analysis_result_invalid_type_raises() -> None:
    with pytest.raises((ValidationError, TypeError)):
        PipelineAnalysisResult(edges="not-a-list")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# MockProvider
# ---------------------------------------------------------------------------


async def test_mock_provider_returns_empty_result() -> None:
    provider = MockProvider()
    result = await provider.analyze("sys", "usr", PipelineAnalysisResult)
    assert isinstance(result, PipelineAnalysisResult)
    assert result.edges == []
    assert result.repo_summary == "Mock provider: no analysis performed"


async def test_mock_provider_returns_empty_asset_enrichment_result() -> None:
    provider = MockProvider()
    result = await provider.analyze("sys", "usr", AnnotationResult)
    assert isinstance(result, AnnotationResult)
    assert result.annotations == []
    assert result.repo_summary == "Mock provider: no enrichment performed"


async def test_mock_provider_returns_fixed_result() -> None:
    fixed = PipelineAnalysisResult(
        edges=[
            EdgeEnrichment(
                source_table="raw.users",
                dest_table="staging.stg_users",
                transport_kind="CUSTOM_SCRIPT",
                confidence_note="test",
            )
        ]
    )
    provider = MockProvider(fixed_result=fixed)
    result = await provider.analyze("sys", "usr", PipelineAnalysisResult)
    assert result is fixed


async def test_mock_provider_ignores_prompts() -> None:
    """MockProvider does not use the provided prompts — always returns fixed result."""
    provider = MockProvider()
    result1 = await provider.analyze("sys-a", "usr-a", PipelineAnalysisResult)
    result2 = await provider.analyze("sys-b", "usr-b", PipelineAnalysisResult)
    assert result1.edges == result2.edges


# ---------------------------------------------------------------------------
# make_provider factory
# ---------------------------------------------------------------------------


def test_make_provider_mock() -> None:
    p = make_provider("mock", model="unused")
    assert isinstance(p, MockProvider)


def test_make_provider_unknown_raises() -> None:
    with pytest.raises(ValueError, match="Unknown provider"):
        make_provider("unknown-llm", model="x")


def test_make_provider_anthropic_raises() -> None:
    with pytest.raises(ValueError, match="no longer supported"):
        make_provider("anthropic", model="claude-test", api_key="sk-test")


def test_make_provider_openai_raises() -> None:
    with pytest.raises(ValueError, match="no longer supported"):
        make_provider("openai", model="gpt-4o", api_key="sk-test")


# ---------------------------------------------------------------------------
# Pipeline analyzer — prompt construction
# ---------------------------------------------------------------------------


def test_build_user_prompt_lists_edges() -> None:
    edges = [
        _make_edge("src::raw.users", "dst::staging.stg_users"),
        _make_edge("src::raw.orders", "dst::staging.stg_orders", kind="dbt_source_ref"),
    ]
    prompt = _build_user_prompt(edges, [], Path("/repo"))
    assert "src::raw.users" in prompt
    assert "dst::staging.stg_users" in prompt
    assert "src::raw.orders" in prompt
    assert "dbt_source_ref" in prompt


def test_build_user_prompt_includes_file_contents(tmp_path: Path) -> None:
    py_file = tmp_path / "load.py"
    py_file.write_text("# airflow dag\nwith DAG('pipeline'):\n    pass\n")
    edges = [_make_edge("src::raw.a", "dst::prod.a")]
    files = [(py_file, py_file.read_text())]
    prompt = _build_user_prompt(edges, files, tmp_path)
    assert "airflow dag" in prompt
    assert "load.py" in prompt


def test_build_user_prompt_no_files_shows_placeholder() -> None:
    edges = [_make_edge("a::x.y", "b::x.y")]
    prompt = _build_user_prompt(edges, [], Path("/repo"))
    assert "no relevant files found" in prompt


def test_collect_repo_files_finds_py_and_sql(tmp_path: Path) -> None:
    (tmp_path / "dag.py").write_text("# dag")
    (tmp_path / "transform.sql").write_text("SELECT 1")
    files = _collect_repo_files(tmp_path)
    names = {f.name for f, _ in files}
    assert "dag.py" in names
    assert "transform.sql" in names


def test_collect_repo_files_skips_hidden_dirs(tmp_path: Path) -> None:
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / "config").write_text("bogus")
    (tmp_path / "pipeline.py").write_text("# real")
    files = _collect_repo_files(tmp_path)
    paths = [f for f, _ in files]
    assert not any(".git" in str(p) for p in paths)


def test_collect_repo_files_caps_content(tmp_path: Path) -> None:
    large = tmp_path / "huge.py"
    large.write_text("x" * 100_000)
    files = _collect_repo_files(tmp_path)
    assert all(len(content) <= 4_001 for _, content in files)


async def test_analyze_edges_empty_returns_empty(tmp_path: Path) -> None:
    provider = MockProvider()
    result = await analyze_edges([], tmp_path, provider)
    assert result == []


async def test_analyze_edges_calls_provider(tmp_path: Path) -> None:
    fixed = PipelineAnalysisResult(
        edges=[
            EdgeEnrichment(
                source_table="raw.users",
                dest_table="staging.stg_users",
                transport_kind="CUSTOM_SCRIPT",
                confidence_note="Found in load.py",
            )
        ]
    )
    provider = MockProvider(fixed_result=fixed)
    edges = [_make_edge("src::raw.users", "dst::staging.stg_users")]
    result = await analyze_edges(edges, tmp_path, provider)
    assert len(result) == 1
    assert result[0].transport_kind == "CUSTOM_SCRIPT"


async def test_analyze_edges_provider_failure_returns_empty(tmp_path: Path) -> None:
    """If provider raises, analyze_edges returns [] without re-raising."""
    from alma_atlas.agents.provider import LLMProvider

    class FailingProvider(LLMProvider):
        async def analyze(self, system_prompt, user_prompt, response_schema):
            raise RuntimeError("API error")

    edges = [_make_edge("a::x.y", "b::x.y")]
    result = await analyze_edges(edges, tmp_path, FailingProvider())
    assert result == []


# ---------------------------------------------------------------------------
# Enrichment orchestrator — get_unlearned_edges
# ---------------------------------------------------------------------------


def test_get_unlearned_edges_returns_unenriched(db: Database) -> None:
    _seed_edge(db, _make_edge("src::raw.users", "dst::stg.users", kind="schema_match"))
    _seed_edge(db, _make_edge("a::raw.orders", "b::stg.orders", kind="dbt_source_ref"))
    unenriched = get_unlearned_edges(db)
    assert len(unenriched) == 2


def test_get_unlearned_edges_skips_already_enriched(db: Database) -> None:
    _seed_edge(
        db,
        _make_edge(
            "src::raw.users",
            "dst::stg.users",
            kind="schema_match",
            metadata={"learning_status": "learned"},
        ),
    )
    assert get_unlearned_edges(db) == []


def test_get_unlearned_edges_skips_other_kinds(db: Database) -> None:
    _seed_edge(db, _make_edge("a::x.y", "b::x.y", kind="reads"))
    assert get_unlearned_edges(db) == []


def test_get_unlearned_edges_mixed(db: Database) -> None:
    _seed_edge(db, _make_edge("a::raw.users", "b::stg.users", kind="schema_match"))
    _seed_edge(db, _make_edge("c::raw.orders", "d::stg.orders", kind="schema_match", metadata={"learning_status": "learned"}))
    _seed_edge(db, _make_edge("e::x.y", "f::x.y", kind="reads"))
    unenriched = get_unlearned_edges(db)
    assert len(unenriched) == 1
    assert unenriched[0].upstream_id == "a::raw.users"


def test_get_unlearned_edges_cross_system_same_table(db: Database) -> None:
    """Cross-system edges with identical schema.table ARE learnable — different source systems."""
    _seed_edge(db, _make_edge("pg::raw.users", "dbt::raw.users", kind="schema_match"))
    _seed_edge(db, _make_edge("pg::raw.orders", "dbt::raw.orders", kind="dbt_source_ref"))
    unenriched = get_unlearned_edges(db)
    assert len(unenriched) == 2


def test_get_unlearned_edges_skips_same_system_self_loop(db: Database) -> None:
    """True self-loops within the same source system are skipped."""
    _seed_edge(db, _make_edge("pg::raw.users", "pg::raw.users", kind="schema_match"))
    assert get_unlearned_edges(db) == []


# ---------------------------------------------------------------------------
# Enrichment orchestrator — run_edge_learning
# ---------------------------------------------------------------------------


async def test_run_edge_learning_persists_enrichment(db: Database, tmp_path: Path) -> None:
    _seed_edge(db, _make_edge("src::raw.users", "dst::staging.stg_users", kind="schema_match"))

    fixed = PipelineAnalysisResult(
        edges=[
            EdgeEnrichment(
                source_table="raw.users",
                dest_table="staging.stg_users",
                transport_kind="CUSTOM_SCRIPT",
                schedule="0 2 * * *",
                strategy="INCREMENTAL",
                confidence_note="Found in load.py",
            )
        ]
    )
    provider = MockProvider(fixed_result=fixed)
    count = await run_edge_learning(db, tmp_path, provider)

    assert count == 1
    edges = EdgeRepository(db).list_all()
    assert len(edges) == 1
    meta = edges[0].metadata
    assert meta["learning_status"] == "learned"
    assert meta["transport_kind"] == "CUSTOM_SCRIPT"
    assert meta["strategy"] == "INCREMENTAL"


async def test_run_edge_learning_no_edges_returns_zero(db: Database, tmp_path: Path) -> None:
    provider = MockProvider()
    count = await run_edge_learning(db, tmp_path, provider)
    assert count == 0


async def test_run_edge_learning_already_enriched_skipped(db: Database, tmp_path: Path) -> None:
    _seed_edge(
        db,
        _make_edge(
            "src::raw.users",
            "dst::stg.users",
            kind="schema_match",
            metadata={"learning_status": "learned"},
        ),
    )
    provider = MockProvider()
    count = await run_edge_learning(db, tmp_path, provider)
    assert count == 0


async def test_run_edge_learning_unmatched_edge_not_updated(db: Database, tmp_path: Path) -> None:
    """If agent returns no match for an edge, it remains unenriched."""
    _seed_edge(db, _make_edge("src::raw.orders", "dst::staging.stg_orders", kind="schema_match"))
    # MockProvider returns empty edges list — no match
    provider = MockProvider()
    count = await run_edge_learning(db, tmp_path, provider)
    assert count == 0
    edges = EdgeRepository(db).list_all()
    assert edges[0].metadata.get("learning_status") != "learned"


# ---------------------------------------------------------------------------
# Asset enrichment orchestrator (P2)
# ---------------------------------------------------------------------------


async def test_get_unannotated_assets_returns_assets_without_annotations(db: Database) -> None:
    AssetRepository(db).upsert(Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders"))
    AssetRepository(db).upsert(Asset(id="pg::public.users", source="pg:test", kind="table", name="public.users"))

    # Annotate one of them
    AnnotationRepository(db).upsert(
        AnnotationRecord(
            asset_id="pg::public.orders",
            ownership="data",
            join_keys=["order_id"],
            annotated_by="agent:mock",
        )
    )

    unannotated = get_unannotated_assets(db)
    assert "pg::public.users" in unannotated
    assert "pg::public.orders" not in unannotated


async def test_run_asset_annotation_persists_annotations(db: Database, tmp_path: Path) -> None:
    # Seed one asset + schema
    asset_id = "pg::public.orders"
    AssetRepository(db).upsert(Asset(id=asset_id, source="pg:test", kind="table", name="public.orders"))
    SchemaRepository(db).upsert(
        SchemaSnapshot(
            asset_id=asset_id,
            columns=[ColumnInfo(name="order_id", type="int"), ColumnInfo(name="user_id", type="int")],
        )
    )

    fixed = AnnotationResult(
        annotations=[
            AssetAnnotation(
                asset_id=asset_id,
                ownership="data",
                granularity="one row per order",
                join_keys=["order_id"],
                freshness_guarantee="hourly",
                business_logic_summary="orders table",
                sensitivity="financial",
            )
        ],
        repo_summary="ok",
    )
    provider = MockProvider(fixed_result=fixed)

    count = await run_asset_annotation(
        db,
        tmp_path,
        provider,
        provider_name="mock",
        model="test",
        limit=10,
        batch_size=20,
    )
    assert count == 1

    record = AnnotationRepository(db).get(asset_id)
    assert record is not None
    assert record.ownership == "data"
    assert record.join_keys == ["order_id"]
    assert record.annotated_by == "agent:mock:test"


# ---------------------------------------------------------------------------
# CLI — dry-run
# ---------------------------------------------------------------------------


def test_cli_enrich_help() -> None:
    from typer.testing import CliRunner

    from alma_atlas.cli.main import app

    runner = CliRunner()
    result = runner.invoke(app, ["learn", "--help"])
    assert result.exit_code == 0
    assert "learn" in result.output.lower()


def test_cli_enrich_dry_run_no_edges(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from alma_atlas.cli.main import app
    from alma_atlas.config import AtlasConfig

    db_path = tmp_path / "atlas.db"
    with Database(db_path):
        pass  # create the DB

    cfg = AtlasConfig(config_dir=tmp_path, db_path=db_path)

    runner = CliRunner()
    with patch("alma_atlas.cli.learn.get_config", return_value=cfg):
        result = runner.invoke(app, ["learn", "--dry-run"])

    assert result.exit_code == 0
    assert "No unlearned edges" in result.output


def test_cli_enrich_assets_dry_run_no_assets(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from alma_atlas.cli.main import app

    db_path = tmp_path / "atlas.db"
    with Database(db_path):
        pass

    cfg = AtlasConfig(config_dir=tmp_path, db_path=db_path)

    runner = CliRunner()
    with patch("alma_atlas.cli.learn.get_config", return_value=cfg):
        result = runner.invoke(app, ["learn", "--assets", "--dry-run"])

    assert result.exit_code == 0
    assert "No unannotated assets" in result.output


def test_cli_enrich_assets_dry_run_shows_assets(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from alma_atlas.cli.main import app

    db_path = tmp_path / "atlas.db"
    with Database(db_path) as db:
        AssetRepository(db).upsert(Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders"))

    cfg = AtlasConfig(config_dir=tmp_path, db_path=db_path)

    runner = CliRunner()
    with patch("alma_atlas.cli.learn.get_config", return_value=cfg):
        result = runner.invoke(app, ["learn", "--assets", "--dry-run"])

    assert result.exit_code == 0
    assert "pg::public.orders" in result.output


def test_cli_enrich_dry_run_shows_edges(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from alma_atlas.cli.main import app

    db_path = tmp_path / "atlas.db"
    with Database(db_path) as db:
        _seed_edge(db, _make_edge("src::raw.users", "dst::stg.users", kind="schema_match"))

    cfg = AtlasConfig(config_dir=tmp_path, db_path=db_path)

    runner = CliRunner()
    with patch("alma_atlas.cli.learn.get_config", return_value=cfg):
        result = runner.invoke(app, ["learn", "--dry-run"])

    assert result.exit_code == 0
    assert "raw.users" in result.output or "src::raw.users" in result.output


def test_cli_enrich_no_repo_exits_nonzero(tmp_path: Path) -> None:
    """Running without --repo and without --dry-run should fail."""
    from typer.testing import CliRunner

    from alma_atlas.cli.main import app

    db_path = tmp_path / "atlas.db"
    with Database(db_path) as db:
        _seed_edge(db, _make_edge("src::raw.users", "dst::stg.users", kind="schema_match"))

    cfg = AtlasConfig(config_dir=tmp_path, db_path=db_path)

    runner = CliRunner()
    with patch("alma_atlas.cli.learn.get_config", return_value=cfg):
        result = runner.invoke(app, ["learn"])

    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Config parsing — enrichment section
# ---------------------------------------------------------------------------


def test_enrichment_config_defaults() -> None:
    cfg = LearningConfig()
    assert cfg.provider == "mock"
    assert cfg.model == "claude-opus-4-6"
    assert cfg.api_key_env == "ANTHROPIC_API_KEY"
    assert cfg.timeout == 120
    assert cfg.max_tokens == 4096


def test_atlas_config_has_default_enrichment() -> None:
    cfg = AtlasConfig()
    assert isinstance(cfg.learning, LearningConfig)
    assert cfg.learning.provider == "mock"


def test_load_atlas_yml_enrichment_section(tmp_path: Path) -> None:
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          provider: anthropic
          model: claude-opus-4-6
          api_key_env: MY_ANTHROPIC_KEY
          timeout: 60
          max_tokens: 2048
        """)
    )
    cfg = load_atlas_yml(yml)
    assert cfg.learning.provider == "anthropic"
    assert cfg.learning.model == "claude-opus-4-6"
    assert cfg.learning.api_key_env == "MY_ANTHROPIC_KEY"
    assert cfg.learning.timeout == 60
    assert cfg.learning.max_tokens == 2048


def test_load_atlas_yml_enrichment_defaults_when_absent(tmp_path: Path) -> None:
    yml = tmp_path / "atlas.yml"
    yml.write_text("version: 1\n")
    cfg = load_atlas_yml(yml)
    assert cfg.learning.provider == "mock"


def test_load_atlas_yml_unknown_enrichment_key_not_rejected(tmp_path: Path) -> None:
    """Unknown keys inside enrichment sub-dict are silently ignored (only top-level is strict)."""
    yml = tmp_path / "atlas.yml"
    yml.write_text(
        textwrap.dedent("""\
        version: 1
        learning:
          provider: mock
          unknown_future_key: value
        """)
    )
    # Should not raise
    cfg = load_atlas_yml(yml)
    assert cfg.learning.provider == "mock"
