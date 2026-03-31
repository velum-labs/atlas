"""End-to-end integration test for the lead/specialist agent orchestration pipeline.

Flow:
  1. Create fake dbt project + fake pipeline repo (Airflow DAG, SQL scripts)
  2. Scan dbt source → populate store with assets + edges
  3. Seed cross-system edges (schema_match / dbt_source_ref)
  4. Run full learning with lead/specialist config (explorer → parallel specialists)
  5. Verify edge transport metadata persisted
  6. Verify asset annotations persisted
  7. Verify MCP tools surface learned data

No network access required — uses MockProvider with realistic fixed results
to validate the full orchestration flow without hitting any LLM API.
"""

from __future__ import annotations

import asyncio
import json
import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from alma_atlas.agents.provider import LLMProvider, MockProvider
from alma_atlas.agents.schemas import (
    AnnotationResult,
    AssetAnnotation,
    EdgeEnrichment,
    ExplorerResult,
    FileRelevance,
    PipelineAnalysisResult,
)
from alma_atlas.config import AgentConfig, AtlasConfig, LearningConfig, SourceConfig
from alma_atlas.pipeline.learn import (
    get_unannotated_assets,
    get_unlearned_edges,
    run_asset_annotation,
    run_edge_learning,
)
from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot

# ---------------------------------------------------------------------------
# Realistic fixtures
# ---------------------------------------------------------------------------

MANIFEST = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json",
        "project_name": "fintual",
    },
    "nodes": {
        "model.fintual.stg_users": {
            "unique_id": "model.fintual.stg_users",
            "resource_type": "model",
            "schema": "analytics",
            "name": "stg_users",
            "description": "Staged user accounts",
            "config": {"materialized": "table"},
            "columns": {
                "user_id": {"name": "user_id", "data_type": "bigint"},
                "email": {"name": "email", "data_type": "varchar"},
                "created_at": {"name": "created_at", "data_type": "timestamp"},
            },
            "depends_on": {"nodes": ["source.fintual.raw.users"]},
        },
        "model.fintual.stg_transactions": {
            "unique_id": "model.fintual.stg_transactions",
            "resource_type": "model",
            "schema": "analytics",
            "name": "stg_transactions",
            "description": "Staged transactions with amounts",
            "config": {"materialized": "incremental"},
            "columns": {
                "txn_id": {"name": "txn_id", "data_type": "bigint"},
                "user_id": {"name": "user_id", "data_type": "bigint"},
                "amount": {"name": "amount", "data_type": "numeric"},
                "txn_date": {"name": "txn_date", "data_type": "date"},
            },
            "depends_on": {"nodes": ["source.fintual.raw.transactions"]},
        },
    },
    "sources": {
        "source.fintual.raw.users": {
            "unique_id": "source.fintual.raw.users",
            "resource_type": "source",
            "schema": "raw",
            "name": "users",
            "source_name": "raw",
            "columns": {
                "user_id": {"name": "user_id", "data_type": "bigint"},
                "email": {"name": "email", "data_type": "varchar"},
                "created_at": {"name": "created_at", "data_type": "timestamp"},
            },
        },
        "source.fintual.raw.transactions": {
            "unique_id": "source.fintual.raw.transactions",
            "resource_type": "source",
            "schema": "raw",
            "name": "transactions",
            "source_name": "raw",
            "columns": {
                "txn_id": {"name": "txn_id", "data_type": "bigint"},
                "user_id": {"name": "user_id", "data_type": "bigint"},
                "amount": {"name": "amount", "data_type": "numeric"},
                "txn_date": {"name": "txn_date", "data_type": "date"},
            },
        },
    },
}

AIRFLOW_DAG = textwrap.dedent("""\
    from airflow import DAG
    from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    from datetime import datetime, timedelta

    default_args = {"owner": "data-team", "retries": 2}

    with DAG(
        "load_users_to_bq",
        schedule_interval="0 2 * * *",
        default_args=default_args,
        start_date=datetime(2025, 1, 1),
    ) as dag:
        extract = PostgresToGCSOperator(
            task_id="extract_users",
            postgres_conn_id="pg_main",
            sql="SELECT * FROM public.users WHERE updated_at > '{{ ds }}'",
            bucket="fintual-data",
            filename="users/{{ ds }}/users.json",
        )
        load = GCSToBigQueryOperator(
            task_id="load_users_bq",
            bucket="fintual-data",
            source_objects=["users/{{ ds }}/users.json"],
            destination_project_dataset_table="raw.users",
            write_disposition="WRITE_TRUNCATE",
        )
        extract >> load
""")

TXN_LOAD_SQL = textwrap.dedent("""\
    -- Incremental load: transactions from source to raw
    INSERT INTO raw.transactions (txn_id, user_id, amount, txn_date)
    SELECT txn_id, user_id, amount, txn_date
    FROM source_db.public.transactions
    WHERE txn_date > (SELECT COALESCE(MAX(txn_date), '1970-01-01') FROM raw.transactions);
""")


@pytest.fixture
def dbt_project(tmp_path: Path) -> Path:
    target_dir = tmp_path / "fintual_dbt" / "target"
    target_dir.mkdir(parents=True)
    (target_dir / "manifest.json").write_text(json.dumps(MANIFEST))
    return tmp_path / "fintual_dbt"


@pytest.fixture
def pipeline_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "pipeline_repo"
    dags = repo / "dags"
    dags.mkdir(parents=True)
    (dags / "load_users.py").write_text(AIRFLOW_DAG)
    pipelines = repo / "pipelines"
    pipelines.mkdir()
    (pipelines / "load_transactions.sql").write_text(TXN_LOAD_SQL)
    utils = repo / "utils"
    utils.mkdir()
    (utils / "logging.py").write_text("import logging\nlogger = logging.getLogger(__name__)\n")
    return repo


@pytest.fixture
def e2e_config(tmp_path: Path) -> AtlasConfig:
    return AtlasConfig(
        config_dir=tmp_path / "alma",
        db_path=tmp_path / "atlas.db",
        learning=LearningConfig(
            explorer=AgentConfig(provider="acp", model="haiku"),
            pipeline_analyzer=AgentConfig(provider="acp", model="sonnet"),
            annotator=AgentConfig(provider="acp", model="sonnet"),
        ),
    )


@pytest.fixture
def dbt_source(dbt_project: Path) -> SourceConfig:
    manifest = str(dbt_project / "target" / "manifest.json")
    return SourceConfig(id="dbt:fintual", kind="dbt", params={"manifest_path": manifest})


# ---------------------------------------------------------------------------
# Smart MockProvider that returns realistic results keyed on prompt content
# ---------------------------------------------------------------------------


class SmartMockProvider(LLMProvider):
    def __init__(self, repo_files: list[str] | None = None) -> None:
        self._repo_files = repo_files or []

    async def analyze(self, system_prompt: str, user_prompt: str, response_schema: type) -> Any:
        from alma_atlas.agents.schemas import ExplorerResult

        if issubclass(response_schema, ExplorerResult):
            return ExplorerResult(
                files=[
                    FileRelevance(path=f, relevance_score=0.9, reason="pipeline code")
                    for f in self._repo_files
                ],
                repo_structure_summary=f"{len(self._repo_files)} relevant files",
            )

        if issubclass(response_schema, PipelineAnalysisResult):
            edges = []
            if "raw.users" in user_prompt:
                edges.append(
                    EdgeEnrichment(
                        source_table="raw.users",
                        dest_table="staging.users",
                        transport_kind="CUSTOM_SCRIPT",
                        schedule="0 2 * * *",
                        strategy="FULL",
                        write_disposition="TRUNCATE",
                        owner="data-team",
                        confidence_note="Found Airflow DAG load_users_to_bq with PostgresToGCS + GCSToBigQuery.",
                    )
                )
            if "raw.transactions" in user_prompt:
                edges.append(
                    EdgeEnrichment(
                        source_table="raw.transactions",
                        dest_table="staging.transactions",
                        transport_kind="CUSTOM_SCRIPT",
                        schedule=None,
                        strategy="INCREMENTAL",
                        write_disposition="APPEND",
                        watermark_column="txn_date",
                        confidence_note="Found SQL incremental load keyed on txn_date.",
                    )
                )
            return PipelineAnalysisResult(edges=edges, repo_summary="2 pipeline files analyzed")

        if issubclass(response_schema, AnnotationResult):
            annotations = []
            if "stg_users" in user_prompt or "raw.users" in user_prompt:
                annotations.append(
                    AssetAnnotation(
                        asset_id="dbt:fintual::analytics.stg_users",
                        ownership="data-team",
                        granularity="one row per user",
                        join_keys=["user_id"],
                        freshness_guarantee="daily at 02:00 UTC",
                        business_logic_summary="Staged user accounts from Postgres.",
                        sensitivity="PII",
                    )
                )
            if "stg_transactions" in user_prompt or "raw.transactions" in user_prompt:
                annotations.append(
                    AssetAnnotation(
                        asset_id="dbt:fintual::analytics.stg_transactions",
                        ownership="data-team",
                        granularity="one row per transaction",
                        join_keys=["txn_id", "user_id"],
                        freshness_guarantee="incremental, near-real-time",
                        business_logic_summary="Staged financial transactions with amounts.",
                        sensitivity="financial",
                    )
                )
            return AnnotationResult(annotations=annotations, repo_summary="annotated from pipeline code")

        return response_schema.model_validate({})


# ---------------------------------------------------------------------------
# E2E: scan → seed edges → learn → annotate → verify
# ---------------------------------------------------------------------------


class TestE2ELearningPipeline:
    """Full end-to-end test: dbt scan → cross-system edges → learning → annotations."""

    @pytest.fixture(autouse=True)
    def setup(self, e2e_config: AtlasConfig, dbt_source: SourceConfig, pipeline_repo: Path) -> None:
        self.config = e2e_config
        self.dbt_source = dbt_source
        self.pipeline_repo = pipeline_repo

    def _scan_and_seed(self) -> Database:
        from alma_atlas.pipeline.scan import run_scan

        result = run_scan(self.dbt_source, self.config)
        assert result.error is None
        assert result.asset_count >= 4

        db = Database(self.config.db_path)

        asset_repo = AssetRepository(db)
        edge_repo = EdgeRepository(db)

        asset_repo.upsert(Asset(id="pg:main::raw.users", source="pg:main", kind="table", name="raw.users"))
        asset_repo.upsert(Asset(id="pg:main::raw.transactions", source="pg:main", kind="table", name="raw.transactions"))
        asset_repo.upsert(Asset(id="dbt:fintual::staging.users", source="dbt:fintual", kind="model", name="staging.users"))
        asset_repo.upsert(Asset(id="dbt:fintual::staging.transactions", source="dbt:fintual", kind="model", name="staging.transactions"))

        edge_repo.upsert(Edge(
            upstream_id="pg:main::raw.users",
            downstream_id="dbt:fintual::staging.users",
            kind="schema_match",
            metadata={"confidence": 0.95},
        ))
        edge_repo.upsert(Edge(
            upstream_id="pg:main::raw.transactions",
            downstream_id="dbt:fintual::staging.transactions",
            kind="schema_match",
            metadata={"confidence": 0.92},
        ))

        SchemaRepository(db).upsert(SchemaSnapshot(
            asset_id="dbt:fintual::analytics.stg_users",
            columns=[
                ColumnInfo(name="user_id", type="bigint"),
                ColumnInfo(name="email", type="varchar"),
                ColumnInfo(name="created_at", type="timestamp"),
            ],
        ))
        SchemaRepository(db).upsert(SchemaSnapshot(
            asset_id="dbt:fintual::analytics.stg_transactions",
            columns=[
                ColumnInfo(name="txn_id", type="bigint"),
                ColumnInfo(name="user_id", type="bigint"),
                ColumnInfo(name="amount", type="numeric"),
                ColumnInfo(name="txn_date", type="date"),
            ],
        ))

        return db

    def test_e2e_full_pipeline_scan_learn_annotate(self) -> None:
        """Full pipeline: scan → learn edges → annotate assets → verify MCP."""
        db = self._scan_and_seed()
        try:
            unlearned = get_unlearned_edges(db)
            assert len(unlearned) >= 2
            kinds = {e.kind for e in unlearned}
            assert "schema_match" in kinds or "depends_on" in kinds

            smart_provider = SmartMockProvider(
                repo_files=["dags/load_users.py", "pipelines/load_transactions.sql"]
            )
            with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=smart_provider):
                edge_count = asyncio.run(
                    run_edge_learning(db, self.pipeline_repo, config=self.config.learning)
                )
            assert edge_count == 2

            edges = EdgeRepository(db).list_all()
            learned_edges = [e for e in edges if e.metadata.get("learning_status") == "learned"]
            assert len(learned_edges) == 2

            by_upstream = {e.upstream_id: e for e in learned_edges}

            users_edge = by_upstream["pg:main::raw.users"]
            assert users_edge.metadata["transport_kind"] == "CUSTOM_SCRIPT"
            assert users_edge.metadata["schedule"] == "0 2 * * *"
            assert users_edge.metadata["strategy"] == "FULL"
            assert users_edge.metadata["write_disposition"] == "TRUNCATE"
            assert users_edge.metadata["owner"] == "data-team"
            assert "Airflow" in users_edge.metadata["confidence_note"]

            txn_edge = by_upstream["pg:main::raw.transactions"]
            assert txn_edge.metadata["transport_kind"] == "CUSTOM_SCRIPT"
            assert txn_edge.metadata["strategy"] == "INCREMENTAL"
            assert txn_edge.metadata["watermark_column"] == "txn_date"

            # Verify the seeded schema_match edges are now learned
            unlearned_ids = {(e.upstream_id, e.downstream_id) for e in get_unlearned_edges(db)}
            assert ("pg:main::raw.users", "dbt:fintual::staging.users") not in unlearned_ids
            assert ("pg:main::raw.transactions", "dbt:fintual::staging.transactions") not in unlearned_ids

            unannotated = get_unannotated_assets(db)
            assert len(unannotated) >= 2

            with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=smart_provider):
                asset_count = asyncio.run(
                    run_asset_annotation(db, self.pipeline_repo, config=self.config.learning)
                )
            assert asset_count >= 2

            ann_repo = AnnotationRepository(db)

            users_ann = ann_repo.get("dbt:fintual::analytics.stg_users")
            assert users_ann is not None
            assert users_ann.ownership == "data-team"
            assert users_ann.granularity == "one row per user"
            assert users_ann.join_keys == ["user_id"]
            assert users_ann.sensitivity == "PII"
            assert "acp" in users_ann.annotated_by

            txn_ann = ann_repo.get("dbt:fintual::analytics.stg_transactions")
            assert txn_ann is not None
            assert txn_ann.ownership == "data-team"
            assert txn_ann.join_keys == ["txn_id", "user_id"]
            assert txn_ann.sensitivity == "financial"
            assert txn_ann.granularity == "one row per transaction"

            from alma_atlas.mcp.tools import _handle_get_asset, _handle_lineage, _handle_status

            status = _handle_status(self.config)
            assert len(status) == 1
            status_text = status[0].text
            assert "assets" in status_text.lower()

            lineage_result = _handle_lineage(
                self.config,
                {"asset_id": "pg:main::raw.users", "direction": "downstream"},
            )
            assert len(lineage_result) == 1
            lineage_text = lineage_result[0].text
            assert "dbt:fintual::staging.users" in lineage_text

            asset_result = _handle_get_asset(
                self.config,
                {"asset_id": "dbt:fintual::analytics.stg_users"},
            )
            assert len(asset_result) == 1
            asset_data = json.loads(asset_result[0].text)
            assert asset_data["id"] == "dbt:fintual::analytics.stg_users"
            assert asset_data["kind"] == "table"

        finally:
            db.close()

    def test_e2e_idempotent_learning(self) -> None:
        """Running learning twice doesn't re-process already-learned edges."""
        db = self._scan_and_seed()
        try:
            smart_provider = SmartMockProvider(
                repo_files=["dags/load_users.py", "pipelines/load_transactions.sql"]
            )
            with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=smart_provider):
                first_count = asyncio.run(
                    run_edge_learning(db, self.pipeline_repo, config=self.config.learning)
                )
            assert first_count == 2

            with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=smart_provider):
                second_count = asyncio.run(
                    run_edge_learning(db, self.pipeline_repo, config=self.config.learning)
                )
            assert second_count == 0
        finally:
            db.close()

    def test_e2e_explorer_fallback_on_failure(self) -> None:
        """If explorer LLM fails, learning still works via glob fallback."""
        db = self._scan_and_seed()
        try:
            call_count = {"explorer": 0, "analyzer": 0}

            class TrackingProvider(LLMProvider):
                async def analyze(self, system_prompt, user_prompt, response_schema):
                    if issubclass(response_schema, ExplorerResult):
                        call_count["explorer"] += 1
                        raise RuntimeError("Explorer down")
                    call_count["analyzer"] += 1
                    return await SmartMockProvider(
                        repo_files=["dags/load_users.py", "pipelines/load_transactions.sql"]
                    ).analyze(system_prompt, user_prompt, response_schema)

            with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=TrackingProvider()):
                count = asyncio.run(
                    run_edge_learning(db, self.pipeline_repo, config=self.config.learning)
                )

            assert count == 2
            assert call_count["explorer"] >= 1
            assert call_count["analyzer"] >= 1
        finally:
            db.close()

    def test_e2e_config_from_yaml(self, tmp_path: Path) -> None:
        """Verify the full YAML → config → provider instantiation path."""
        from alma_atlas.config import load_atlas_yml

        yml = tmp_path / "atlas.yml"
        yml.write_text(textwrap.dedent("""\
            version: 1
            learning:
              explorer:
                provider: mock
                model: claude-haiku-4-20250514
                timeout: 30
              pipeline_analyzer:
                provider: mock
                model: claude-sonnet-4-20250514
              annotator:
                provider: mock
                model: claude-sonnet-4-20250514
        """))

        cfg = load_atlas_yml(yml)

        assert cfg.learning.explorer.provider == "mock"
        assert cfg.learning.explorer.model == "claude-haiku-4-20250514"
        assert cfg.learning.explorer.timeout == 30
        assert cfg.learning.pipeline_analyzer.provider == "mock"
        assert cfg.learning.annotator.provider == "mock"

        from alma_atlas.pipeline.learn import _provider_from_agent_config

        explorer_p = _provider_from_agent_config(cfg.learning.explorer)
        assert isinstance(explorer_p, MockProvider)
        analyzer_p = _provider_from_agent_config(cfg.learning.pipeline_analyzer)
        assert isinstance(analyzer_p, MockProvider)

    def test_e2e_parallel_edge_and_asset_learning(self) -> None:
        """Edge and asset learning can run via the same config, sequentially."""
        db = self._scan_and_seed()
        try:
            smart_provider = SmartMockProvider(
                repo_files=["dags/load_users.py", "pipelines/load_transactions.sql"]
            )

            async def run_both():
                with patch("alma_atlas.pipeline.learn._provider_from_agent_config", return_value=smart_provider):
                    edges = await run_edge_learning(db, self.pipeline_repo, config=self.config.learning)
                    assets = await run_asset_annotation(db, self.pipeline_repo, config=self.config.learning)
                return edges, assets

            edge_count, asset_count = asyncio.run(run_both())
            assert edge_count == 2
            assert asset_count >= 2

            remaining = get_unlearned_edges(db)
            assert all(e.kind != "schema_match" for e in remaining)

            ann_repo = AnnotationRepository(db)
            assert ann_repo.get("dbt:fintual::analytics.stg_users") is not None
            assert ann_repo.get("dbt:fintual::analytics.stg_transactions") is not None
        finally:
            db.close()
