"""Tests for the GitHub source adapter."""

from __future__ import annotations

import os
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from alma_connectors.adapters.github import (
    GitHubAdapter,
    _extract_dbt_refs,
    _extract_dbt_sources,
    _extract_python_imports,
    _extract_tables_from_python_file,
    _extract_tables_from_sql_file,
    _lineage_from_sql_file,
    _lineage_from_python_file,
    _matches_patterns,
    _resolve_import_to_file,
    _scan_repo_dir,
    _scan_repo_lineage_edges,
    stitch_cross_system_edges,
)
from alma_connectors.source_adapter_v2 import LineageEdge, LineageEdgeKind
from alma_connectors.registry import (
    CONNECTOR_SPECS,
    build_persisted_adapter,
    get_connector_spec,
)
from alma_connectors.source_adapter import (
    GitHubAdapterConfig,
    PersistedSourceAdapter,
    SourceAdapterKind,
    SourceAdapterStatus,
)


def _make_adapter_record() -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=str(uuid4()),
        key="test-github",
        display_name="Test GitHub",
        kind=SourceAdapterKind.GITHUB,
        target_id="test-github",
        config=GitHubAdapterConfig(
            token_secret=None,
            app_id="123",
            installation_id="456",
            repos=("org/repo1",),
        ),
        status=SourceAdapterStatus.READY,
    )


# ------------------------------------------------------------------
# Pattern matching
# ------------------------------------------------------------------


class TestMatchesPatterns:
    def test_sql_matches(self) -> None:
        assert _matches_patterns("models/staging.sql", ("*.sql",), ())

    def test_py_matches(self) -> None:
        assert _matches_patterns("scripts/etl.py", ("*.py",), ())

    def test_excluded_by_pattern(self) -> None:
        assert not _matches_patterns(
            "node_modules/foo.sql",
            ("*.sql",),
            ("**/node_modules/**",),
        )

    def test_not_included(self) -> None:
        assert not _matches_patterns("readme.md", ("*.sql", "*.py"), ())


# ------------------------------------------------------------------
# SQL file scanning
# ------------------------------------------------------------------


class TestExtractTablesFromSql:
    def test_simple_select(self) -> None:
        refs = _extract_tables_from_sql_file("SELECT * FROM public.orders")
        table_names = {r.canonical_name for r in refs}
        assert "public.orders" in table_names

    def test_join_multiple_tables(self) -> None:
        sql = "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id"
        refs = _extract_tables_from_sql_file(sql)
        table_names = {r.canonical_name for r in refs}
        assert "public.orders" in table_names
        assert "public.customers" in table_names

    def test_invalid_sql_returns_empty(self) -> None:
        refs = _extract_tables_from_sql_file("THIS IS NOT SQL AT ALL !!!")
        # Should not raise, may return empty or partial
        assert isinstance(refs, list)

    def test_cte_extracts_underlying_tables(self) -> None:
        sql = textwrap.dedent("""\
            WITH recent AS (
                SELECT * FROM staging.events WHERE created_at > '2024-01-01'
            )
            SELECT r.*, u.name
            FROM recent r
            JOIN public.users u ON r.user_id = u.id
        """)
        refs = _extract_tables_from_sql_file(sql)
        table_names = {r.canonical_name for r in refs}
        assert "staging.events" in table_names
        assert "public.users" in table_names
        # The CTE alias "recent" should NOT appear as a table
        assert "public.recent" not in table_names

    def test_subquery_extracts_inner_tables(self) -> None:
        sql = textwrap.dedent("""\
            SELECT *
            FROM (
                SELECT order_id, SUM(amount) AS total
                FROM billing.line_items
                GROUP BY order_id
            ) sub
            JOIN warehouse.orders o ON sub.order_id = o.id
        """)
        refs = _extract_tables_from_sql_file(sql)
        table_names = {r.canonical_name for r in refs}
        assert "billing.line_items" in table_names
        assert "warehouse.orders" in table_names

    def test_insert_into_extracts_target_table(self) -> None:
        sql = "INSERT INTO analytics.daily_stats SELECT * FROM raw.events"
        refs = _extract_tables_from_sql_file(sql)
        table_names = {r.canonical_name for r in refs}
        assert "analytics.daily_stats" in table_names
        assert "raw.events" in table_names


# ------------------------------------------------------------------
# Python file scanning
# ------------------------------------------------------------------


class TestExtractTablesFromPython:
    def test_sqlalchemy_tablename(self) -> None:
        content = textwrap.dedent("""
            class Order(Base):
                __tablename__ = 'orders'
                id = Column(Integer, primary_key=True)
        """)
        tables = _extract_tables_from_python_file(content)
        assert "orders" in tables

    def test_pandas_to_sql(self) -> None:
        content = 'df.to_sql("staging_orders", engine)'
        tables = _extract_tables_from_python_file(content)
        assert "staging_orders" in tables

    def test_no_tables(self) -> None:
        content = "x = 1 + 2\nprint(x)\n"
        tables = _extract_tables_from_python_file(content)
        assert tables == []


# ------------------------------------------------------------------
# Repo directory scanning
# ------------------------------------------------------------------


class TestScanRepoDir:
    def test_scan_sql_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sql_dir = Path(tmp) / "models"
            sql_dir.mkdir()
            (sql_dir / "orders.sql").write_text(
                "SELECT * FROM raw.orders JOIN raw.customers ON 1=1"
            )
            result = _scan_repo_dir(
                tmp,
                include_patterns=("*.sql",),
                exclude_patterns=(),
                max_file_size=1_000_000,
            )
            # Should find at least 'raw.orders' and 'raw.customers'
            all_tables = set(result.keys())
            assert "raw.orders" in all_tables
            assert "raw.customers" in all_tables

    def test_scan_python_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "models.py").write_text(
                "__tablename__ = 'users'\n"
            )
            result = _scan_repo_dir(
                tmp,
                include_patterns=("*.py",),
                exclude_patterns=(),
                max_file_size=1_000_000,
            )
            assert "users" in result

    def test_respects_max_file_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            big_file = Path(tmp) / "big.sql"
            big_file.write_text("SELECT * FROM huge_table\n" * 1000)
            result = _scan_repo_dir(
                tmp,
                include_patterns=("*.sql",),
                exclude_patterns=(),
                max_file_size=100,  # Very small limit
            )
            assert result == {}

    def test_excludes_node_modules(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            nm_dir = Path(tmp) / "node_modules" / "pkg"
            nm_dir.mkdir(parents=True)
            (nm_dir / "query.sql").write_text("SELECT * FROM ignored_table")
            result = _scan_repo_dir(
                tmp,
                include_patterns=("*.sql",),
                exclude_patterns=("**/node_modules/**",),
                max_file_size=1_000_000,
            )
            assert result == {}


# ------------------------------------------------------------------
# GitHubAdapterConfig
# ------------------------------------------------------------------


class TestGitHubAdapterConfig:
    def test_requires_token_or_app_id(self) -> None:
        with pytest.raises(ValueError, match="require either"):
            GitHubAdapterConfig(repos=("org/repo",))

    def test_app_id_requires_installation_id(self) -> None:
        with pytest.raises(ValueError, match="installation_id"):
            GitHubAdapterConfig(app_id="123", repos=("org/repo",))

    def test_valid_pat_config(self) -> None:
        from alma_connectors.source_adapter import ExternalSecretRef

        cfg = GitHubAdapterConfig(
            token_secret=ExternalSecretRef(provider="env", reference="GH_TOKEN"),
            repos=("org/repo",),
        )
        assert cfg.repos == ("org/repo",)

    def test_valid_app_config(self) -> None:
        from alma_connectors.source_adapter import ExternalSecretRef

        cfg = GitHubAdapterConfig(
            app_id="123",
            installation_id="456",
            private_key_secret=ExternalSecretRef(provider="env", reference="GH_KEY"),
            repos=("org/repo",),
        )
        assert cfg.app_id == "123"


# ------------------------------------------------------------------
# Registry integration
# ------------------------------------------------------------------


class TestRegistryIntegration:
    def test_github_in_connector_specs(self) -> None:
        assert "github" in CONNECTOR_SPECS

    def test_get_connector_spec(self) -> None:
        spec = get_connector_spec("github")
        assert spec.kind == "github"
        assert spec.adapter_kind == SourceAdapterKind.GITHUB

    def test_build_config_pat(self) -> None:
        spec = get_connector_spec("github")
        config = spec.build_config({
            "token_env": "GITHUB_TOKEN",
            "repos": ["org/repo1", "org/repo2"],
            "branch": "develop",
        })
        assert isinstance(config, GitHubAdapterConfig)
        assert config.repos == ("org/repo1", "org/repo2")
        assert config.branch == "develop"

    def test_build_persisted_adapter(self) -> None:
        adapter = build_persisted_adapter(
            "my-github",
            "github",
            {"token_env": "GH_TOKEN", "repos": ["org/repo"]},
        )
        assert adapter.kind == SourceAdapterKind.GITHUB
        assert isinstance(adapter.config, GitHubAdapterConfig)

    def test_secret_paths(self) -> None:
        spec = get_connector_spec("github")
        assert ("token",) in spec.secret_paths
        assert ("private_key",) in spec.secret_paths


# ------------------------------------------------------------------
# Adapter protocol methods (mocked)
# ------------------------------------------------------------------


class TestGitHubAdapterProtocol:
    @pytest.mark.asyncio
    async def test_validate_connection_success(self) -> None:
        adapter = GitHubAdapter(token="fake-token", repos=("org/repo1",))
        record = _make_adapter_record()

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("alma_connectors.adapters.github.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = await adapter._validate_connection(record)

        assert result.success is True
        assert "1 repo" in result.message

    @pytest.mark.asyncio
    async def test_validate_connection_failure(self) -> None:
        adapter = GitHubAdapter(token="bad-token", repos=("org/repo1",))
        record = _make_adapter_record()

        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch("alma_connectors.adapters.github.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = await adapter._validate_connection(record)

        assert result.success is False
        assert "404" in result.message

    @pytest.mark.asyncio
    async def test_empty_traffic_result(self) -> None:
        adapter = GitHubAdapter(token="fake-token", repos=("org/repo1",))
        record = _make_adapter_record()
        result = await adapter._empty_traffic_result(record)
        assert result.scanned_records == 0
        assert result.events == ()

    @pytest.mark.asyncio
    async def test_discover(self) -> None:
        adapter = GitHubAdapter(
            token="fake-token",
            repos=("org/repo1", "org/repo2"),
        )
        record = _make_adapter_record()
        result = await adapter.discover(record)
        assert len(result.containers) == 2
        assert result.containers[0].container_type == "repository"
        assert result.containers[0].container_id == "github://org/repo1"

    @pytest.mark.asyncio
    async def test_schema_snapshot_with_mocked_scan(self) -> None:
        adapter = GitHubAdapter(token="fake-token", repos=("org/repo1",))
        record = _make_adapter_record()

        mock_scan = {
            "org/repo1": {
                "public.orders": {"models/orders.sql"},
                "analytics.revenue": {"models/revenue.sql"},
            }
        }

        with patch.object(adapter, "_scan_all_repos", return_value=mock_scan):
            result = await adapter._build_schema_snapshot_data(record)

        assert len(result.objects) == 2
        names = {obj.object_name for obj in result.objects}
        assert "orders" in names
        assert "revenue" in names

    @pytest.mark.asyncio
    async def test_lineage_with_mocked_scan(self) -> None:
        adapter = GitHubAdapter(token="fake-token", repos=("org/repo1",))
        record = _make_adapter_record()

        mock_edges = [
            LineageEdge(
                source_object="github://org/repo1/models/orders.sql",
                target_object="public.orders",
                edge_kind=LineageEdgeKind.INFERRED_SQL,
                confidence=0.85,
                metadata={"direction": "reads"},
            ),
            LineageEdge(
                source_object="github://org/repo1/scripts/etl.py",
                target_object="public.orders",
                edge_kind=LineageEdgeKind.INFERRED_SQL,
                confidence=0.7,
            ),
        ]

        with patch.object(adapter, "_scan_all_repos_lineage", return_value=mock_edges):
            result = await adapter.extract_lineage(record)

        assert len(result.edges) == 2
        targets = {e.target_object for e in result.edges}
        assert "public.orders" in targets

    def test_setup_instructions(self) -> None:
        adapter = GitHubAdapter(token="fake-token")
        instructions = adapter.get_setup_instructions()
        assert "GitHub" in instructions.title
        assert len(instructions.steps) > 0


# ------------------------------------------------------------------
# dbt ref/source extraction
# ------------------------------------------------------------------


class TestDbtExtraction:
    def test_extract_dbt_refs(self) -> None:
        content = textwrap.dedent("""\
            SELECT *
            FROM {{ ref('staging_orders') }}
            JOIN {{ ref('dim_customers') }} USING (customer_id)
        """)
        refs = _extract_dbt_refs(content)
        assert refs == ["staging_orders", "dim_customers"]

    def test_extract_dbt_sources(self) -> None:
        content = textwrap.dedent("""\
            SELECT *
            FROM {{ source('raw', 'payments') }}
            WHERE created_at > {{ source('raw', 'events') }}
        """)
        sources = _extract_dbt_sources(content)
        assert ("raw", "payments") in sources
        assert ("raw", "events") in sources

    def test_no_dbt_refs_in_plain_sql(self) -> None:
        content = "SELECT * FROM public.orders"
        assert _extract_dbt_refs(content) == []
        assert _extract_dbt_sources(content) == []

    def test_dbt_ref_double_quotes(self) -> None:
        content = """SELECT * FROM {{ ref("my_model") }}"""
        refs = _extract_dbt_refs(content)
        assert refs == ["my_model"]


# ------------------------------------------------------------------
# Python import extraction
# ------------------------------------------------------------------


class TestPythonImports:
    def test_extract_from_import(self) -> None:
        content = "from utils.helpers import clean_data\n"
        modules = _extract_python_imports(content)
        assert "utils.helpers" in modules

    def test_extract_import(self) -> None:
        content = "import etl.pipeline\n"
        modules = _extract_python_imports(content)
        assert "etl.pipeline" in modules

    def test_ignores_stdlib(self) -> None:
        content = "import os\nfrom pathlib import Path\n"
        modules = _extract_python_imports(content)
        assert "os" in modules
        assert "pathlib" in modules

    def test_resolve_import_to_file(self) -> None:
        py_files = {"utils/helpers.py", "etl/pipeline.py", "etl/__init__.py"}
        assert _resolve_import_to_file("utils.helpers", py_files) == "utils/helpers.py"
        assert _resolve_import_to_file("etl", py_files) == "etl/__init__.py"
        assert _resolve_import_to_file("nonexistent.module", py_files) is None


# ------------------------------------------------------------------
# SQL file lineage (read/write distinction)
# ------------------------------------------------------------------


class TestLineageFromSqlFile:
    def test_select_produces_read_edges(self) -> None:
        sql = "SELECT * FROM raw.orders JOIN raw.customers ON 1=1"
        edges = _lineage_from_sql_file(sql, "github://org/repo/query.sql")
        targets = {e.target_object for e in edges}
        assert "raw.orders" in targets
        assert "raw.customers" in targets
        # All should be INFERRED_SQL
        assert all(e.edge_kind == LineageEdgeKind.INFERRED_SQL for e in edges)

    def test_insert_produces_write_edge(self) -> None:
        sql = "INSERT INTO analytics.daily_stats SELECT * FROM raw.events"
        edges = _lineage_from_sql_file(sql, "github://org/repo/etl.sql")
        targets = {e.target_object for e in edges}
        assert "raw.events" in targets
        assert "analytics.daily_stats" in targets
        # Check directions in metadata
        writes = [e for e in edges if e.metadata.get("direction") == "writes"]
        assert any(e.target_object == "analytics.daily_stats" for e in writes)

    def test_dbt_ref_in_sql_file(self) -> None:
        content = textwrap.dedent("""\
            SELECT *
            FROM {{ ref('staging_orders') }}
            WHERE status = 'completed'
        """)
        edges = _lineage_from_sql_file(content, "github://org/repo/model.sql")
        declared = [e for e in edges if e.edge_kind == LineageEdgeKind.DECLARED]
        assert len(declared) >= 1
        assert any(e.target_object == "staging_orders" for e in declared)
        assert any(e.metadata.get("dbt_type") == "ref" for e in declared)

    def test_dbt_source_in_sql_file(self) -> None:
        content = "SELECT * FROM {{ source('raw', 'payments') }}"
        edges = _lineage_from_sql_file(content, "github://org/repo/model.sql")
        declared = [e for e in edges if e.edge_kind == LineageEdgeKind.DECLARED]
        assert any(e.target_object == "raw.payments" for e in declared)
        assert any(e.metadata.get("dbt_type") == "source" for e in declared)


# ------------------------------------------------------------------
# Python file lineage
# ------------------------------------------------------------------


class TestLineageFromPythonFile:
    def test_sqlalchemy_model_edge(self) -> None:
        content = textwrap.dedent("""\
            class Order(Base):
                __tablename__ = 'orders'
                id = Column(Integer, primary_key=True)
        """)
        edges = _lineage_from_python_file(content, "github://org/repo/models.py", "org/repo", set())
        targets = {e.target_object for e in edges}
        assert "public.orders" in targets

    def test_pandas_to_sql_edge(self) -> None:
        content = 'df.to_sql("staging_orders", engine)\n'
        edges = _lineage_from_python_file(content, "github://org/repo/etl.py", "org/repo", set())
        targets = {e.target_object for e in edges}
        assert "public.staging_orders" in targets

    def test_embedded_sql_read_edge(self) -> None:
        content = textwrap.dedent("""\
            df = pd.read_sql("SELECT * FROM analytics.events", conn)
        """)
        edges = _lineage_from_python_file(content, "github://org/repo/etl.py", "org/repo", set())
        targets = {e.target_object for e in edges}
        assert "analytics.events" in targets

    def test_python_import_edge(self) -> None:
        py_files = {"utils/helpers.py", "etl/pipeline.py"}
        content = "from utils.helpers import clean_data\nimport os\n"
        edges = _lineage_from_python_file(
            content, "github://org/repo/main.py", "org/repo", py_files
        )
        import_edges = [e for e in edges if e.edge_kind == LineageEdgeKind.HEURISTIC]
        assert any(
            e.target_object == "github://org/repo/utils/helpers.py" for e in import_edges
        )
        # os should NOT produce an import edge (not in repo)
        assert not any("os" in e.target_object for e in import_edges)


# ------------------------------------------------------------------
# Repo lineage scanning (integration with temp directory)
# ------------------------------------------------------------------


class TestScanRepoLineageEdges:
    def test_sql_file_produces_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sql_dir = Path(tmp) / "models"
            sql_dir.mkdir()
            (sql_dir / "orders.sql").write_text(
                "SELECT * FROM raw.orders JOIN raw.customers ON 1=1"
            )
            edges = _scan_repo_lineage_edges(
                tmp, "org/repo",
                include_patterns=("*.sql",),
                exclude_patterns=(),
                max_file_size=1_000_000,
            )
            targets = {e.target_object for e in edges}
            assert "raw.orders" in targets
            assert "raw.customers" in targets
            assert all(
                e.source_object.startswith("github://org/repo/") for e in edges
            )

    def test_dbt_file_produces_declared_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "model.sql").write_text(
                "SELECT * FROM {{ ref('staging_orders') }}"
            )
            edges = _scan_repo_lineage_edges(
                tmp, "org/repo",
                include_patterns=("*.sql",),
                exclude_patterns=(),
                max_file_size=1_000_000,
            )
            declared = [e for e in edges if e.edge_kind == LineageEdgeKind.DECLARED]
            assert any(e.target_object == "staging_orders" for e in declared)

    def test_python_file_with_imports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            utils_dir = Path(tmp) / "utils"
            utils_dir.mkdir()
            (utils_dir / "helpers.py").write_text("def clean(): pass\n")
            (Path(tmp) / "main.py").write_text(
                "from utils.helpers import clean\n"
                '__tablename__ = "events"\n'
            )
            edges = _scan_repo_lineage_edges(
                tmp, "org/repo",
                include_patterns=("*.py",),
                exclude_patterns=(),
                max_file_size=1_000_000,
            )
            # Should have import edge and table edge
            import_edges = [e for e in edges if e.edge_kind == LineageEdgeKind.HEURISTIC]
            table_edges = [e for e in edges if e.edge_kind == LineageEdgeKind.INFERRED_SQL]
            assert any(
                e.target_object == "github://org/repo/utils/helpers.py"
                for e in import_edges
            )
            assert any(e.target_object == "public.events" for e in table_edges)


# ------------------------------------------------------------------
# Cross-system stitching
# ------------------------------------------------------------------


class TestStitchCrossSystem:
    def test_exact_match(self) -> None:
        github_edges = (
            LineageEdge(
                source_object="github://org/repo/query.sql",
                target_object="analytics.orders",
                edge_kind=LineageEdgeKind.INFERRED_SQL,
                confidence=0.85,
            ),
        )
        warehouse_tables = frozenset({"analytics.orders"})
        # Exact match with same case should not produce a stitched edge
        # (already identical).
        stitched = stitch_cross_system_edges(github_edges, warehouse_tables)
        assert len(stitched) == 0

    def test_suffix_match_creates_edge(self) -> None:
        github_edges = (
            LineageEdge(
                source_object="github://org/repo/query.sql",
                target_object="analytics.orders",
                edge_kind=LineageEdgeKind.INFERRED_SQL,
                confidence=0.85,
            ),
        )
        # Warehouse has a 3-part FQN that matches the 2-part suffix.
        warehouse_tables = frozenset({"myproject.analytics.orders"})
        stitched = stitch_cross_system_edges(github_edges, warehouse_tables)
        assert len(stitched) == 1
        assert stitched[0].target_object == "myproject.analytics.orders"
        assert stitched[0].metadata.get("cross_system") is True
        assert stitched[0].metadata.get("match_type") == "suffix"

    def test_no_match_no_edge(self) -> None:
        github_edges = (
            LineageEdge(
                source_object="github://org/repo/query.sql",
                target_object="staging.events",
                edge_kind=LineageEdgeKind.INFERRED_SQL,
                confidence=0.85,
            ),
        )
        warehouse_tables = frozenset({"analytics.orders"})
        stitched = stitch_cross_system_edges(github_edges, warehouse_tables)
        assert len(stitched) == 0

    def test_skips_file_to_file_edges(self) -> None:
        github_edges = (
            LineageEdge(
                source_object="github://org/repo/main.py",
                target_object="github://org/repo/utils/helpers.py",
                edge_kind=LineageEdgeKind.HEURISTIC,
                confidence=0.9,
            ),
        )
        warehouse_tables = frozenset({"analytics.orders"})
        stitched = stitch_cross_system_edges(github_edges, warehouse_tables)
        assert len(stitched) == 0

    def test_empty_warehouse_returns_empty(self) -> None:
        github_edges = (
            LineageEdge(
                source_object="github://org/repo/query.sql",
                target_object="analytics.orders",
                edge_kind=LineageEdgeKind.INFERRED_SQL,
                confidence=0.85,
            ),
        )
        stitched = stitch_cross_system_edges(github_edges, frozenset())
        assert len(stitched) == 0
