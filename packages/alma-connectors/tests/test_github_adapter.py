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
    _extract_tables_from_python_file,
    _extract_tables_from_sql_file,
    _matches_patterns,
    _scan_repo_dir,
)
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

        mock_scan = {
            "org/repo1": {
                "public.orders": {"models/orders.sql", "scripts/etl.py"},
            }
        }

        with patch.object(adapter, "_scan_all_repos", return_value=mock_scan):
            result = await adapter.extract_lineage(record)

        assert len(result.edges) == 2
        targets = {e.target_object for e in result.edges}
        assert "public.orders" in targets

    def test_setup_instructions(self) -> None:
        adapter = GitHubAdapter(token="fake-token")
        instructions = adapter.get_setup_instructions()
        assert "GitHub" in instructions.title
        assert len(instructions.steps) > 0
