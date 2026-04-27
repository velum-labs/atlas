"""Shared fixtures for alma-atlas tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "real_e2e: marks tests that hit real external services (Postgres + Anthropic API)",
    )
    config.addinivalue_line(
        "markers",
        "connector_smoke: marks connector smoke tests that hit real Snowflake / dbt trial accounts",
    )

from alma_atlas.config import AtlasConfig, SourceConfig  # noqa: E402
from alma_atlas_store.db import Database  # noqa: E402


@pytest.fixture
def tmp_config_dir(tmp_path: Path) -> Path:
    """Return a temporary directory for config files."""
    return tmp_path / "alma"


@pytest.fixture
def cfg(tmp_config_dir: Path) -> AtlasConfig:
    """Return an AtlasConfig pointing at a temporary directory."""
    return AtlasConfig(config_dir=tmp_config_dir)


@pytest.fixture
def db(tmp_config_dir: Path):
    """Return an in-memory Atlas Database."""
    with Database(":memory:") as database:
        yield database


@pytest.fixture
def cfg_with_db(tmp_config_dir: Path, tmp_path: Path) -> AtlasConfig:
    """Return an AtlasConfig that points to a real (on-disk) SQLite DB."""
    db_path = tmp_path / "atlas.db"
    c = AtlasConfig(config_dir=tmp_config_dir, db_path=db_path)
    # create the db so it exists on disk
    with Database(db_path):
        pass
    return c


@pytest.fixture
def pg_source() -> SourceConfig:
    return SourceConfig(id="postgres:mydb", kind="postgres", params={"dsn_env": "PG_DATABASE_URL"})


@pytest.fixture
def bq_source() -> SourceConfig:
    return SourceConfig(
        id="bigquery:my-project",
        kind="bigquery",
        params={"project_id": "my-project", "service_account_env": "BQ_SA"},
    )


@pytest.fixture
def mock_schema_snapshot():
    """Return a minimal mock SchemaSnapshot."""
    from alma_connectors.source_adapter import SchemaObjectKind, SchemaSnapshot, SourceTableSchema

    obj = SourceTableSchema(
        schema_name="public",
        object_name="orders",
        object_kind=SchemaObjectKind.TABLE,
        columns=(),
    )
    return SchemaSnapshot(captured_at=None, objects=(obj,), dependencies=())


@pytest.fixture
def mock_traffic_result():
    """Return a minimal mock TrafficObservationResult with one SQL event."""
    from datetime import UTC, datetime

    from alma_connectors.source_adapter import ObservedQueryEvent, TrafficObservationResult

    event = ObservedQueryEvent(
        captured_at=datetime.now(UTC),
        sql="SELECT id FROM public.orders",
        source_name="test-source",
        query_type="SELECT",
        database_user="analyst",
    )
    return TrafficObservationResult(scanned_records=1, events=(event,))


@pytest.fixture
def mock_adapter(mock_schema_snapshot, mock_traffic_result):
    """Return a mock SourceAdapter with async introspect/observe."""
    adapter = MagicMock()
    adapter.introspect_schema = AsyncMock(return_value=mock_schema_snapshot)
    adapter.observe_traffic = AsyncMock(return_value=mock_traffic_result)
    return adapter
