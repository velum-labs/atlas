"""Shared fixtures for alma-atlas-store tests."""

from __future__ import annotations

import pytest

from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.consumer_repository import Consumer, ConsumerRepository
from alma_atlas_store.contract_repository import Contract, ContractRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.query_repository import QueryObservation, QueryRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot


@pytest.fixture
def db():
    with Database(":memory:") as database:
        yield database


@pytest.fixture
def asset_repo(db):
    return AssetRepository(db)


@pytest.fixture
def edge_repo(db):
    return EdgeRepository(db)


@pytest.fixture
def query_repo(db):
    return QueryRepository(db)


@pytest.fixture
def consumer_repo(db):
    return ConsumerRepository(db)


@pytest.fixture
def contract_repo(db):
    return ContractRepository(db)


@pytest.fixture
def schema_repo(db):
    return SchemaRepository(db)


@pytest.fixture
def sample_asset():
    return Asset(
        id="project.dataset.table_a",
        source="bigquery",
        kind="table",
        name="Table A",
        description="A sample table",
        tags=["pii", "analytics"],
        metadata={"owner": "team-data"},
    )


@pytest.fixture
def sample_asset_b():
    return Asset(
        id="project.dataset.table_b",
        source="bigquery",
        kind="view",
        name="View B",
        description="A sample view",
        tags=["reporting"],
        metadata={},
    )


@pytest.fixture
def sample_edge(sample_asset, sample_asset_b):
    return Edge(
        upstream_id=sample_asset.id,
        downstream_id=sample_asset_b.id,
        kind="reads",
        metadata={"via": "dbt"},
    )


@pytest.fixture
def sample_query():
    return QueryObservation(
        fingerprint="abc123",
        sql_text="SELECT * FROM project.dataset.table_a",
        tables=["project.dataset.table_a"],
        source="bigquery",
    )


@pytest.fixture
def sample_consumer():
    return Consumer(
        id="consumer-1",
        kind="dashboard",
        name="Sales Dashboard",
        source="looker",
        asset_ids=["project.dataset.table_a"],
        metadata={"url": "https://looker.example.com/1"},
    )


@pytest.fixture
def sample_contract(sample_asset):
    return Contract(
        id="contract-1",
        asset_id=sample_asset.id,
        version="1.0.0",
        spec={"columns": [{"name": "id", "type": "STRING"}], "sla": "99.9%"},
        status="active",
    )


@pytest.fixture
def sample_snapshot(sample_asset):
    return SchemaSnapshot(
        asset_id=sample_asset.id,
        columns=[
            ColumnInfo(name="id", type="STRING", nullable=False),
            ColumnInfo(name="value", type="INTEGER", nullable=True, description="A value"),
        ],
    )
