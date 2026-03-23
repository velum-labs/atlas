"""Tests for SchemaRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.schema_repository import ColumnInfo, SchemaSnapshot


@pytest.fixture(autouse=True)
def seed_asset(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)


def test_upsert_creates_snapshot(schema_repo, sample_snapshot):
    schema_repo.upsert(sample_snapshot)
    result = schema_repo.get_latest(sample_snapshot.asset_id)
    assert result is not None
    assert result.asset_id == sample_snapshot.asset_id


def test_upsert_is_noop_for_same_fingerprint(schema_repo, sample_snapshot):
    schema_repo.upsert(sample_snapshot)
    schema_repo.upsert(sample_snapshot)
    history = schema_repo.list_history(sample_snapshot.asset_id)
    assert len(history) == 1


def test_upsert_stores_new_fingerprint_separately(schema_repo, sample_asset):
    s1 = SchemaSnapshot(
        asset_id=sample_asset.id,
        columns=[ColumnInfo(name="id", type="STRING")],
    )
    s2 = SchemaSnapshot(
        asset_id=sample_asset.id,
        columns=[ColumnInfo(name="id", type="STRING"), ColumnInfo(name="name", type="STRING")],
    )
    schema_repo.upsert(s1)
    schema_repo.upsert(s2)
    history = schema_repo.list_history(sample_asset.id)
    assert len(history) == 2


def test_get_latest_returns_none_for_missing(schema_repo):
    assert schema_repo.get_latest("does.not.exist") is None


def test_get_latest_returns_snapshot(schema_repo, sample_snapshot):
    schema_repo.upsert(sample_snapshot)
    result = schema_repo.get_latest(sample_snapshot.asset_id)
    assert result.asset_id == sample_snapshot.asset_id


def test_columns_roundtrip(schema_repo, sample_snapshot):
    schema_repo.upsert(sample_snapshot)
    result = schema_repo.get_latest(sample_snapshot.asset_id)
    assert len(result.columns) == len(sample_snapshot.columns)
    for orig, loaded in zip(sample_snapshot.columns, result.columns, strict=True):
        assert loaded.name == orig.name
        assert loaded.type == orig.type
        assert loaded.nullable == orig.nullable
        assert loaded.description == orig.description


def test_list_history_returns_all_snapshots(schema_repo, sample_asset):
    s1 = SchemaSnapshot(asset_id=sample_asset.id, columns=[ColumnInfo(name="a", type="INT")])
    s2 = SchemaSnapshot(asset_id=sample_asset.id, columns=[ColumnInfo(name="a", type="INT"), ColumnInfo(name="b", type="TEXT")])
    schema_repo.upsert(s1)
    schema_repo.upsert(s2)
    history = schema_repo.list_history(sample_asset.id)
    assert len(history) == 2


def test_list_history_empty(schema_repo, sample_asset):
    assert schema_repo.list_history(sample_asset.id) == []


def test_fingerprint_deterministic(sample_asset):
    s1 = SchemaSnapshot(asset_id=sample_asset.id, columns=[ColumnInfo(name="id", type="STRING")])
    s2 = SchemaSnapshot(asset_id=sample_asset.id, columns=[ColumnInfo(name="id", type="STRING")])
    assert s1.fingerprint == s2.fingerprint


def test_fingerprint_differs_for_different_columns(sample_asset):
    s1 = SchemaSnapshot(asset_id=sample_asset.id, columns=[ColumnInfo(name="id", type="STRING")])
    s2 = SchemaSnapshot(asset_id=sample_asset.id, columns=[ColumnInfo(name="id", type="INTEGER")])
    assert s1.fingerprint != s2.fingerprint


def test_snapshot_id_includes_fingerprint(sample_snapshot):
    assert sample_snapshot.fingerprint in sample_snapshot.id
    assert sample_snapshot.asset_id in sample_snapshot.id


def test_captured_at_set(schema_repo, sample_snapshot):
    schema_repo.upsert(sample_snapshot)
    result = schema_repo.get_latest(sample_snapshot.asset_id)
    assert result.captured_at is not None


@pytest.mark.parametrize("col_type", ["STRING", "INTEGER", "FLOAT", "BOOLEAN", "TIMESTAMP"])
def test_column_types(schema_repo, sample_asset, col_type):
    snapshot = SchemaSnapshot(
        asset_id=sample_asset.id,
        columns=[ColumnInfo(name="col", type=col_type)],
    )
    schema_repo.upsert(snapshot)
    result = schema_repo.get_latest(sample_asset.id)
    assert result.columns[0].type == col_type
