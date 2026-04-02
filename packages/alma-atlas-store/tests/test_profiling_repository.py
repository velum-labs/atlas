"""Tests for ProfilingRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.db import Database
from alma_atlas_store.profiling_repository import ProfilingRepository
from alma_ports.profiling import ColumnProfile


@pytest.fixture
def db():
    with Database(":memory:") as database:
        yield database


@pytest.fixture
def repo(db):
    return ProfilingRepository(db)


@pytest.fixture
def sample_profile():
    return ColumnProfile(
        asset_id="sqlite-test/orders",
        column_name="amount",
        distinct_count=42,
        null_count=3,
        null_fraction=0.06,
        min_value="0.5",
        max_value="999.99",
        top_values=[{"value": "10.0", "count": 15}, {"value": "5.0", "count": 10}],
        sample_values=[],
        profiled_at="2026-04-02T12:00:00+00:00",
    )


def test_upsert_and_get(repo, sample_profile):
    repo.upsert(sample_profile)
    result = repo.get(sample_profile.asset_id, sample_profile.column_name)
    assert result is not None
    assert result.asset_id == sample_profile.asset_id
    assert result.column_name == sample_profile.column_name


def test_get_returns_none_for_missing(repo):
    assert repo.get("no-such/asset", "no_col") is None


def test_upsert_replaces_existing(repo, sample_profile):
    repo.upsert(sample_profile)
    updated = ColumnProfile(
        asset_id=sample_profile.asset_id,
        column_name=sample_profile.column_name,
        distinct_count=99,
        null_count=0,
        null_fraction=0.0,
    )
    repo.upsert(updated)
    result = repo.get(sample_profile.asset_id, sample_profile.column_name)
    assert result is not None
    assert result.distinct_count == 99
    assert result.null_count == 0


def test_numeric_fields_roundtrip(repo, sample_profile):
    repo.upsert(sample_profile)
    result = repo.get(sample_profile.asset_id, sample_profile.column_name)
    assert result.distinct_count == 42
    assert result.null_count == 3
    assert abs(result.null_fraction - 0.06) < 1e-9
    assert result.min_value == "0.5"
    assert result.max_value == "999.99"
    assert result.profiled_at == "2026-04-02T12:00:00+00:00"


def test_top_values_roundtrip(repo, sample_profile):
    repo.upsert(sample_profile)
    result = repo.get(sample_profile.asset_id, sample_profile.column_name)
    assert result.top_values == [{"value": "10.0", "count": 15}, {"value": "5.0", "count": 10}]


def test_sample_values_roundtrip(repo):
    profile = ColumnProfile(
        asset_id="sqlite-test/events",
        column_name="created_at",
        sample_values=["2024-01-01", "2024-01-02", "2024-01-03"],
    )
    repo.upsert(profile)
    result = repo.get(profile.asset_id, profile.column_name)
    assert result.sample_values == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_top_values_empty_list_roundtrip(repo):
    profile = ColumnProfile(
        asset_id="sqlite-test/users",
        column_name="id",
        distinct_count=1000,
        top_values=[],
    )
    repo.upsert(profile)
    result = repo.get(profile.asset_id, profile.column_name)
    assert result.top_values == []


def test_nullable_fields_roundtrip(repo):
    profile = ColumnProfile(
        asset_id="sqlite-test/users",
        column_name="name",
    )
    repo.upsert(profile)
    result = repo.get(profile.asset_id, profile.column_name)
    assert result.distinct_count is None
    assert result.null_count is None
    assert result.null_fraction is None
    assert result.min_value is None
    assert result.max_value is None
    assert result.profiled_at is None


def test_list_for_asset_returns_all_columns(repo):
    asset_id = "sqlite-test/orders"
    for col in ("amount", "user_id", "status"):
        repo.upsert(ColumnProfile(asset_id=asset_id, column_name=col, distinct_count=5))
    results = repo.list_for_asset(asset_id)
    assert len(results) == 3
    assert [r.column_name for r in results] == ["amount", "status", "user_id"]


def test_list_for_asset_empty(repo):
    assert repo.list_for_asset("sqlite-test/nonexistent") == []


def test_list_for_asset_scoped_to_asset(repo):
    repo.upsert(ColumnProfile(asset_id="sqlite-test/table_a", column_name="col1"))
    repo.upsert(ColumnProfile(asset_id="sqlite-test/table_b", column_name="col1"))
    results = repo.list_for_asset("sqlite-test/table_a")
    assert len(results) == 1
    assert results[0].asset_id == "sqlite-test/table_a"
