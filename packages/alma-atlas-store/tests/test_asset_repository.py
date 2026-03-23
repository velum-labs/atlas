"""Tests for AssetRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.asset_repository import Asset


def test_upsert_creates_new_asset(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    result = asset_repo.get(sample_asset.id)
    assert result is not None
    assert result.id == sample_asset.id
    assert result.source == sample_asset.source
    assert result.kind == sample_asset.kind
    assert result.name == sample_asset.name


def test_upsert_updates_existing_asset(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    updated = Asset(
        id=sample_asset.id,
        source="snowflake",
        kind="view",
        name="Updated Name",
        description="Updated desc",
    )
    asset_repo.upsert(updated)
    result = asset_repo.get(sample_asset.id)
    assert result.source == "snowflake"
    assert result.name == "Updated Name"
    assert result.description == "Updated desc"


def test_get_returns_none_for_missing(asset_repo):
    assert asset_repo.get("nonexistent.id") is None


def test_get_returns_asset_by_id(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    result = asset_repo.get(sample_asset.id)
    assert result.id == sample_asset.id


def test_list_all_returns_all_assets(asset_repo, sample_asset, sample_asset_b):
    asset_repo.upsert(sample_asset)
    asset_repo.upsert(sample_asset_b)
    all_assets = asset_repo.list_all()
    ids = {a.id for a in all_assets}
    assert sample_asset.id in ids
    assert sample_asset_b.id in ids


def test_list_all_empty(asset_repo):
    assert asset_repo.list_all() == []


def test_search_finds_by_id_substring(asset_repo, sample_asset, sample_asset_b):
    asset_repo.upsert(sample_asset)
    asset_repo.upsert(sample_asset_b)
    results = asset_repo.search("table_a")
    assert len(results) == 1
    assert results[0].id == sample_asset.id


def test_search_finds_by_name_substring(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    results = asset_repo.search("Table A")
    assert any(r.id == sample_asset.id for r in results)


def test_search_finds_by_description(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    results = asset_repo.search("sample table")
    assert any(r.id == sample_asset.id for r in results)


def test_search_returns_empty_for_no_match(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    results = asset_repo.search("zzz_no_match_zzz")
    assert results == []


def test_delete_removes_asset(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    asset_repo.delete(sample_asset.id)
    assert asset_repo.get(sample_asset.id) is None


def test_delete_nonexistent_is_noop(asset_repo):
    asset_repo.delete("does.not.exist")  # should not raise


def test_delete_cascades_to_edges(db, asset_repo, sample_asset, sample_asset_b):
    from alma_atlas_store.edge_repository import Edge, EdgeRepository

    asset_repo.upsert(sample_asset)
    asset_repo.upsert(sample_asset_b)
    edge_repo = EdgeRepository(db)
    edge = Edge(upstream_id=sample_asset.id, downstream_id=sample_asset_b.id, kind="reads")
    edge_repo.upsert(edge)

    asset_repo.delete(sample_asset.id)

    remaining_edges = edge_repo.list_all()
    assert not any(e.upstream_id == sample_asset.id for e in remaining_edges)


def test_json_roundtrip_tags(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    result = asset_repo.get(sample_asset.id)
    assert result.tags == sample_asset.tags


def test_json_roundtrip_metadata(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    result = asset_repo.get(sample_asset.id)
    assert result.metadata == sample_asset.metadata


def test_timestamps_set_on_insert(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)
    result = asset_repo.get(sample_asset.id)
    assert result.first_seen is not None
    assert result.last_seen is not None


@pytest.mark.parametrize("query,field_value", [
    ("table_a", "project.dataset.table_a"),
    ("Table A", "Table A"),
    ("sample table", "A sample table"),
])
def test_search_parametrized(asset_repo, sample_asset, query, field_value):
    asset_repo.upsert(sample_asset)
    results = asset_repo.search(query)
    assert len(results) >= 1
