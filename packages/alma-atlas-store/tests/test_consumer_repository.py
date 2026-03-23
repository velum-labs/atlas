"""Tests for ConsumerRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.consumer_repository import Consumer


@pytest.fixture(autouse=True)
def seed_assets(asset_repo, sample_asset, sample_asset_b):
    asset_repo.upsert(sample_asset)
    asset_repo.upsert(sample_asset_b)


def test_upsert_creates_consumer(consumer_repo, sample_consumer):
    consumer_repo.upsert(sample_consumer)
    result = consumer_repo.get(sample_consumer.id)
    assert result is not None
    assert result.id == sample_consumer.id
    assert result.name == sample_consumer.name
    assert result.kind == sample_consumer.kind


def test_upsert_updates_existing_consumer(consumer_repo, sample_consumer):
    consumer_repo.upsert(sample_consumer)
    updated = Consumer(
        id=sample_consumer.id,
        kind="notebook",
        name="Updated Consumer",
        source="jupyter",
        asset_ids=[],
    )
    consumer_repo.upsert(updated)
    result = consumer_repo.get(sample_consumer.id)
    assert result.kind == "notebook"
    assert result.name == "Updated Consumer"


def test_get_returns_none_for_missing(consumer_repo):
    assert consumer_repo.get("nonexistent") is None


def test_get_includes_asset_ids(consumer_repo, sample_consumer):
    consumer_repo.upsert(sample_consumer)
    result = consumer_repo.get(sample_consumer.id)
    assert result.asset_ids == sample_consumer.asset_ids


def test_asset_ids_not_duplicated_on_re_upsert(consumer_repo, sample_consumer):
    consumer_repo.upsert(sample_consumer)
    consumer_repo.upsert(sample_consumer)
    result = consumer_repo.get(sample_consumer.id)
    assert len(result.asset_ids) == len(set(result.asset_ids))


def test_list_for_asset_returns_consumers(consumer_repo, sample_consumer, sample_asset):
    consumer_repo.upsert(sample_consumer)
    results = consumer_repo.list_for_asset(sample_asset.id)
    assert any(c.id == sample_consumer.id for c in results)


def test_list_for_asset_excludes_non_related(consumer_repo, sample_consumer, sample_asset_b):
    consumer_repo.upsert(sample_consumer)
    # sample_consumer only references sample_asset, not sample_asset_b
    results = consumer_repo.list_for_asset(sample_asset_b.id)
    assert not any(c.id == sample_consumer.id for c in results)


def test_list_all_returns_all(consumer_repo):
    c1 = Consumer(id="c1", kind="dashboard", name="D1", source="looker", asset_ids=[])
    c2 = Consumer(id="c2", kind="notebook", name="N2", source="jupyter", asset_ids=[])
    consumer_repo.upsert(c1)
    consumer_repo.upsert(c2)
    all_c = consumer_repo.list_all()
    ids = {c.id for c in all_c}
    assert {"c1", "c2"}.issubset(ids)


def test_list_all_empty(consumer_repo):
    assert consumer_repo.list_all() == []


def test_metadata_roundtrip(consumer_repo, sample_consumer):
    consumer_repo.upsert(sample_consumer)
    result = consumer_repo.get(sample_consumer.id)
    assert result.metadata == sample_consumer.metadata


def test_timestamps_set(consumer_repo, sample_consumer):
    consumer_repo.upsert(sample_consumer)
    result = consumer_repo.get(sample_consumer.id)
    assert result.first_seen is not None
    assert result.last_seen is not None


@pytest.mark.parametrize("kind", ["user", "service", "dashboard", "notebook"])
def test_consumer_kinds(consumer_repo, kind):
    c = Consumer(id=f"c_{kind}", kind=kind, name=f"Consumer {kind}", source="test", asset_ids=[])
    consumer_repo.upsert(c)
    result = consumer_repo.get(f"c_{kind}")
    assert result.kind == kind
