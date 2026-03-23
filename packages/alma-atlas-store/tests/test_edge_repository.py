"""Tests for EdgeRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.edge_repository import Edge


@pytest.fixture(autouse=True)
def seed_assets(asset_repo, sample_asset, sample_asset_b):
    """Edges FK-reference assets; ensure they exist for all edge tests."""
    asset_repo.upsert(sample_asset)
    asset_repo.upsert(sample_asset_b)


def test_upsert_creates_edge(edge_repo, sample_edge):
    edge_repo.upsert(sample_edge)
    all_edges = edge_repo.list_all()
    assert len(all_edges) == 1
    e = all_edges[0]
    assert e.upstream_id == sample_edge.upstream_id
    assert e.downstream_id == sample_edge.downstream_id
    assert e.kind == sample_edge.kind


def test_upsert_updates_existing_edge(edge_repo, sample_edge):
    edge_repo.upsert(sample_edge)
    # Same composite key (upstream, downstream, kind) — should update, not duplicate
    edge_repo.upsert(sample_edge)
    assert len(edge_repo.list_all()) == 1


def test_edge_id_property(sample_edge):
    assert sample_edge.id == f"{sample_edge.upstream_id}:{sample_edge.downstream_id}:{sample_edge.kind}"


def test_get_upstream(edge_repo, sample_edge, sample_asset_b):
    edge_repo.upsert(sample_edge)
    upstream = edge_repo.get_upstream(sample_asset_b.id)
    assert len(upstream) == 1
    assert upstream[0].upstream_id == sample_edge.upstream_id


def test_get_upstream_empty(edge_repo, sample_asset):
    assert edge_repo.get_upstream(sample_asset.id) == []


def test_get_downstream(edge_repo, sample_edge, sample_asset):
    edge_repo.upsert(sample_edge)
    downstream = edge_repo.get_downstream(sample_asset.id)
    assert len(downstream) == 1
    assert downstream[0].downstream_id == sample_edge.downstream_id


def test_get_downstream_empty(edge_repo, sample_asset_b):
    assert edge_repo.get_downstream(sample_asset_b.id) == []


def test_list_all_returns_all(edge_repo, sample_asset, sample_asset_b):
    e1 = Edge(upstream_id=sample_asset.id, downstream_id=sample_asset_b.id, kind="reads")
    e2 = Edge(upstream_id=sample_asset.id, downstream_id=sample_asset_b.id, kind="writes")
    edge_repo.upsert(e1)
    edge_repo.upsert(e2)
    assert len(edge_repo.list_all()) == 2


def test_list_all_empty(edge_repo):
    assert edge_repo.list_all() == []


def test_metadata_roundtrip(edge_repo, sample_edge):
    edge_repo.upsert(sample_edge)
    result = edge_repo.list_all()[0]
    assert result.metadata == sample_edge.metadata


def test_timestamps_set(edge_repo, sample_edge):
    edge_repo.upsert(sample_edge)
    result = edge_repo.list_all()[0]
    assert result.first_seen is not None
    assert result.last_seen is not None


@pytest.mark.parametrize("kind", ["reads", "writes", "depends_on", "triggers"])
def test_edge_kinds(edge_repo, sample_asset, sample_asset_b, kind):
    edge = Edge(upstream_id=sample_asset.id, downstream_id=sample_asset_b.id, kind=kind)
    edge_repo.upsert(edge)
    found = edge_repo.get_downstream(sample_asset.id)
    assert any(e.kind == kind for e in found)
