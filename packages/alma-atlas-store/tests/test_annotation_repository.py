"""Tests for AnnotationRepository including FTS5 full-text search."""

from __future__ import annotations

import pytest

from alma_atlas_store.annotation_repository import AnnotationRepository, _build_fts_content
from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.db import Database
from alma_ports.annotation import AnnotationRecord
from alma_ports.asset import Asset


@pytest.fixture
def db():
    with Database(":memory:") as database:
        yield database


@pytest.fixture
def repo(db):
    return AnnotationRepository(db)


def make_record(asset_id: str, **kwargs) -> AnnotationRecord:
    return AnnotationRecord(asset_id=asset_id, **kwargs)


# ---------------------------------------------------------------------------
# _build_fts_content unit tests
# ---------------------------------------------------------------------------


def test_build_fts_content_basic_fields():
    record = AnnotationRecord(
        asset_id="a.b.c",
        business_logic_summary="Tracks daily revenue",
        granularity="one row per day",
        sensitivity="financial",
        ownership="team-finance",
    )
    content = _build_fts_content(record)
    assert "Tracks daily revenue" in content
    assert "one row per day" in content
    assert "financial" in content
    assert "team-finance" in content


def test_build_fts_content_column_notes():
    record = AnnotationRecord(
        asset_id="a.b.c",
        properties={"column_notes": {"Consumption": "tracks EUR currency"}},
    )
    content = _build_fts_content(record)
    assert "Consumption: tracks EUR currency" in content


def test_build_fts_content_nested_properties():
    record = AnnotationRecord(
        asset_id="a.b.c",
        properties={"notes": "extra info"},
    )
    content = _build_fts_content(record)
    assert "extra info" in content


def test_build_fts_content_empty():
    record = AnnotationRecord(asset_id="a.b.c")
    content = _build_fts_content(record)
    assert content == ""


# ---------------------------------------------------------------------------
# FTS search integration tests
# ---------------------------------------------------------------------------


def test_fts_search_by_currency(repo):
    """FTS search for 'currency' finds the asset with column_notes containing it."""
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.a",
        business_logic_summary="Revenue table",
        properties={
            "column_notes": {"Consumption": "tracks EUR currency"},
            "notes": "test",
        },
    ))
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.b",
        business_logic_summary="User dimension",
    ))
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.c",
        business_logic_summary="Session events",
        sensitivity="PII",
    ))

    results = repo.search_fts("currency")
    asset_ids = [r[0] for r in results]
    assert "ds.tbl.a" in asset_ids
    assert "ds.tbl.b" not in asset_ids
    assert "ds.tbl.c" not in asset_ids


def test_fts_search_by_eur(repo):
    """FTS search for 'EUR' finds the asset with EUR in column notes."""
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.a",
        properties={
            "column_notes": {"Consumption": "tracks EUR currency"},
            "notes": "test",
        },
    ))
    repo.upsert(AnnotationRecord(asset_id="ds.tbl.b", business_logic_summary="Unrelated"))
    repo.upsert(AnnotationRecord(asset_id="ds.tbl.c", sensitivity="PII"))

    results = repo.search_fts("EUR")
    asset_ids = [r[0] for r in results]
    assert "ds.tbl.a" in asset_ids
    assert "ds.tbl.b" not in asset_ids


def test_fts_search_nonexistent_returns_empty(repo):
    """FTS search for a term that matches nothing returns an empty list."""
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.a",
        business_logic_summary="Revenue table",
    ))

    results = repo.search_fts("nonexistent_xyzzy_term")
    assert results == []


def test_fts_search_snippet_contains_match(repo):
    """Snippet returned by FTS search contains the matched term."""
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.a",
        business_logic_summary="Tracks EUR currency conversions",
        properties={"column_notes": {"Consumption": "tracks EUR currency"}},
    ))

    results = repo.search_fts("currency")
    assert len(results) >= 1
    asset_id, snippet = results[0]
    assert asset_id == "ds.tbl.a"
    assert "currency" in snippet.lower() or "currenc" in snippet.lower()


def test_fts_upsert_updates_index(repo):
    """Re-upserting an annotation replaces its FTS entry."""
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.a",
        business_logic_summary="Old description about apples",
    ))
    assert repo.search_fts("apples") != []
    assert repo.search_fts("oranges") == []

    # Update the annotation
    repo.upsert(AnnotationRecord(
        asset_id="ds.tbl.a",
        business_logic_summary="New description about oranges",
    ))
    assert repo.search_fts("oranges") != []
    assert repo.search_fts("apples") == []


# ---------------------------------------------------------------------------
# Properties round-trip tests
# ---------------------------------------------------------------------------


def test_properties_round_trip(repo):
    """Properties dict is persisted and retrieved without data loss."""
    props = {"column_notes": {"id": "surrogate key"}, "notes": "test DB"}
    record = AnnotationRecord(
        asset_id="src::schema.orders",
        business_logic_summary="Order fact table",
        properties=props,
    )
    repo.upsert(record)
    result = repo.get("src::schema.orders")
    assert result is not None
    assert result.properties == props
    assert result.properties["column_notes"] == {"id": "surrogate key"}
    assert result.properties["notes"] == "test DB"


def test_properties_default_empty(repo):
    """AnnotationRecord with no properties stores and retrieves an empty dict."""
    repo.upsert(AnnotationRecord(asset_id="src::schema.empty"))
    result = repo.get("src::schema.empty")
    assert result is not None
    assert result.properties == {}


def test_properties_overwritten_on_upsert(repo):
    """Re-upserting an annotation replaces properties."""
    repo.upsert(AnnotationRecord(asset_id="src::t", properties={"notes": "v1"}))
    repo.upsert(AnnotationRecord(asset_id="src::t", properties={"notes": "v2", "column_notes": {"col": "desc"}}))
    result = repo.get("src::t")
    assert result is not None
    assert result.properties["notes"] == "v2"
    assert result.properties["column_notes"] == {"col": "desc"}


# ---------------------------------------------------------------------------
# list_unannotated with source_prefix
# ---------------------------------------------------------------------------


def _seed_asset(db: Database, asset_id: str) -> None:
    source = asset_id.split("::", 1)[0]
    AssetRepository(db).upsert(Asset(id=asset_id, source=source, kind="table", name=asset_id))


def test_list_unannotated_no_prefix_returns_all(db, repo):
    """list_unannotated without source_prefix returns all unannotated assets."""
    _seed_asset(db, "pg:henkel::analytics.revenue")
    _seed_asset(db, "sqlite:bird::frpm")
    unannotated = repo.list_unannotated()
    assert "pg:henkel::analytics.revenue" in unannotated
    assert "sqlite:bird::frpm" in unannotated


def test_list_unannotated_with_prefix_filters_by_source(db, repo):
    """list_unannotated with source_prefix only returns assets from that source."""
    _seed_asset(db, "pg:henkel::analytics.revenue")
    _seed_asset(db, "sqlite:bird::frpm")
    unannotated = repo.list_unannotated(source_prefix="pg:henkel")
    assert "pg:henkel::analytics.revenue" in unannotated
    assert "sqlite:bird::frpm" not in unannotated


def test_list_unannotated_with_prefix_excludes_annotated(db, repo):
    """list_unannotated with source_prefix excludes already-annotated assets."""
    _seed_asset(db, "pg:henkel::analytics.revenue")
    _seed_asset(db, "pg:henkel::analytics.users")
    repo.upsert(AnnotationRecord(asset_id="pg:henkel::analytics.revenue"))
    unannotated = repo.list_unannotated(source_prefix="pg:henkel")
    assert "pg:henkel::analytics.revenue" not in unannotated
    assert "pg:henkel::analytics.users" in unannotated


def test_list_unannotated_with_prefix_no_match_returns_empty(db, repo):
    """list_unannotated with unknown source_prefix returns empty list."""
    _seed_asset(db, "pg:henkel::analytics.revenue")
    unannotated = repo.list_unannotated(source_prefix="nonexistent")
    assert unannotated == []
