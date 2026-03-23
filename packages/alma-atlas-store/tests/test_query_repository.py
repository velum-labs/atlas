"""Tests for QueryRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.query_repository import QueryObservation


def test_upsert_creates_query(query_repo, sample_query):
    query_repo.upsert(sample_query)
    result = query_repo.get_by_fingerprint(sample_query.fingerprint)
    assert result is not None
    assert result.fingerprint == sample_query.fingerprint
    assert result.sql_text == sample_query.sql_text


def test_upsert_increments_execution_count(query_repo, sample_query):
    query_repo.upsert(sample_query)
    query_repo.upsert(sample_query)
    result = query_repo.get_by_fingerprint(sample_query.fingerprint)
    assert result.execution_count == 2


def test_upsert_triple_increments(query_repo, sample_query):
    for _ in range(3):
        query_repo.upsert(sample_query)
    result = query_repo.get_by_fingerprint(sample_query.fingerprint)
    assert result.execution_count == 3


def test_get_by_fingerprint_returns_none_for_missing(query_repo):
    assert query_repo.get_by_fingerprint("nonexistent") is None


def test_list_for_asset_returns_matching(query_repo, sample_query):
    query_repo.upsert(sample_query)
    results = query_repo.list_for_asset("project.dataset.table_a")
    assert any(q.fingerprint == sample_query.fingerprint for q in results)


def test_list_for_asset_excludes_non_matching(query_repo, sample_query):
    query_repo.upsert(sample_query)
    results = query_repo.list_for_asset("project.other.table_z")
    assert results == []


def test_list_all_returns_all(query_repo):
    q1 = QueryObservation(fingerprint="fp1", sql_text="SELECT 1", tables=["a.b.c"], source="pg")
    q2 = QueryObservation(fingerprint="fp2", sql_text="SELECT 2", tables=["a.b.d"], source="pg")
    query_repo.upsert(q1)
    query_repo.upsert(q2)
    all_q = query_repo.list_all()
    assert len(all_q) == 2


def test_list_all_ordered_by_execution_count(query_repo):
    q1 = QueryObservation(fingerprint="fp1", sql_text="SELECT 1", tables=[], source="pg")
    q2 = QueryObservation(fingerprint="fp2", sql_text="SELECT 2", tables=[], source="pg")
    query_repo.upsert(q1)
    query_repo.upsert(q1)
    query_repo.upsert(q2)
    results = query_repo.list_all()
    assert results[0].fingerprint == "fp1"
    assert results[0].execution_count == 2


def test_tables_json_roundtrip(query_repo):
    q = QueryObservation(
        fingerprint="fp_rt",
        sql_text="SELECT a, b FROM t",
        tables=["schema.table_x", "schema.table_y"],
        source="snowflake",
    )
    query_repo.upsert(q)
    result = query_repo.get_by_fingerprint("fp_rt")
    assert result.tables == ["schema.table_x", "schema.table_y"]


def test_timestamps_set(query_repo, sample_query):
    query_repo.upsert(sample_query)
    result = query_repo.get_by_fingerprint(sample_query.fingerprint)
    assert result.first_seen is not None
    assert result.last_seen is not None


def test_list_all_empty(query_repo):
    assert query_repo.list_all() == []


@pytest.mark.parametrize("source", ["bigquery", "snowflake", "postgres", "redshift"])
def test_various_sources(query_repo, source):
    q = QueryObservation(fingerprint=f"fp_{source}", sql_text="SELECT 1", tables=[], source=source)
    query_repo.upsert(q)
    result = query_repo.get_by_fingerprint(f"fp_{source}")
    assert result.source == source
