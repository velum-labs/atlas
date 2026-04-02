"""Tests for BusinessTermRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.business_term_repository import BusinessTermRepository
from alma_atlas_store.db import Database
from alma_ports.business_term import BusinessTerm


@pytest.fixture
def db():
    with Database(":memory:") as database:
        yield database


@pytest.fixture
def repo(db):
    return BusinessTermRepository(db)


def make_term(name: str, **kwargs) -> BusinessTerm:
    return BusinessTerm(name=name, **kwargs)


# ---------------------------------------------------------------------------
# upsert / get round-trip
# ---------------------------------------------------------------------------


def test_upsert_and_get_basic(repo):
    term = make_term("revenue", definition="Total net sales", source="manual")
    repo.upsert(term)
    retrieved = repo.get("revenue")
    assert retrieved is not None
    assert retrieved.name == "revenue"
    assert retrieved.definition == "Total net sales"
    assert retrieved.source == "manual"


def test_upsert_and_get_with_formula_and_columns(repo):
    term = make_term(
        "active_user",
        definition="User who logged in within the last 30 days",
        formula="COUNT(DISTINCT user_id) WHERE last_login > NOW() - INTERVAL 30 DAY",
        referenced_columns=["users.user_id", "users.last_login"],
        source="learned",
    )
    repo.upsert(term)
    retrieved = repo.get("active_user")
    assert retrieved is not None
    assert retrieved.formula is not None
    assert "COUNT" in retrieved.formula
    assert retrieved.referenced_columns == ["users.user_id", "users.last_login"]
    assert retrieved.source == "learned"


def test_upsert_updates_existing(repo):
    repo.upsert(make_term("churn", definition="Original definition"))
    repo.upsert(make_term("churn", definition="Updated definition", formula="churn_rate = X / Y"))
    retrieved = repo.get("churn")
    assert retrieved is not None
    assert retrieved.definition == "Updated definition"
    assert retrieved.formula == "churn_rate = X / Y"


def test_get_nonexistent_returns_none(repo):
    assert repo.get("nonexistent_term") is None


def test_referenced_columns_round_trip(repo):
    cols = ["orders.amount", "orders.status", "customers.id"]
    repo.upsert(make_term("test_metric", referenced_columns=cols))
    retrieved = repo.get("test_metric")
    assert retrieved is not None
    assert retrieved.referenced_columns == cols


def test_empty_referenced_columns(repo):
    repo.upsert(make_term("simple_term"))
    retrieved = repo.get("simple_term")
    assert retrieved is not None
    assert retrieved.referenced_columns == []


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


def test_search_by_name_fragment(repo):
    repo.upsert(make_term("revenue", definition="Total sales"))
    repo.upsert(make_term("revenue_growth", definition="MoM revenue increase"))
    repo.upsert(make_term("churn", definition="Customer attrition rate"))

    results = repo.search("revenue")
    names = [t.name for t in results]
    assert "revenue" in names
    assert "revenue_growth" in names
    assert "churn" not in names


def test_search_by_definition_fragment(repo):
    repo.upsert(make_term("dau", definition="Daily active users count"))
    repo.upsert(make_term("mau", definition="Monthly active users count"))
    repo.upsert(make_term("orders", definition="Purchase transactions"))

    results = repo.search("active users")
    names = [t.name for t in results]
    assert "dau" in names
    assert "mau" in names
    assert "orders" not in names


def test_search_no_match_returns_empty(repo):
    repo.upsert(make_term("revenue", definition="Total sales"))
    results = repo.search("zzz_no_match_zzz")
    assert results == []


def test_search_results_ordered_by_name(repo):
    repo.upsert(make_term("zebra_metric", definition="something"))
    repo.upsert(make_term("alpha_metric", definition="something"))
    repo.upsert(make_term("beta_metric", definition="something"))
    results = repo.search("metric")
    names = [t.name for t in results]
    assert names == sorted(names)


# ---------------------------------------------------------------------------
# list_all
# ---------------------------------------------------------------------------


def test_list_all_returns_all_terms(repo):
    repo.upsert(make_term("term_a"))
    repo.upsert(make_term("term_b"))
    repo.upsert(make_term("term_c"))
    results = repo.list_all()
    names = {t.name for t in results}
    assert {"term_a", "term_b", "term_c"}.issubset(names)


def test_list_all_respects_limit(repo):
    for i in range(10):
        repo.upsert(make_term(f"term_{i:02d}"))
    results = repo.list_all(limit=3)
    assert len(results) == 3


def test_list_all_empty_db(repo):
    assert repo.list_all() == []
