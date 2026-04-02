"""Tests for atlas_verify and upgraded atlas_find_term MCP handlers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp.tools import _handle_define_term, _handle_find_term, _handle_verify
from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.business_term_repository import BusinessTermRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_ports.annotation import AnnotationRecord
from alma_ports.business_term import BusinessTerm


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    with Database(db_path):
        pass
    return cfg


def _seed_assets(db_path: Path) -> None:
    with Database(db_path) as db:
        repo = AssetRepository(db)
        repo.upsert(Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders"))
        repo.upsert(Asset(id="pg::public.customers", source="pg:test", kind="table", name="public.customers"))


# ---------------------------------------------------------------------------
# atlas_verify tests
# ---------------------------------------------------------------------------


class TestAtlasVerify:
    def test_valid_simple_query_no_warnings(self, tmp_path):
        """A simple query with no annotated tables returns valid=True and no warnings."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)

        result_texts = _handle_verify(cfg, {"sql": "SELECT * FROM public.orders"})
        assert len(result_texts) == 1
        data = json.loads(result_texts[0].text)
        assert data["valid"] is True
        assert data["warnings"] == []

    def test_join_with_guidance_warning(self, tmp_path):
        """A query joining tables whose edge has join_guidance emits that guidance as a warning."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)

        with Database(cfg.db_path) as db:
            EdgeRepository(db).upsert(
                Edge(
                    upstream_id="pg::public.orders",
                    downstream_id="pg::public.customers",
                    kind="fk",
                    metadata={"join_guidance": "Always filter by tenant_id before joining."},
                )
            )

        sql = "SELECT * FROM public.orders o JOIN public.customers c ON o.customer_id = c.id"
        result_texts = _handle_verify(cfg, {"sql": sql})
        data = json.loads(result_texts[0].text)

        assert data["valid"] is False
        assert len(data["warnings"]) >= 1
        assert any("tenant_id" in w for w in data["warnings"])

    def test_sum_on_surrogate_key_column_emits_warning(self, tmp_path):
        """SUM on a column annotated as surrogate key triggers a warning."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)

        with Database(cfg.db_path) as db:
            AnnotationRepository(db).upsert(
                AnnotationRecord(
                    asset_id="pg::public.orders",
                    properties={
                        "column_notes": {
                            "order_id": "This is a surrogate key, do not use in aggregates"
                        }
                    },
                )
            )

        sql = "SELECT SUM(order_id) FROM public.orders"
        result_texts = _handle_verify(cfg, {"sql": sql})
        data = json.loads(result_texts[0].text)

        assert data["valid"] is False
        assert any("surrogate key" in w.lower() for w in data["warnings"])
        assert any("order_id" in w for w in data["warnings"])

    def test_avg_on_surrogate_key_column_emits_warning(self, tmp_path):
        """AVG on a surrogate key column also triggers a warning."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)

        with Database(cfg.db_path) as db:
            AnnotationRepository(db).upsert(
                AnnotationRecord(
                    asset_id="pg::public.orders",
                    properties={
                        "column_notes": {
                            "order_id": "surrogate key - internal identifier only"
                        }
                    },
                )
            )

        sql = "SELECT AVG(order_id) FROM public.orders"
        result_texts = _handle_verify(cfg, {"sql": sql})
        data = json.loads(result_texts[0].text)

        assert data["valid"] is False
        assert any("order_id" in w for w in data["warnings"])

    def test_non_surrogate_column_in_sum_no_warning(self, tmp_path):
        """SUM on a normal column does not trigger surrogate key warning."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)

        with Database(cfg.db_path) as db:
            AnnotationRepository(db).upsert(
                AnnotationRecord(
                    asset_id="pg::public.orders",
                    properties={
                        "column_notes": {
                            "amount": "The order total in USD"
                        }
                    },
                )
            )

        sql = "SELECT SUM(amount) FROM public.orders"
        result_texts = _handle_verify(cfg, {"sql": sql})
        data = json.loads(result_texts[0].text)

        assert data["valid"] is True
        assert data["warnings"] == []

    def test_unparseable_sql_graceful_error(self, tmp_path):
        """Totally broken SQL does not crash — returns a valid JSON response."""
        cfg = _make_cfg(tmp_path)

        result_texts = _handle_verify(cfg, {"sql": "@@@ NOT VALID SQL @@@"})
        assert len(result_texts) == 1
        # Must be valid JSON, must not raise
        data = json.loads(result_texts[0].text)
        assert "valid" in data
        assert "warnings" in data

    def test_empty_sql_returns_error(self, tmp_path):
        """Empty SQL returns valid=False with an informative warning."""
        cfg = _make_cfg(tmp_path)
        result_texts = _handle_verify(cfg, {"sql": ""})
        data = json.loads(result_texts[0].text)
        assert data["valid"] is False
        assert len(data["warnings"]) >= 1

    def test_no_join_guidance_no_warning(self, tmp_path):
        """An edge without join_guidance metadata does not produce a warning."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)

        with Database(cfg.db_path) as db:
            EdgeRepository(db).upsert(
                Edge(
                    upstream_id="pg::public.orders",
                    downstream_id="pg::public.customers",
                    kind="fk",
                    metadata={},
                )
            )

        sql = "SELECT * FROM public.orders o JOIN public.customers c ON o.customer_id = c.id"
        result_texts = _handle_verify(cfg, {"sql": sql})
        data = json.loads(result_texts[0].text)
        assert data["valid"] is True
        assert data["warnings"] == []


# ---------------------------------------------------------------------------
# atlas_define_term + atlas_find_term tests
# ---------------------------------------------------------------------------


class TestDefineTerm:
    def test_define_term_basic(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        result_texts = _handle_define_term(
            cfg,
            {
                "name": "revenue",
                "definition": "Total net sales in USD",
                "formula": "SUM(amount) WHERE status = 'completed'",
                "referenced_columns": ["orders.amount", "orders.status"],
            },
        )
        assert len(result_texts) == 1
        assert "revenue" in result_texts[0].text

        with Database(cfg.db_path) as db:
            term = BusinessTermRepository(db).get("revenue")
        assert term is not None
        assert term.definition == "Total net sales in USD"
        assert "orders.amount" in term.referenced_columns

    def test_define_term_minimal(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _handle_define_term(cfg, {"name": "dau"})
        with Database(cfg.db_path) as db:
            term = BusinessTermRepository(db).get("dau")
        assert term is not None
        assert term.name == "dau"


class TestFindTermWithBusinessTerms:
    def test_find_term_returns_business_terms_first(self, tmp_path):
        """BusinessTerm matches appear before annotation/asset matches."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)

        with Database(cfg.db_path) as db:
            BusinessTermRepository(db).upsert(
                BusinessTerm(
                    name="revenue",
                    definition="Total net sales",
                    formula="SUM(amount)",
                    referenced_columns=["orders.amount"],
                )
            )

        result_texts = _handle_find_term(cfg, {"term": "revenue"})
        text = result_texts[0].text

        assert "Business terms:" in text
        assert "revenue" in text
        assert "[business_term]" in text

    def test_find_term_business_term_before_assets(self, tmp_path):
        """BusinessTerm section appears before asset section in output."""
        cfg = _make_cfg(tmp_path)

        with Database(cfg.db_path) as db:
            AssetRepository(db).upsert(
                Asset(id="pg::public.revenue_table", source="pg:test", kind="table", name="revenue_table")
            )
            BusinessTermRepository(db).upsert(
                BusinessTerm(name="revenue", definition="The money metric")
            )

        result_texts = _handle_find_term(cfg, {"term": "revenue"})
        text = result_texts[0].text

        business_terms_pos = text.find("Business terms:")
        asset_pos = text.find("revenue_table")

        assert business_terms_pos != -1
        assert asset_pos == -1 or business_terms_pos < asset_pos

    def test_find_term_no_results_when_nothing_matches(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        result_texts = _handle_find_term(cfg, {"term": "zzz_no_match_zzz"})
        assert "No assets or terms found" in result_texts[0].text

    def test_find_term_shows_definition_and_formula(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        with Database(cfg.db_path) as db:
            BusinessTermRepository(db).upsert(
                BusinessTerm(
                    name="churn_rate",
                    definition="Percentage of users who stopped using the product",
                    formula="churned_users / total_users",
                )
            )

        result_texts = _handle_find_term(cfg, {"term": "churn_rate"})
        text = result_texts[0].text
        assert "Percentage of users" in text
        assert "formula:" in text
        assert "churned_users" in text
