"""Tests for the CompanionBundle schema, ContextPackage adapter, and renderer."""

from __future__ import annotations

from alma_atlas.agents.atlas_agent_schemas import ContextPackage, JoinRecommendation
from alma_atlas.mcp.agent_bundle import (
    MAX_COLUMNS_SHOWN,
    MAX_RELATED_ASSETS_SHOWN,
    MAX_RELATIONSHIPS_SHOWN,
    CompanionAsset,
    CompanionBundle,
    CompanionColumn,
    CompanionRelationship,
    adapt,
    render,
)

# ---------------------------------------------------------------------------
# Adapter (ContextPackage -> CompanionBundle)
# ---------------------------------------------------------------------------


def test_adapt_empty_context_package_yields_empty_bundle():
    bundle = adapt(ContextPackage())
    assert bundle.primary_asset is None
    assert bundle.related_assets == []
    assert bundle.relationships == []
    assert bundle.summary is None
    assert bundle.warnings == []


def test_adapt_maps_relevant_tables_to_related_assets():
    pkg = ContextPackage(relevant_tables=["pg::public.orders", "bq::analytics.users"])
    bundle = adapt(pkg)
    assert [a.asset_id for a in bundle.related_assets] == ["pg::public.orders", "bq::analytics.users"]
    # Adapter doesn't have kind/source info, defaults to unknown
    assert all(a.kind == "unknown" for a in bundle.related_assets)


def test_adapt_maps_recommended_joins_to_relationships():
    pkg = ContextPackage(
        recommended_joins=[
            JoinRecommendation(
                tables=["orders", "users"],
                join_path="orders.user_id = users.id",
                guidance="users.id is the primary key",
            ),
        ]
    )
    bundle = adapt(pkg)
    assert len(bundle.relationships) == 1
    rel = bundle.relationships[0]
    assert rel.upstream == "orders"
    assert rel.downstream == "users"
    assert rel.kind == "join_path"
    assert rel.guidance == "users.id is the primary key"


def test_adapt_falls_back_to_join_path_when_guidance_missing():
    pkg = ContextPackage(
        recommended_joins=[JoinRecommendation(tables=["a", "b"], join_path="a.x = b.x")]
    )
    bundle = adapt(pkg)
    assert bundle.relationships[0].guidance == "a.x = b.x"


def test_adapt_handles_join_with_fewer_than_two_tables():
    pkg = ContextPackage(recommended_joins=[JoinRecommendation(tables=[], join_path="(empty)")])
    bundle = adapt(pkg)
    rel = bundle.relationships[0]
    assert rel.upstream == ""
    assert rel.downstream == ""


def test_adapt_carries_warnings_and_summary_and_evidence():
    pkg = ContextPackage(
        warnings=["w1", "w2"],
        summary="A summary line.",
        evidence_interpretation="Amounts are in CLP, not USD.",
    )
    bundle = adapt(pkg)
    assert bundle.warnings == ["w1", "w2"]
    assert bundle.summary == "A summary line."
    assert bundle.context_note == "Amounts are in CLP, not USD."


# ---------------------------------------------------------------------------
# Renderer (CompanionBundle -> str)
# ---------------------------------------------------------------------------


def test_render_empty_bundle_returns_no_results_message():
    text = render(CompanionBundle())
    assert "No matching context" in text


def test_render_summary_only():
    text = render(CompanionBundle(summary="Only a summary."))
    assert text.strip() == "Only a summary."


def test_render_warnings_section():
    bundle = CompanionBundle(warnings=["watch out", "be careful"])
    text = render(bundle)
    assert "Warnings:" in text
    assert "  - watch out" in text
    assert "  - be careful" in text


def test_render_context_note_section():
    bundle = CompanionBundle(context_note="amounts in CLP not USD")
    text = render(bundle)
    assert "Context: amounts in CLP not USD" in text


def test_render_primary_asset_with_full_details():
    asset = CompanionAsset(
        asset_id="bigquery:fintual::analytics.orders",
        kind="bigquery",
        source="fintual",
        owner="data-eng@fintual.com",
        last_updated="16 hours ago",
        description="Order facts table.",
        columns=[
            CompanionColumn(name="order_id", type="STRING", nullable=False, annotation="primary key"),
            CompanionColumn(name="user_id", type="STRING", nullable=False),
            CompanionColumn(name="amount_clp", type="NUMERIC", nullable=True),
        ],
        columns_total=3,
    )
    text = render(CompanionBundle(primary_asset=asset))
    assert "bigquery:fintual::analytics.orders [bigquery -> fintual]" in text
    assert "Owner: data-eng@fintual.com" in text
    assert "Updated: 16 hours ago" in text
    assert "Description: Order facts table." in text
    assert "Schema (3 columns):" in text
    assert "order_id (STRING, NOT NULL) -- primary key" in text
    assert "amount_clp (NUMERIC)" in text
    # Nullable columns omit the NOT NULL marker
    assert "amount_clp (NUMERIC, NOT NULL)" not in text


def test_render_primary_asset_truncates_long_column_lists():
    columns = [
        CompanionColumn(name=f"col_{i}", type="STRING")
        for i in range(MAX_COLUMNS_SHOWN + 5)
    ]
    asset = CompanionAsset(
        asset_id="bq::wide_table",
        columns=columns,
        columns_total=len(columns),
    )
    text = render(CompanionBundle(primary_asset=asset))
    assert f"Schema ({MAX_COLUMNS_SHOWN} of {len(columns)} columns shown" in text
    assert "ask for more" in text
    # First column shown, last column NOT shown
    assert "col_0 (STRING)" in text
    assert f"col_{len(columns) - 1} (STRING)" not in text


def test_render_related_assets_truncates_long_lists():
    related = [CompanionAsset(asset_id=f"asset_{i}") for i in range(MAX_RELATED_ASSETS_SHOWN + 4)]
    text = render(CompanionBundle(related_assets=related))
    assert f"Related assets ({MAX_RELATED_ASSETS_SHOWN} of {len(related)} shown):" in text
    assert "asset_0" in text
    assert f"asset_{len(related) - 1}" not in text


def test_render_relationships_truncates_long_lists():
    rels = [
        CompanionRelationship(upstream=f"a_{i}", downstream=f"b_{i}", kind="join_path")
        for i in range(MAX_RELATIONSHIPS_SHOWN + 2)
    ]
    text = render(CompanionBundle(relationships=rels))
    assert f"Relationships ({MAX_RELATIONSHIPS_SHOWN} of {len(rels)} shown):" in text


def test_render_full_bundle_includes_all_sections():
    bundle = CompanionBundle(
        summary="Top-line summary.",
        primary_asset=CompanionAsset(asset_id="x", kind="bq", source="src"),
        related_assets=[CompanionAsset(asset_id="related_1")],
        relationships=[
            CompanionRelationship(upstream="x", downstream="related_1", kind="lineage_edge")
        ],
        context_note="business note",
        warnings=["one warning"],
    )
    text = render(bundle)
    # Sections appear in order: summary, primary, related, relationships, context, warnings
    indices = [
        text.index("Top-line summary."),
        text.index("x [bq -> src]"),
        text.index("Related assets"),
        text.index("Relationships"),
        text.index("Context: business note"),
        text.index("Warnings:"),
    ]
    assert indices == sorted(indices), f"Sections not in expected order: {indices}"


def test_render_handles_circular_relationship_without_looping():
    # Adapter/renderer don't traverse — they just print what they got. Self-referential
    # relationships in input must not infinite-loop.
    rels = [CompanionRelationship(upstream="a", downstream="a", kind="self_ref")]
    text = render(CompanionBundle(relationships=rels))
    assert "a -> a (self_ref)" in text
