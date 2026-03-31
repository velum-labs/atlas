from __future__ import annotations

from alma_algebrakit import AtomicPredicate, ColumnRef, ComparisonOp, Literal, Relation
from alma_algebrakit.rewriting.predicates import (
    rewrite_on_predicate,
    rewrite_predicate_columns,
)


def test_rewrite_predicate_columns_replaces_exact_identifiers_only() -> None:
    predicate = AtomicPredicate(
        left=ColumnRef(table="t", column="id"),
        op=ComparisonOp.EQ,
        right=ColumnRef(table="t", column="id2"),
    )

    rewritten = rewrite_predicate_columns(predicate, {"t.id": "cv.order_id"})

    assert "cv.order_id" in rewritten
    assert "t.id2" in rewritten
    assert "cv.order_id2" not in rewritten


def test_rewrite_on_predicate_replaces_only_covered_columns() -> None:
    predicate = AtomicPredicate(
        left=ColumnRef(table="orders", column="id"),
        op=ComparisonOp.EQ,
        right=Literal(value=1, data_type="integer"),
    )
    covered_rels = {Relation(name="orders", alias="orders").to_relation_ref()}

    rewritten = rewrite_on_predicate(
        predicate,
        covered_rels=covered_rels,
        lineage={("orders", "id"): "order_id"},
        view_alias="cv",
    )

    assert rewritten == "cv.order_id = 1"
