"""Public API export guards for algebrakit."""

from __future__ import annotations

import alma_algebrakit as algebrakit


def test_algebrakit_exports_are_unique() -> None:
    exports = list(algebrakit.__all__)
    assert len(exports) == len(set(exports))


def test_algebrakit_exports_snapshot_edges() -> None:
    exports = tuple(algebrakit.__all__)

    assert exports[:10] == (
        "RAExpression",
        "Relation",
        "Selection",
        "Projection",
        "Join",
        "Aggregation",
        "Union",
        "Difference",
        "Intersect",
        "Sort",
    )
    assert exports[-10:] == (
        "SUBQUERY_ID_PREFIX",
        "effective_table_name",
        "generate_subquery_alias",
        "generate_column_alias",
        "normalize_cte_name",
        "generate_subquery_id",
        "generate_cte_id",
        "QualifiedName",
        "parse_parts",
        "normalize_name",
    )
