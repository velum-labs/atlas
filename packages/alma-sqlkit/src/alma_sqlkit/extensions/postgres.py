"""PostgreSQL-specific extensions to relational algebra types.

These extensions add PostgreSQL-specific features:
- DISTINCT ON (ExtendedProjection)
- LATERAL joins (ExtendedJoin)
"""

from __future__ import annotations

from typing import Literal as TypeLiteral

from alma_algebrakit import (
    Expression,
    Join,
    JoinType,
    Projection,
)
from pydantic import Field


class ExtendedProjection(Projection):
    """Projection with PostgreSQL DISTINCT ON support.

    DISTINCT ON (expr, ...) returns only the first row for each distinct
    combination of the specified expressions, useful with ORDER BY.
    """

    type: TypeLiteral["projection"] = "projection"
    distinct_on: list[Expression] | None = Field(
        default=None, description="DISTINCT ON expressions (PostgreSQL)"
    )

    def fingerprint(self) -> str:
        cols = []
        for expr, alias in self.columns:
            if alias:
                cols.append(f"{expr.fingerprint()} AS {alias}")
            else:
                cols.append(expr.fingerprint())

        if self.distinct_on:
            distinct_on_str = ", ".join(e.fingerprint() for e in self.distinct_on)
            prefix = f"DISTINCT ON ({distinct_on_str}) "
        elif self.distinct:
            prefix = "DISTINCT "
        else:
            prefix = ""

        return f"π[{prefix}{', '.join(cols)}]({self.input.fingerprint()})"


class ExtendedJoin(Join):
    """Join with LATERAL support.

    LATERAL joins allow the right side of the join to reference columns
    from the left side, enabling correlated subqueries in the FROM clause.
    """

    type: TypeLiteral["join"] = "join"
    is_lateral: bool = Field(default=False, description="Whether this is a LATERAL join")

    def fingerprint(self) -> str:
        join_sym = {
            JoinType.INNER: "⋈",
            JoinType.LEFT: "⟕",
            JoinType.RIGHT: "⟖",
            JoinType.FULL: "⟗",
            JoinType.CROSS: "×",
        }[self.join_type]
        lateral_str = "LATERAL " if self.is_lateral else ""
        cond_str = f"[{self.condition.fingerprint()}]" if self.condition else ""
        return f"({self.left.fingerprint()} {join_sym}{lateral_str}{cond_str} {self.right.fingerprint()})"
