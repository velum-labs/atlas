"""Capability tracking for queries and contracts.

Every parsed query and contract view carries a capability vector that
enables sound rewriting by refusing operations outside supported subsets.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class SQLFeature(StrEnum):
    """SQL features that affect rewriting safety."""

    # Basic
    SELECT = "select"
    FROM = "from"
    WHERE = "where"
    JOIN_INNER = "join_inner"

    # Outer joins (different semantics)
    JOIN_LEFT = "join_left"
    JOIN_RIGHT = "join_right"
    JOIN_FULL = "join_full"
    JOIN_CROSS = "join_cross"
    JOIN_LATERAL = "join_lateral"

    # Subqueries
    SUBQUERY_FROM = "subquery_from"
    SUBQUERY_WHERE = "subquery_where"
    SUBQUERY_CORRELATED = "subquery_correlated"

    # CTEs
    CTE = "cte"
    CTE_RECURSIVE = "cte_recursive"

    # Set operations
    UNION = "union"
    UNION_ALL = "union_all"
    INTERSECT = "intersect"
    EXCEPT = "except"

    # Aggregation
    GROUP_BY = "group_by"
    HAVING = "having"
    AGGREGATE_FUNCTION = "aggregate_function"

    # Window functions
    WINDOW_FUNCTION = "window_function"
    QUALIFY = "qualify"

    # Distinct
    DISTINCT = "distinct"
    DISTINCT_ON = "distinct_on"

    # Ordering/Limiting
    ORDER_BY = "order_by"
    LIMIT = "limit"
    OFFSET = "offset"

    # Expressions
    CASE_WHEN = "case_when"
    COALESCE = "coalesce"
    NULLIF = "nullif"
    CAST = "cast"

    # Special
    EXISTS = "exists"
    IN_SUBQUERY = "in_subquery"
    SCALAR_SUBQUERY = "scalar_subquery"


class SemanticsMode(StrEnum):
    """Semantics mode for query operators."""

    BAG = "bag"  # Allows duplicates (SQL default for most operations)
    SET = "set"  # No duplicates (after DISTINCT, UNION, etc.)


class NullMode(StrEnum):
    """NULL handling mode."""

    SQL_3VL = "sql_3vl"  # SQL three-valued logic (TRUE, FALSE, UNKNOWN)


class DeterminismLevel(StrEnum):
    """Determinism level of a query/expression."""

    DETERMINISTIC = "deterministic"  # Same inputs always produce same outputs
    STABLE = "stable"  # Same within a transaction
    VOLATILE = "volatile"  # May return different values on each call


class QueryCapabilities(BaseModel):
    """Capability vector for a query.

    Used to:
    1. Gate rewriting - only rewrite queries with supported features
    2. Track what proof methods are applicable
    3. Document unsupported features
    """

    sql_features: set[SQLFeature] = Field(
        default_factory=set,
        description="SQL features used in the query",
    )
    semantics_mode: SemanticsMode = Field(
        default=SemanticsMode.BAG,
        description="Whether query produces bag or set semantics",
    )
    null_mode: NullMode = Field(
        default=NullMode.SQL_3VL,
        description="NULL handling mode",
    )
    determinism: DeterminismLevel = Field(
        default=DeterminismLevel.DETERMINISTIC,
        description="Query determinism level",
    )
    volatile_functions: list[str] = Field(
        default_factory=list,
        description="Names of volatile functions used",
    )
    unsupported_constructs: list[str] = Field(
        default_factory=list,
        description="Constructs that are parsed but not supported for rewriting",
    )

    def add_feature(self, feature: SQLFeature) -> None:
        """Add a feature to the capability set."""
        self.sql_features.add(feature)

    def has_feature(self, feature: SQLFeature) -> bool:
        """Check if query uses a feature."""
        return feature in self.sql_features

    def has_outer_joins(self) -> bool:
        """Check if query uses outer joins."""
        return bool(
            self.sql_features
            & {
                SQLFeature.JOIN_LEFT,
                SQLFeature.JOIN_RIGHT,
                SQLFeature.JOIN_FULL,
            }
        )

    def has_subqueries(self) -> bool:
        """Check if query uses subqueries."""
        return bool(
            self.sql_features
            & {
                SQLFeature.SUBQUERY_FROM,
                SQLFeature.SUBQUERY_WHERE,
                SQLFeature.SUBQUERY_CORRELATED,
                SQLFeature.IN_SUBQUERY,
                SQLFeature.SCALAR_SUBQUERY,
                SQLFeature.EXISTS,
            }
        )

    def has_correlated_subqueries(self) -> bool:
        """Check for correlated subqueries (undecidable containment)."""
        return SQLFeature.SUBQUERY_CORRELATED in self.sql_features

    def has_window_functions(self) -> bool:
        """Check for window functions."""
        return SQLFeature.WINDOW_FUNCTION in self.sql_features

    def has_set_operations(self) -> bool:
        """Check for UNION/INTERSECT/EXCEPT."""
        return bool(
            self.sql_features
            & {
                SQLFeature.UNION,
                SQLFeature.UNION_ALL,
                SQLFeature.INTERSECT,
                SQLFeature.EXCEPT,
            }
        )

    def is_spj(self) -> bool:
        """Check if query is Select-Project-Join only (most amenable to rewriting)."""
        spj_allowed = {
            SQLFeature.SELECT,
            SQLFeature.FROM,
            SQLFeature.WHERE,
            SQLFeature.JOIN_INNER,
        }
        return self.sql_features <= spj_allowed

    def is_spjg(self) -> bool:
        """Check if query is SPJ with GROUP BY (supports more rewrites)."""
        spjg_allowed = {
            SQLFeature.SELECT,
            SQLFeature.FROM,
            SQLFeature.WHERE,
            SQLFeature.JOIN_INNER,
            SQLFeature.GROUP_BY,
            SQLFeature.HAVING,
            SQLFeature.AGGREGATE_FUNCTION,
        }
        return self.sql_features <= spjg_allowed

    def supports_containment_proof(self) -> bool:
        """Check if containment can be proven for this query class."""
        # Containment is decidable for conjunctive queries (SPJ)
        # Extended to SPJG with restrictions
        # NOT decidable for:
        # - Correlated subqueries
        # - Arbitrary NOT/EXCEPT
        # - Window functions in general

        if self.has_correlated_subqueries():
            return False

        if self.determinism == DeterminismLevel.VOLATILE:
            return False

        # For now, support SPJ and SPJG
        return self.is_spj() or self.is_spjg()

    def supports_algebraic_rewriting(self) -> bool:
        """Check if algebraic rewriting rules apply."""
        # Outer joins break associativity/commutativity
        # Window functions have order dependencies
        # DISTINCT changes bag to set semantics

        if self.has_outer_joins():
            return False

        if self.has_window_functions():
            return False

        return True

    def mark_unsupported(self, construct: str) -> None:
        """Mark a construct as unsupported for rewriting."""
        if construct not in self.unsupported_constructs:
            self.unsupported_constructs.append(construct)


# Rewrite capability requirements
REWRITE_REQUIREMENTS = {
    "view_folding_spj": {
        "required_absent": {
            SQLFeature.SUBQUERY_CORRELATED,
            SQLFeature.WINDOW_FUNCTION,
        },
        "determinism": DeterminismLevel.DETERMINISTIC,
    },
    "view_folding_outer_join": {
        "required_present": {
            SQLFeature.JOIN_LEFT,
            SQLFeature.JOIN_RIGHT,
            SQLFeature.JOIN_FULL,
        },
        "note": "Outer join folding requires exact structure match",
    },
    "aggregation_rollup": {
        "required_present": {
            SQLFeature.GROUP_BY,
            SQLFeature.AGGREGATE_FUNCTION,
        },
        "note": "Rollup requires decomposable aggregates",
    },
}


def check_rewrite_compatibility(
    query_caps: QueryCapabilities,
    contract_caps: QueryCapabilities,
) -> tuple[bool, list[str]]:
    """Check if a query can potentially be rewritten using a contract.

    Returns:
        (compatible, reasons) - whether compatible and list of blocking reasons
    """
    reasons: list[str] = []

    # Volatile queries cannot be rewritten
    if query_caps.determinism == DeterminismLevel.VOLATILE:
        reasons.append(f"Query uses volatile functions: {query_caps.volatile_functions}")

    # Correlated subqueries block proof
    if query_caps.has_correlated_subqueries():
        reasons.append("Query contains correlated subqueries (undecidable)")

    # Contract with outer joins requires exact match
    if contract_caps.has_outer_joins() and query_caps.has_outer_joins():
        reasons.append("Both have outer joins - requires exact structure match")

    # Semantics mode mismatch
    if query_caps.semantics_mode != contract_caps.semantics_mode:
        if query_caps.semantics_mode == SemanticsMode.SET:
            reasons.append("Query requires SET semantics but contract produces BAG")

    return len(reasons) == 0, reasons
