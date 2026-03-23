"""Type definitions for bound (resolved) expressions.

Provides strongly-typed representations of bound elements with
stable identifiers that don't depend on alias strings.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from alma_algebrakit.schema.types import DataType, SQLDataType


class AttributeRef(BaseModel):
    """A fully resolved attribute reference with stable identifiers.

    Unlike string-based column references (e.g., "orders.id"), this provides:
    - Unique table_id that survives alias changes
    - Unique column_id within the table schema
    - Full type information
    - Original name for rendering

    This is the algebraic equivalent of a "bound variable" - it uniquely
    identifies which attribute from which relation instance is being referenced.
    """

    id: UUID = Field(default_factory=uuid4, description="Unique identifier for this reference")
    table_id: str = Field(description="Stable fully-qualified table identifier")
    column_id: str = Field(description="Stable column identifier within table schema")
    sql_name: str = Field(description="Original name (for rendering)")
    table_alias: str | None = Field(default=None, description="Table alias used in query")
    data_type: DataType = Field(
        default_factory=lambda: DataType(base_type=SQLDataType.UNKNOWN),
        description="Attribute data type",
    )

    def qualified_id(self) -> str:
        """Return fully qualified bound identifier."""
        return f"{self.table_id}.{self.column_id}"

    def display_name(self) -> str:
        """Return display name for error messages."""
        if self.table_alias:
            return f"{self.table_alias}.{self.sql_name}"
        return self.sql_name

    def __hash__(self) -> int:
        return hash(self.qualified_id())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AttributeRef):
            return False
        return self.qualified_id() == other.qualified_id()


# Backward compatibility alias
BoundColumnRef = AttributeRef


class BoundLiteral(BaseModel):
    """A bound literal value with type information."""

    id: UUID = Field(default_factory=uuid4)
    value: Any = Field(description="The literal value")
    data_type: DataType = Field(
        default_factory=lambda: DataType(base_type=SQLDataType.UNKNOWN),
        description="Inferred or explicit type",
    )
    sql_text: str = Field(default="", description="Original representation")

    def is_null(self) -> bool:
        """Check if this is a NULL literal."""
        return self.value is None


class BoundExpression(BaseModel):
    """Base class for bound expressions with lineage tracking."""

    id: UUID = Field(default_factory=uuid4)
    upstream_columns: list[AttributeRef] = Field(
        default_factory=list,
        description="All attribute references this expression depends on",
    )
    result_type: DataType = Field(
        default_factory=lambda: DataType(base_type=SQLDataType.UNKNOWN),
        description="Result type of the expression",
    )
    deterministic: bool = Field(default=True, description="False if uses volatile functions")
    sql_text: str | None = Field(default=None, description="Original text for rendering")


class BoundFunctionCall(BoundExpression):
    """A bound function call with argument types."""

    function_name: str = Field(description="Normalized function name")
    args: list[Any] = Field(default_factory=list)
    is_aggregate: bool = Field(default=False)
    is_window: bool = Field(default=False)
    distinct: bool = Field(default=False)


class BoundWindowFunction(BoundExpression):
    """A bound window function (function with OVER clause).

    Window functions compute values across a set of rows related to the current row.
    They don't reduce the number of rows like aggregates do.
    """

    function: Any = Field(description="The inner function being windowed")
    partition_by: list[Any] = Field(default_factory=list, description="Expressions to partition by")
    order_by: list[Any] = Field(
        default_factory=list, description="Expressions to order by within partitions"
    )
    frame_spec: str | None = Field(
        default=None, description="Window frame specification (ROWS/RANGE BETWEEN...)"
    )

    def is_ranking_function(self) -> bool:
        """Check if this is a ranking function (ROW_NUMBER, RANK, etc.)."""
        if hasattr(self.function, "function_name"):
            name = self.function.function_name.lower().replace("_", "")
            return name in ("rownumber", "rank", "denserank", "ntile", "percentrank", "cumedist")
        return False

    def is_offset_function(self) -> bool:
        """Check if this is an offset function (LAG, LEAD, etc.)."""
        if hasattr(self.function, "function_name"):
            name = self.function.function_name.lower()
            return name in ("lag", "lead", "first_value", "last_value", "nth_value")
        return False


class BoundBinaryOp(BoundExpression):
    """A bound binary operation."""

    operator: str = Field(description="Operator symbol")
    left: Any = Field(description="Left operand")
    right: Any = Field(description="Right operand")


# Type alias for any expression-like bound value
BoundExprLike = AttributeRef | BoundLiteral | BoundExpression


class BoundPredicate(BaseModel):
    """Base class for bound predicates (conditions).

    Predicates are evaluated under SQL 3-valued logic:
    - TRUE: condition holds
    - FALSE: condition does not hold
    - UNKNOWN: condition involves NULL

    WHERE clause semantics: only TRUE rows pass (FALSE and UNKNOWN filtered).
    """

    id: UUID = Field(default_factory=uuid4)
    upstream_columns: list[AttributeRef] = Field(
        default_factory=list,
        description="All attribute references in this predicate",
    )
    deterministic: bool = Field(default=True)


class BoundComparison(BoundPredicate):
    """A bound comparison predicate."""

    left: BoundExprLike = Field(description="Left side")
    operator: str = Field(description="Comparison operator: =, !=, <, <=, >, >=")
    right: BoundExprLike = Field(description="Right side")


class BoundIsNull(BoundPredicate):
    """IS NULL or IS NOT NULL predicate."""

    expression: BoundExprLike = Field(description="Expression being tested")
    negated: bool = Field(default=False, description="True for IS NOT NULL")


class BoundIn(BoundPredicate):
    """IN predicate."""

    expression: BoundExprLike = Field(description="Expression being tested")
    values: list[BoundExprLike] = Field(description="List of values")
    negated: bool = Field(default=False, description="True for NOT IN")


class BoundBetween(BoundPredicate):
    """BETWEEN predicate."""

    expression: BoundExprLike = Field(description="Expression being tested")
    low: BoundExprLike = Field(description="Lower bound")
    high: BoundExprLike = Field(description="Upper bound")
    negated: bool = Field(default=False, description="True for NOT BETWEEN")


class BoundLike(BoundPredicate):
    """LIKE/ILIKE predicate."""

    expression: BoundExprLike = Field(description="Expression being tested")
    pattern: BoundExprLike = Field(description="Pattern")
    case_insensitive: bool = Field(default=False, description="True for ILIKE")
    negated: bool = Field(default=False)


class BoundLogical(BoundPredicate):
    """Logical combination of predicates (AND, OR, NOT)."""

    operator: str = Field(description="AND, OR, or NOT")
    operands: list[BoundPredicate] = Field(description="Child predicates")
