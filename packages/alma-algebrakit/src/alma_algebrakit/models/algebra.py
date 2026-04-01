"""Relational Algebra AST types.

This module defines the AST for relational algebra expressions, which serve
as the canonical representation for queries during analysis and rewriting.

The supported operations are:
- Base relations (R ∈ S_raw)
- Selection (σ_φ(E))
- Projection (π_A(E))
- Join (E_1 ⋈_θ E_2)
- Grouping/aggregation (γ_{G; aggs}(E))
- Union (E_1 ∪ E_2)
- Difference (E_1 - E_2)
- Intersection (E_1 ∩ E_2)
- Sort (τ_keys(E)) - ORDER BY
- Limit (λ_n(E)) - LIMIT/OFFSET
- WITH/CTE expressions

Expression types include:
- Column references, literals, binary expressions, function calls
- Window functions (OVER clause)
- CASE expressions
- Subquery expressions (scalar, EXISTS, IN)

Note: This module is SQL-agnostic. For SQL emission, use sqlkit.emit_sql().
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from typing import Literal as TypeLiteral

if TYPE_CHECKING:
    pass  # Forward references if needed

from pydantic import BaseModel, Field

# =============================================================================
# Relation Identity Types (for alias-safe rewriting)
# =============================================================================


@dataclass(frozen=True)
class RelationRef:
    """A relation instance in a query (physical table + alias).

    This provides alias-safe identity for handling self-joins and repeated
    table references. Two RelationRef instances are equal if they refer to
    the same physical table with the same alias.

    Example:
        - `orders o` -> RelationRef("orders", "o")
        - `orders` -> RelationRef("orders", "orders")
        - Self-join: `orders o1 JOIN orders o2` creates two distinct RelationRefs
    """

    physical_table: str
    alias: str  # Use table name if no alias provided

    def effective_name(self) -> str:
        """Return the name used to qualify columns (alias or table name)."""
        return self.alias

    def __str__(self) -> str:
        if self.alias != self.physical_table:
            return f"{self.physical_table} AS {self.alias}"
        return self.physical_table


@dataclass(frozen=True)
class BoundColumnRef:
    """A column reference bound to a specific relation instance.

    Unlike ColumnRef (which is a syntactic element), BoundColumnRef
    explicitly ties a column to its source relation instance, enabling
    correct handling of aliases and self-joins.

    Note: This is a lightweight RA-level binding type. For full binding
    with type information, use algebrakit.bound.AttributeRef.
    """

    rel: RelationRef
    column: str

    def qualified(self) -> str:
        """Return fully qualified column reference."""
        return f"{self.rel.effective_name()}.{self.column}"

    def __str__(self) -> str:
        return self.qualified()


# =============================================================================
# Enums
# =============================================================================


class JoinType(StrEnum):
    """Type of join operation."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    # Semi-joins and anti-joins (SQL: EXISTS/NOT EXISTS, IN/NOT IN)
    SEMI = "semi"  # Returns rows from left that have a match in right
    ANTI = "anti"  # Returns rows from left that have NO match in right


class ComparisonOp(StrEnum):
    """Comparison operators for predicates."""

    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    LIKE = "like"
    ILIKE = "ilike"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"


class LogicalOp(StrEnum):
    """Logical operators for compound predicates."""

    AND = "and"
    OR = "or"
    NOT = "not"


class AggregateFunction(StrEnum):
    """Aggregate functions."""

    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    ARRAY_AGG = "array_agg"
    STRING_AGG = "string_agg"
    BOOL_AND = "bool_and"
    BOOL_OR = "bool_or"


class SortDirection(StrEnum):
    """Sort direction for ORDER BY."""

    ASC = "asc"
    DESC = "desc"


class NullsPosition(StrEnum):
    """NULL ordering in ORDER BY."""

    FIRST = "first"
    LAST = "last"


class WindowFrameType(StrEnum):
    """Window frame type."""

    ROWS = "rows"
    RANGE = "range"
    GROUPS = "groups"


class WindowFrameBound(StrEnum):
    """Window frame boundary."""

    UNBOUNDED_PRECEDING = "unbounded_preceding"
    CURRENT_ROW = "current_row"
    UNBOUNDED_FOLLOWING = "unbounded_following"
    PRECEDING = "preceding"  # With offset
    FOLLOWING = "following"  # With offset


# =============================================================================
# Expression Types (for predicates and computed columns)
# =============================================================================


class Expression(BaseModel, ABC):
    """Base class for expressions (scalars, columns, etc.).

    Note: Expression objects should be treated as immutable. Creating modified
    expressions should produce new objects rather than mutating existing ones.
    """

    @abstractmethod
    def referenced_columns(self) -> set[str]:
        """Return set of column references in this expression."""
        ...

    @abstractmethod
    def fingerprint(self) -> str:
        """Return a canonical string representation for comparison."""
        ...


class ColumnRef(Expression):
    """Reference to a table column."""

    type: TypeLiteral["column_ref"] = "column_ref"
    table: str | None = Field(default=None, description="Table name/alias (optional)")
    column: str = Field(description="Column name")

    def referenced_columns(self) -> set[str]:
        if self.table:
            return {f"{self.table}.{self.column}"}
        return {self.column}

    def fingerprint(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column

    def qualified_name(self) -> str:
        """Return the fully qualified column name."""
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column


class Literal(Expression):
    """A literal value.

    For IN lists, value can be a list of Expression objects.
    """

    type: TypeLiteral["literal"] = "literal"
    value: Any = Field(description="The literal value (or list of values for IN)")
    data_type: str | None = Field(default=None, description="Data type hint")

    def referenced_columns(self) -> set[str]:
        return set()

    def fingerprint(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, str):
            return f"'{self.value}'"
        # Handle list of values (for IN expressions)
        if isinstance(self.value, list):
            items = []
            for item in self.value:
                if hasattr(item, "fingerprint"):
                    items.append(item.fingerprint())
                elif isinstance(item, str):
                    items.append(f"'{item}'")
                else:
                    items.append(str(item))
            return f"({', '.join(items)})"
        return str(self.value)


class BinaryExpression(Expression):
    """Binary arithmetic/string expression."""

    type: TypeLiteral["binary_expr"] = "binary_expr"
    left: Expression = Field(description="Left operand")
    operator: str = Field(description="Operator (+, -, *, /, ||, etc.)")
    right: Expression = Field(description="Right operand")

    def referenced_columns(self) -> set[str]:
        return self.left.referenced_columns() | self.right.referenced_columns()

    def fingerprint(self) -> str:
        return f"({self.left.fingerprint()} {self.operator} {self.right.fingerprint()})"


class FunctionCall(Expression):
    """A function call expression."""

    type: TypeLiteral["function_call"] = "function_call"
    name: str = Field(description="Function name")
    args: list[Expression] = Field(default_factory=list, description="Function arguments")
    distinct: bool = Field(default=False, description="Whether DISTINCT is applied")

    def referenced_columns(self) -> set[str]:
        result: set[str] = set()
        for arg in self.args:
            result |= arg.referenced_columns()
        return result

    def fingerprint(self) -> str:
        args_str = ", ".join(a.fingerprint() for a in self.args)
        distinct_str = "DISTINCT " if self.distinct else ""
        return f"{self.name}({distinct_str}{args_str})"


class WindowFrameSpec(BaseModel):
    """Window frame specification (ROWS/RANGE/GROUPS BETWEEN)."""

    frame_type: WindowFrameType = Field(
        default=WindowFrameType.RANGE, description="Frame type (ROWS, RANGE, or GROUPS)"
    )
    start_bound: WindowFrameBound = Field(
        default=WindowFrameBound.UNBOUNDED_PRECEDING, description="Start boundary"
    )
    start_offset: int | None = Field(
        default=None, description="Offset for PRECEDING/FOLLOWING start"
    )
    end_bound: WindowFrameBound = Field(
        default=WindowFrameBound.CURRENT_ROW, description="End boundary"
    )
    end_offset: int | None = Field(default=None, description="Offset for PRECEDING/FOLLOWING end")

    def fingerprint(self) -> str:
        def bound_str(bound: WindowFrameBound, offset: int | None) -> str:
            if bound == WindowFrameBound.UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING"
            if bound == WindowFrameBound.CURRENT_ROW:
                return "CURRENT ROW"
            if bound == WindowFrameBound.UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING"
            if bound == WindowFrameBound.PRECEDING:
                return f"{offset} PRECEDING"
            if bound == WindowFrameBound.FOLLOWING:
                return f"{offset} FOLLOWING"
            return str(bound.value)

        start = bound_str(self.start_bound, self.start_offset)
        end = bound_str(self.end_bound, self.end_offset)
        return f"{self.frame_type.value.upper()} BETWEEN {start} AND {end}"


class WindowSpec(BaseModel):
    """Window specification (OVER clause)."""

    partition_by: list[Expression] = Field(
        default_factory=list, description="PARTITION BY expressions"
    )
    order_by: list[tuple[Expression, SortDirection]] = Field(
        default_factory=list, description="ORDER BY expressions with direction"
    )
    frame: WindowFrameSpec | None = Field(default=None, description="Window frame specification")

    def referenced_columns(self) -> set[str]:
        result: set[str] = set()
        for expr in self.partition_by:
            result |= expr.referenced_columns()
        for expr, _ in self.order_by:
            result |= expr.referenced_columns()
        return result

    def fingerprint(self) -> str:
        parts = []
        if self.partition_by:
            partition_str = ", ".join(e.fingerprint() for e in self.partition_by)
            parts.append(f"PARTITION BY {partition_str}")
        if self.order_by:
            order_items = []
            for expr, direction in self.order_by:
                order_items.append(f"{expr.fingerprint()} {direction.value.upper()}")
            parts.append(f"ORDER BY {', '.join(order_items)}")
        if self.frame:
            parts.append(self.frame.fingerprint())
        return " ".join(parts) if parts else ""


class WindowExpression(Expression):
    """Window function expression (function OVER window_spec)."""

    type: TypeLiteral["window_expr"] = "window_expr"
    function: FunctionCall = Field(description="The window function")
    window: WindowSpec = Field(description="Window specification")

    def referenced_columns(self) -> set[str]:
        return self.function.referenced_columns() | self.window.referenced_columns()

    def fingerprint(self) -> str:
        window_str = self.window.fingerprint()
        over_clause = f"OVER ({window_str})" if window_str else "OVER ()"
        return f"{self.function.fingerprint()} {over_clause}"


class CaseExpression(Expression):
    """CASE WHEN expression.

    Supports both simple CASE (with operand) and searched CASE (without operand).

    Simple CASE: CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END
    Searched CASE: CASE WHEN x > 0 THEN 'pos' WHEN x < 0 THEN 'neg' ELSE 'zero' END
    """

    type: TypeLiteral["case_expr"] = "case_expr"
    operand: Expression | None = Field(
        default=None, description="Simple CASE operand (None for searched CASE)"
    )
    when_clauses: list[tuple[Expression, Expression]] = Field(
        description="List of (condition, result) pairs"
    )
    else_result: Expression | None = Field(
        default=None, description="ELSE result (None if no ELSE clause)"
    )

    def referenced_columns(self) -> set[str]:
        result: set[str] = set()
        if self.operand:
            result |= self.operand.referenced_columns()
        for condition, value in self.when_clauses:
            result |= condition.referenced_columns()
            result |= value.referenced_columns()
        if self.else_result:
            result |= self.else_result.referenced_columns()
        return result

    def fingerprint(self) -> str:
        parts = ["CASE"]
        if self.operand:
            parts.append(self.operand.fingerprint())
        for condition, value in self.when_clauses:
            parts.append(f"WHEN {condition.fingerprint()} THEN {value.fingerprint()}")
        if self.else_result:
            parts.append(f"ELSE {self.else_result.fingerprint()}")
        parts.append("END")
        return " ".join(parts)


# =============================================================================
# Predicate Types
# =============================================================================


class Predicate(BaseModel, ABC):
    """Base class for predicates in selection/join conditions.

    Note: Predicate objects should be treated as immutable. Creating modified
    predicates should produce new objects rather than mutating existing ones.
    """

    @abstractmethod
    def referenced_columns(self) -> set[str]:
        """Return set of column references in this predicate."""
        ...

    @abstractmethod
    def to_cnf_atoms(self) -> list[AtomicPredicate]:
        """Convert to list of atomic predicates (CNF approximation)."""
        ...

    @abstractmethod
    def fingerprint(self) -> str:
        """Return a canonical string representation."""
        ...


class AtomicPredicate(Predicate):
    """An atomic comparison predicate."""

    type: TypeLiteral["atomic"] = "atomic"
    left: Expression = Field(description="Left side of comparison")
    op: ComparisonOp = Field(description="Comparison operator")
    right: Expression | None = Field(
        default=None, description="Right side (None for IS NULL/IS NOT NULL)"
    )

    def referenced_columns(self) -> set[str]:
        cols = self.left.referenced_columns()
        if self.right:
            cols |= self.right.referenced_columns()
        return cols

    def to_cnf_atoms(self) -> list[AtomicPredicate]:
        return [self]

    def fingerprint(self) -> str:
        left_fp = self.left.fingerprint()
        if self.op in (ComparisonOp.IS_NULL, ComparisonOp.IS_NOT_NULL):
            return f"{left_fp} {self.op.value}"
        if self.right:
            return f"{left_fp} {self.op.value} {self.right.fingerprint()}"
        return f"{left_fp} {self.op.value}"

    def involves_tables(self) -> set[str]:
        """Return set of tables involved in this predicate."""
        tables: set[str] = set()
        for col_ref in self.referenced_columns():
            if "." in col_ref:
                tables.add(col_ref.split(".")[0])
        return tables


class CompoundPredicate(Predicate):
    """A compound predicate (AND, OR, NOT)."""

    type: TypeLiteral["compound"] = "compound"
    op: LogicalOp = Field(description="Logical operator")
    operands: list[Predicate] = Field(description="Operand predicates")

    def referenced_columns(self) -> set[str]:
        result: set[str] = set()
        for operand in self.operands:
            result |= operand.referenced_columns()
        return result

    def to_cnf_atoms(self) -> list[AtomicPredicate]:
        """Approximate CNF conversion (flattens ANDs, treats ORs as single atoms)."""
        if self.op == LogicalOp.AND:
            result: list[AtomicPredicate] = []
            for operand in self.operands:
                result.extend(operand.to_cnf_atoms())
            return result
        # For OR and NOT, we can't easily decompose - return empty
        # A more sophisticated implementation would track OR groups
        return []

    def fingerprint(self) -> str:
        """Return canonical fingerprint with sorted operands for commutative operators.

        AND and OR are commutative, so (A AND B) and (B AND A) should produce
        the same fingerprint. We sort operands lexicographically by their
        fingerprints to achieve this.
        """
        if self.op == LogicalOp.NOT:
            return f"NOT ({self.operands[0].fingerprint()})"
        # Sort operands for commutative operators (AND, OR)
        if self.op in (LogicalOp.AND, LogicalOp.OR):
            sorted_operands = sorted(self.operands, key=lambda p: p.fingerprint())
            sep = f" {self.op.value.upper()} "
            return f"({sep.join(p.fingerprint() for p in sorted_operands)})"
        # Non-commutative operators (if any) preserve order
        sep = f" {self.op.value.upper()} "
        return f"({sep.join(p.fingerprint() for p in self.operands)})"


# =============================================================================
# Relational Algebra Expressions
# =============================================================================


class RAExpression(BaseModel, ABC):
    """Base class for relational algebra expressions.

    Note: This is a pure algebraic type with no SQL dependencies.
    To convert to SQL, use sqlkit.emit_sql(expr).

    Visitor Pattern:
        Use the accept() method to traverse expressions with a visitor:

            class MyVisitor(RAExpressionVisitor[str]):
                def visit_relation(self, expr: Relation) -> str:
                    return expr.name
                # ... implement other visit methods

            visitor = MyVisitor()
            result = expr.accept(visitor)
    """

    @abstractmethod
    def referenced_tables(self) -> set[str]:
        """Return set of base table names referenced.

        Note: This returns physical table names only. For alias-safe
        handling (self-joins, repeated tables), use relation_instances().
        """
        ...

    @abstractmethod
    def relation_instances(self) -> set[RelationRef]:
        """Return set of relation instances (physical table + alias).

        This provides alias-safe identity for handling self-joins and
        repeated table references. Each instance is unique by (table, alias).
        """
        ...

    @abstractmethod
    def output_columns(self) -> set[str]:
        """Return set of output column names (qualified where possible)."""
        ...

    @abstractmethod
    def fingerprint(self) -> str:
        """Return a canonical string representation for comparison."""
        ...

    def accept(self, visitor: Any) -> Any:
        """Accept a visitor and dispatch to the correct visit method.

        This is the entry point for the visitor pattern. The visitor's
        visit_* method will be called based on this expression's type.

        Args:
            visitor: An RAExpressionVisitor instance

        Returns:
            The result of the visitor's visit method
        """
        from alma_algebrakit.visitor import dispatch_accept

        return dispatch_accept(self, visitor)


# =============================================================================
# Mixins for common RA operation patterns
# =============================================================================


class UnaryOperationMixin:
    """Mixin for unary RA operations that delegate to input.

    Provides default implementations for referenced_tables() and
    relation_instances() that delegate to the input expression.
    Subclasses should define their own output_columns() if needed.
    """

    input: RAExpression

    def referenced_tables(self) -> set[str]:
        return self.input.referenced_tables()

    def relation_instances(self) -> set[RelationRef]:
        return self.input.relation_instances()


class UnaryPassthroughMixin(UnaryOperationMixin):
    """Mixin for unary ops that also pass through output_columns.

    Use for operations like Selection, Sort, Limit that don't change
    the output schema.
    """

    def output_columns(self) -> set[str]:
        return self.input.output_columns()


class BinarySetOperationMixin:
    """Mixin for binary set operations (Union, Difference, Intersect).

    Provides default implementations that union referenced_tables and
    relation_instances from both sides, and return left's output_columns.
    """

    left: RAExpression
    right: RAExpression

    def referenced_tables(self) -> set[str]:
        return self.left.referenced_tables() | self.right.referenced_tables()

    def relation_instances(self) -> set[RelationRef]:
        return self.left.relation_instances() | self.right.relation_instances()

    def output_columns(self) -> set[str]:
        return self.left.output_columns()


# =============================================================================
# RA Expression Types
# =============================================================================


class Relation(RAExpression):
    """A base relation (table scan)."""

    type: TypeLiteral["relation"] = "relation"
    name: str = Field(description="Table name")
    schema_name: str | None = Field(default=None, description="Schema name")
    alias: str | None = Field(default=None, description="Table alias")
    columns: list[str] = Field(default_factory=list, description="Known columns (if available)")

    def referenced_tables(self) -> set[str]:
        if self.schema_name:
            return {f"{self.schema_name}.{self.name}"}
        return {self.name}

    def relation_instances(self) -> set[RelationRef]:
        """Return the single relation instance for this base relation."""
        return {self.to_relation_ref()}

    def output_columns(self) -> set[str]:
        prefix = self.alias or self.name
        if self.columns:
            return {f"{prefix}.{c}" for c in self.columns}
        return {f"{prefix}.*"}

    def fingerprint(self) -> str:
        schema_part = f"{self.schema_name}." if self.schema_name else ""
        alias_part = f" AS {self.alias}" if self.alias else ""
        return f"R({schema_part}{self.name}{alias_part})"

    def effective_name(self) -> str:
        """Return the effective name (alias if present, else table name)."""
        return self.alias or self.name

    def to_relation_ref(self) -> RelationRef:
        """Convert this Relation to a RelationRef instance."""
        physical = f"{self.schema_name}.{self.name}" if self.schema_name else self.name
        return RelationRef(
            physical_table=physical,
            alias=self.alias or self.name,
        )


class Selection(UnaryPassthroughMixin, RAExpression):
    """Selection operation σ_φ(E)."""

    type: TypeLiteral["selection"] = "selection"
    predicate: Predicate = Field(description="Selection predicate φ")
    input: RAExpression = Field(description="Input expression E")

    def fingerprint(self) -> str:
        return f"σ[{self.predicate.fingerprint()}]({self.input.fingerprint()})"


class Projection(UnaryOperationMixin, RAExpression):
    """Projection operation π_A(E)."""

    type: TypeLiteral["projection"] = "projection"
    columns: list[tuple[Expression, str | None]] = Field(
        description="List of (expression, alias) pairs"
    )
    input: RAExpression = Field(description="Input expression E")
    distinct: bool = Field(default=False, description="Whether DISTINCT is applied")
    distinct_on: list[Expression] | None = Field(
        default=None, description="DISTINCT ON expressions (PostgreSQL extension)"
    )

    def output_columns(self) -> set[str]:
        result: set[str] = set()
        for expr, alias in self.columns:
            if alias:
                result.add(alias)
            elif isinstance(expr, ColumnRef):
                result.add(expr.column)
            else:
                result.add(expr.fingerprint())
        return result

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


class Join(RAExpression):
    """Join operation E_1 ⋈_θ E_2."""

    type: TypeLiteral["join"] = "join"
    left: RAExpression = Field(description="Left input E_1")
    right: RAExpression = Field(description="Right input E_2")
    join_type: JoinType = Field(default=JoinType.INNER, description="Type of join")
    condition: Predicate | None = Field(default=None, description="Join predicate θ")
    is_lateral: bool = Field(default=False, description="Whether this is a LATERAL join")

    def referenced_tables(self) -> set[str]:
        return self.left.referenced_tables() | self.right.referenced_tables()

    def relation_instances(self) -> set[RelationRef]:
        return self.left.relation_instances() | self.right.relation_instances()

    def output_columns(self) -> set[str]:
        # SEMI and ANTI joins only return columns from the left side
        if self.join_type in (JoinType.SEMI, JoinType.ANTI):
            return self.left.output_columns()
        return self.left.output_columns() | self.right.output_columns()

    def fingerprint(self) -> str:
        join_sym = {
            JoinType.INNER: "⋈",
            JoinType.LEFT: "⟕",
            JoinType.RIGHT: "⟖",
            JoinType.FULL: "⟗",
            JoinType.CROSS: "×",
            JoinType.SEMI: "⋉",  # Left semi-join symbol
            JoinType.ANTI: "▷",  # Anti-join symbol
        }[self.join_type]
        lateral_str = "LATERAL " if self.is_lateral else ""
        cond_str = f"[{self.condition.fingerprint()}]" if self.condition else ""
        return f"({self.left.fingerprint()} {join_sym}{lateral_str}{cond_str} {self.right.fingerprint()})"

    def is_inner_equijoin(self) -> bool:
        """Check if this is an inner equi-join (safe for reordering).

        Returns False for:
        - Non-inner joins
        - Joins without conditions
        - Joins with OR/NOT predicates (to_cnf_atoms returns empty)
        - Joins with non-equality conditions
        """
        if self.join_type != JoinType.INNER:
            return False
        if not self.condition:
            return False
        # Check if condition is equality-based
        atoms = self.condition.to_cnf_atoms()
        # Empty atoms means OR/NOT predicate - not safe to reorder
        if not atoms:
            return False
        return all(a.op == ComparisonOp.EQ for a in atoms)

    def is_outer_join(self) -> bool:
        """Check if this is an outer join (LEFT, RIGHT, or FULL)."""
        return self.join_type in (JoinType.LEFT, JoinType.RIGHT, JoinType.FULL)

    def is_semi_join(self) -> bool:
        """Check if this is a semi-join (EXISTS, IN with subquery)."""
        return self.join_type == JoinType.SEMI

    def is_anti_join(self) -> bool:
        """Check if this is an anti-join (NOT EXISTS, NOT IN with subquery)."""
        return self.join_type == JoinType.ANTI

    def is_filtering_join(self) -> bool:
        """Check if this is a filtering join (SEMI or ANTI).

        Filtering joins only affect row count, not schema - they don't
        add columns from the right side to the output.
        """
        return self.join_type in (JoinType.SEMI, JoinType.ANTI)


class AggregateSpec(BaseModel):
    """Specification of an aggregate in a grouping operation."""

    function: AggregateFunction = Field(description="Aggregate function")
    argument: Expression | None = Field(
        default=None, description="Argument expression (None for COUNT(*))"
    )
    alias: str = Field(description="Output column name")
    distinct: bool = Field(default=False, description="Whether DISTINCT is applied")

    def fingerprint(self) -> str:
        arg_str = self.argument.fingerprint() if self.argument else "*"
        distinct_str = "DISTINCT " if self.distinct else ""
        return f"{self.function.value}({distinct_str}{arg_str})"

    def is_rollup_safe(self) -> bool:
        """Check if this aggregate can be rolled up from finer granularity."""
        # COUNT DISTINCT and similar are NOT rollup-safe
        if self.distinct:
            return False
        return self.function in (
            AggregateFunction.SUM,
            AggregateFunction.COUNT,
            AggregateFunction.MIN,
            AggregateFunction.MAX,
        )


class Aggregation(UnaryOperationMixin, RAExpression):
    """Grouping/aggregation operation γ_{G; aggs}(E)."""

    type: TypeLiteral["aggregation"] = "aggregation"
    group_by: list[Expression] = Field(description="Group-by expressions G")
    aggregates: list[AggregateSpec] = Field(description="Aggregate specifications")
    input: RAExpression = Field(description="Input expression E")
    having: Predicate | None = Field(default=None, description="HAVING clause predicate")

    def output_columns(self) -> set[str]:
        result: set[str] = set()
        for expr in self.group_by:
            if isinstance(expr, ColumnRef):
                result.add(expr.column)
            else:
                result.add(expr.fingerprint())
        for agg in self.aggregates:
            result.add(agg.alias)
        return result

    def fingerprint(self) -> str:
        group_str = ", ".join(g.fingerprint() for g in self.group_by)
        agg_str = ", ".join(f"{a.fingerprint()} AS {a.alias}" for a in self.aggregates)
        having_str = f" HAVING {self.having.fingerprint()}" if self.having else ""
        return f"γ[{group_str}; {agg_str}{having_str}]({self.input.fingerprint()})"

    def grain_columns(self) -> list[str]:
        """Return the grain (group-by) columns as qualified names."""
        return [expr.qualified_name() for expr in self.group_by if hasattr(expr, "qualified_name")]


class Union(BinarySetOperationMixin, RAExpression):
    """Union operation E_1 ∪ E_2."""

    type: TypeLiteral["union"] = "union"
    left: RAExpression = Field(description="Left input E_1")
    right: RAExpression = Field(description="Right input E_2")
    all: bool = Field(default=False, description="UNION ALL (preserve duplicates)")

    def fingerprint(self) -> str:
        op = "∪ALL" if self.all else "∪"
        return f"({self.left.fingerprint()} {op} {self.right.fingerprint()})"


class Difference(BinarySetOperationMixin, RAExpression):
    """Difference operation E_1 - E_2."""

    type: TypeLiteral["difference"] = "difference"
    left: RAExpression = Field(description="Left input E_1")
    right: RAExpression = Field(description="Right input E_2")

    def fingerprint(self) -> str:
        return f"({self.left.fingerprint()} - {self.right.fingerprint()})"


class Intersect(BinarySetOperationMixin, RAExpression):
    """Intersection operation E_1 ∩ E_2.

    Returns tuples that appear in both inputs.
    INTERSECT removes duplicates; INTERSECT ALL preserves minimum multiplicity.
    """

    type: TypeLiteral["intersect"] = "intersect"
    left: RAExpression = Field(description="Left input E_1")
    right: RAExpression = Field(description="Right input E_2")
    all: bool = Field(default=False, description="INTERSECT ALL (preserve duplicates)")

    def fingerprint(self) -> str:
        op = "∩ALL" if self.all else "∩"
        return f"({self.left.fingerprint()} {op} {self.right.fingerprint()})"


class SortSpec(BaseModel):
    """Specification of a single sort key."""

    expression: Expression = Field(description="Sort key expression")
    direction: SortDirection = Field(default=SortDirection.ASC, description="Sort direction")
    nulls: NullsPosition | None = Field(
        default=None, description="NULLS FIRST/LAST (None = database default)"
    )

    def fingerprint(self) -> str:
        parts = [self.expression.fingerprint(), self.direction.value.upper()]
        if self.nulls:
            parts.append(f"NULLS {self.nulls.value.upper()}")
        return " ".join(parts)


class Sort(UnaryPassthroughMixin, RAExpression):
    """Sort operation (ORDER BY).

    Note: Without a deterministic ORDER BY, the result order is undefined.
    LIMIT without ORDER BY returns an arbitrary subset.
    """

    type: TypeLiteral["sort"] = "sort"
    input: RAExpression = Field(description="Input expression")
    order_by: list[SortSpec] = Field(description="Sort specifications")

    def fingerprint(self) -> str:
        order_str = ", ".join(spec.fingerprint() for spec in self.order_by)
        return f"τ[{order_str}]({self.input.fingerprint()})"


class Limit(UnaryPassthroughMixin, RAExpression):
    """Limit operation (LIMIT/OFFSET).

    Returns at most `limit` rows, optionally skipping `offset` rows.
    Without ORDER BY, the result is non-deterministic.
    """

    type: TypeLiteral["limit"] = "limit"
    input: RAExpression = Field(description="Input expression")
    limit: int | None = Field(default=None, description="Maximum rows to return")
    offset: int | None = Field(default=None, description="Rows to skip")

    def fingerprint(self) -> str:
        parts = []
        if self.limit is not None:
            parts.append(f"LIMIT {self.limit}")
        if self.offset is not None:
            parts.append(f"OFFSET {self.offset}")
        limit_str = " ".join(parts) if parts else "LIMIT ALL"
        return f"λ[{limit_str}]({self.input.fingerprint()})"


class CTEDefinition(BaseModel):
    """Definition of a single CTE (Common Table Expression)."""

    name: str = Field(description="CTE name")
    columns: list[str] | None = Field(default=None, description="Optional column aliases")
    query: RAExpression = Field(description="CTE query definition")
    recursive: bool = Field(default=False, description="Whether this CTE is recursive")

    def fingerprint(self) -> str:
        cols = f"({', '.join(self.columns)})" if self.columns else ""
        recursive = "RECURSIVE " if self.recursive else ""
        return f"{recursive}{self.name}{cols} AS ({self.query.fingerprint()})"


class WithExpression(RAExpression):
    """WITH clause (CTE container).

    Represents one or more CTEs followed by a main query.
    CTEs can reference earlier CTEs in the same WITH clause.
    Recursive CTEs can reference themselves.
    """

    type: TypeLiteral["with"] = "with"
    ctes: list[CTEDefinition] = Field(description="CTE definitions")
    main_query: RAExpression = Field(description="Main query using the CTEs")

    def referenced_tables(self) -> set[str]:
        # Include both CTE-referenced tables and main query tables
        result: set[str] = set()
        for cte in self.ctes:
            result |= cte.query.referenced_tables()
        result |= self.main_query.referenced_tables()
        # Remove CTE names (they're not base tables)
        cte_names = {cte.name for cte in self.ctes}
        return result - cte_names

    def relation_instances(self) -> set[RelationRef]:
        result: set[RelationRef] = set()
        for cte in self.ctes:
            result |= cte.query.relation_instances()
        result |= self.main_query.relation_instances()
        return result

    def output_columns(self) -> set[str]:
        return self.main_query.output_columns()

    def fingerprint(self) -> str:
        cte_str = ", ".join(cte.fingerprint() for cte in self.ctes)
        return f"WITH {cte_str} {self.main_query.fingerprint()}"

    def is_recursive(self) -> bool:
        """Check if any CTE is recursive."""
        return any(cte.recursive for cte in self.ctes)


# =============================================================================
# Subquery Expression Types
# =============================================================================
# These reference RAExpression and must be defined after it.


class SubqueryExpression(Expression):
    """Scalar subquery in expression context.

    A subquery that returns a single value, e.g.:
    SELECT (SELECT MAX(price) FROM products) as max_price FROM dual

    If the subquery returns more than one row, it's a runtime error.
    If it returns no rows, the result is NULL.
    """

    type: TypeLiteral["subquery_expr"] = "subquery_expr"
    query: RAExpression = Field(description="The subquery")

    def referenced_columns(self) -> set[str]:
        # Scalar subqueries don't expose their internal columns
        # but may reference outer columns (correlated)
        return set()

    def fingerprint(self) -> str:
        return f"({self.query.fingerprint()})"


class ExistsExpression(Expression, Predicate):
    """EXISTS subquery predicate.

    Returns TRUE if the subquery returns at least one row.
    NOT EXISTS returns TRUE if the subquery returns no rows.
    """

    type: TypeLiteral["exists_expr"] = "exists_expr"
    query: RAExpression = Field(description="The subquery")
    negated: bool = Field(default=False, description="NOT EXISTS")

    def referenced_columns(self) -> set[str]:
        return set()

    def to_cnf_atoms(self) -> list[AtomicPredicate]:
        """EXISTS cannot be decomposed to CNF atoms."""
        return []

    def fingerprint(self) -> str:
        prefix = "NOT " if self.negated else ""
        return f"{prefix}EXISTS ({self.query.fingerprint()})"


class InSubqueryExpression(Expression, Predicate):
    """IN (subquery) expression.

    Tests if the left expression is in the result of the subquery.
    NOT IN tests if the left expression is not in the result.

    Warning: NOT IN with NULLs in the subquery result can be surprising -
    if any row is NULL, the NOT IN may return UNKNOWN.
    """

    type: TypeLiteral["in_subquery"] = "in_subquery"
    left: Expression = Field(description="Left side expression")
    query: RAExpression = Field(description="The subquery")
    negated: bool = Field(default=False, description="NOT IN")

    def referenced_columns(self) -> set[str]:
        return self.left.referenced_columns()

    def to_cnf_atoms(self) -> list[AtomicPredicate]:
        """IN (subquery) cannot be decomposed to CNF atoms."""
        return []

    def fingerprint(self) -> str:
        op = "NOT IN" if self.negated else "IN"
        return f"{self.left.fingerprint()} {op} ({self.query.fingerprint()})"


# =============================================================================
# Utility Functions for Relation Identity
# =============================================================================


def build_alias_map(expr: RAExpression) -> dict[str, RelationRef]:
    """Build a mapping from alias/table name to RelationRef.

    This is useful for resolving column references to their source relations.

    Args:
        expr: An RA expression

    Returns:
        Dict mapping effective name (alias or table name) to RelationRef

    Raises:
        ValueError: If there are duplicate aliases (self-join ambiguity)
    """
    alias_map: dict[str, RelationRef] = {}
    for rel_ref in expr.relation_instances():
        effective_name = rel_ref.effective_name()
        if effective_name in alias_map:
            existing = alias_map[effective_name]
            if existing != rel_ref:
                raise ValueError(
                    f"Duplicate alias '{effective_name}' maps to different relations: "
                    f"{existing} and {rel_ref}"
                )
        alias_map[effective_name] = rel_ref
    return alias_map


def resolve_column_to_relation(
    column_ref: str,
    alias_map: dict[str, RelationRef],
) -> BoundColumnRef | None:
    """Resolve a column reference string to a BoundColumnRef.

    Args:
        column_ref: Column reference like "t.col" or "col"
        alias_map: Mapping from alias to RelationRef

    Returns:
        BoundColumnRef if resolved, None if not resolvable
    """
    if "." in column_ref:
        table_part, col_part = column_ref.split(".", 1)
        if table_part in alias_map:
            return BoundColumnRef(rel=alias_map[table_part], column=col_part)
    # Unqualified column - cannot resolve without schema info
    return None


def check_for_self_joins(expr: RAExpression) -> list[str]:
    """Check if an expression contains self-joins (same table with different aliases).

    Args:
        expr: An RA expression

    Returns:
        List of physical table names that appear multiple times
    """
    rel_instances = expr.relation_instances()
    table_counts: dict[str, int] = {}
    for rel_ref in rel_instances:
        table_counts[rel_ref.physical_table] = table_counts.get(rel_ref.physical_table, 0) + 1

    return [table for table, count in table_counts.items() if count > 1]
