"""SQL Builder - Fluent API for constructing SQL queries.

This module provides the SQLBuilder class which offers a type-safe,
fluent interface for building SQL queries programmatically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Self

import sqlglot
from sqlglot import exp

from alma_sqlkit.dialect import Dialect


@dataclass
class JoinSpec:
    """Specification for a JOIN clause."""

    table: str
    alias: str | None
    on: str
    join_type: str  # INNER, LEFT, RIGHT, FULL, CROSS


@dataclass
class SQLBuilderState:
    """Internal state for SQLBuilder."""

    select_columns: list[str] = field(default_factory=list)
    select_distinct: bool = False
    from_table: str | None = None
    from_alias: str | None = None
    joins: list[JoinSpec] = field(default_factory=list)
    where_conditions: list[str] = field(default_factory=list)
    group_by_columns: list[str] = field(default_factory=list)
    having_conditions: list[str] = field(default_factory=list)
    order_by_columns: list[tuple[str, str]] = field(default_factory=list)  # (column, direction)
    limit_value: int | None = None
    offset_value: int | None = None


class SQLBuilder:
    """Fluent builder for constructing SQL queries.

    Provides a chainable API for building SELECT statements with
    proper escaping and dialect-specific SQL generation via sqlglot.

    Example:
        >>> sql = (
        ...     SQLBuilder(dialect="postgres")
        ...     .select("id", "name", "email")
        ...     .from_table("users", alias="u")
        ...     .join("orders", on="u.id = orders.user_id", join_type="LEFT")
        ...     .where("u.active = true")
        ...     .where("orders.amount > 100")
        ...     .group_by("u.id")
        ...     .order_by("u.name")
        ...     .limit(10)
        ...     .build()
        ... )
    """

    def __init__(self, dialect: str | Dialect = "postgres") -> None:
        """Initialize the builder with a SQL dialect.

        Args:
            dialect: Either a dialect name string or a Dialect object.
        """
        if isinstance(dialect, str):
            self._dialect = Dialect.from_name(dialect)
        else:
            self._dialect = dialect
        self._state = SQLBuilderState()

    def select(self, *columns: str) -> Self:
        """Add columns to the SELECT clause.

        Args:
            columns: Column names or expressions to select.
                    Use "*" for all columns.

        Returns:
            Self for method chaining
        """
        self._state.select_columns.extend(columns)
        return self

    def select_distinct(self, *columns: str) -> Self:
        """Add DISTINCT columns to the SELECT clause.

        Args:
            columns: Column names or expressions to select.

        Returns:
            Self for method chaining
        """
        self._state.select_distinct = True
        self._state.select_columns.extend(columns)
        return self

    def from_table(self, table: str, alias: str | None = None) -> Self:
        """Set the FROM clause table.

        Args:
            table: Table name
            alias: Optional table alias

        Returns:
            Self for method chaining
        """
        self._state.from_table = table
        self._state.from_alias = alias
        return self

    def join(
        self,
        table: str,
        on: str,
        alias: str | None = None,
        join_type: str = "INNER",
    ) -> Self:
        """Add a JOIN clause.

        Args:
            table: Table to join
            on: Join condition (e.g., "t1.id = t2.foreign_id")
            alias: Optional table alias
            join_type: Type of join (INNER, LEFT, RIGHT, FULL, CROSS)

        Returns:
            Self for method chaining
        """
        self._state.joins.append(
            JoinSpec(
                table=table,
                alias=alias,
                on=on,
                join_type=join_type.upper(),
            )
        )
        return self

    def left_join(self, table: str, on: str, alias: str | None = None) -> Self:
        """Add a LEFT JOIN clause.

        Args:
            table: Table to join
            on: Join condition
            alias: Optional table alias

        Returns:
            Self for method chaining
        """
        return self.join(table, on, alias, "LEFT")

    def right_join(self, table: str, on: str, alias: str | None = None) -> Self:
        """Add a RIGHT JOIN clause.

        Args:
            table: Table to join
            on: Join condition
            alias: Optional table alias

        Returns:
            Self for method chaining
        """
        return self.join(table, on, alias, "RIGHT")

    def full_join(self, table: str, on: str, alias: str | None = None) -> Self:
        """Add a FULL OUTER JOIN clause.

        Args:
            table: Table to join
            on: Join condition
            alias: Optional table alias

        Returns:
            Self for method chaining
        """
        return self.join(table, on, alias, "FULL")

    def cross_join(self, table: str, alias: str | None = None) -> Self:
        """Add a CROSS JOIN clause.

        Args:
            table: Table to join
            alias: Optional table alias

        Returns:
            Self for method chaining
        """
        self._state.joins.append(
            JoinSpec(
                table=table,
                alias=alias,
                on="",  # CROSS JOIN has no ON condition
                join_type="CROSS",
            )
        )
        return self

    def where(self, *conditions: str) -> Self:
        """Add WHERE conditions (ANDed together).

        Args:
            conditions: SQL condition expressions

        Returns:
            Self for method chaining
        """
        self._state.where_conditions.extend(conditions)
        return self

    def group_by(self, *columns: str) -> Self:
        """Add GROUP BY columns.

        Args:
            columns: Column names or expressions to group by

        Returns:
            Self for method chaining
        """
        self._state.group_by_columns.extend(columns)
        return self

    def having(self, *conditions: str) -> Self:
        """Add HAVING conditions (ANDed together).

        Args:
            conditions: SQL condition expressions

        Returns:
            Self for method chaining
        """
        self._state.having_conditions.extend(conditions)
        return self

    def order_by(self, column: str, direction: str = "ASC") -> Self:
        """Add ORDER BY column.

        Args:
            column: Column name or expression
            direction: Sort direction (ASC or DESC)

        Returns:
            Self for method chaining
        """
        self._state.order_by_columns.append((column, direction.upper()))
        return self

    def order_by_desc(self, column: str) -> Self:
        """Add ORDER BY column with DESC direction.

        Args:
            column: Column name or expression

        Returns:
            Self for method chaining
        """
        return self.order_by(column, "DESC")

    def limit(self, value: int) -> Self:
        """Set LIMIT clause.

        Args:
            value: Maximum number of rows

        Returns:
            Self for method chaining
        """
        self._state.limit_value = value
        return self

    def offset(self, value: int) -> Self:
        """Set OFFSET clause.

        Args:
            value: Number of rows to skip

        Returns:
            Self for method chaining
        """
        self._state.offset_value = value
        return self

    def build(self) -> str:
        """Build and return the SQL string.

        Returns:
            The constructed SQL query string

        Raises:
            ValueError: If required clauses are missing
        """
        ast = self._build_ast()
        return ast.sql(dialect=self._dialect.name, pretty=self._dialect.pretty)

    def build_ast(self) -> exp.Select:
        """Build and return the sqlglot AST.

        Returns:
            The constructed sqlglot Select expression

        Raises:
            ValueError: If required clauses are missing
        """
        return self._build_ast()

    def _build_ast(self) -> exp.Select:
        """Internal method to build the sqlglot AST."""
        # Validate required parts
        if not self._state.from_table:
            raise ValueError("FROM clause is required. Call from_table() first.")

        # Build SELECT expressions
        if not self._state.select_columns:
            select_exprs = [exp.Star()]
        else:
            select_exprs = []
            for col in self._state.select_columns:
                if col == "*":
                    select_exprs.append(exp.Star())
                else:
                    # Parse the column expression
                    try:
                        parsed = sqlglot.parse_one(f"SELECT {col}")
                        if isinstance(parsed, exp.Select) and parsed.expressions:
                            select_exprs.append(parsed.expressions[0])
                        else:
                            select_exprs.append(exp.Column(this=exp.to_identifier(col)))
                    except Exception:
                        select_exprs.append(exp.Column(this=exp.to_identifier(col)))

        # Build FROM clause
        from_table = exp.Table(this=exp.to_identifier(self._state.from_table))
        if self._state.from_alias:
            from_table = from_table.as_(self._state.from_alias)

        # Create the SELECT
        select = exp.Select(
            expressions=select_exprs,
            from_=exp.From(this=from_table),
        )

        # Add DISTINCT
        if self._state.select_distinct:
            select.set("distinct", exp.Distinct())

        # Add JOINs
        for join_spec in self._state.joins:
            join_table = exp.Table(this=exp.to_identifier(join_spec.table))
            if join_spec.alias:
                join_table = join_table.as_(join_spec.alias)

            # Parse ON condition
            on_expr = None
            if join_spec.on and join_spec.join_type != "CROSS":
                try:
                    parsed = sqlglot.parse_one(f"SELECT * FROM t WHERE {join_spec.on}")
                    where = parsed.find(exp.Where)
                    if where:
                        on_expr = where.this
                except Exception:
                    # Fallback: use raw string
                    on_expr = exp.Literal.string(join_spec.on)

            join_node = exp.Join(
                this=join_table,
                kind=join_spec.join_type if join_spec.join_type != "INNER" else None,
                on=on_expr,
            )
            select.append("joins", join_node)

        # Add WHERE
        if self._state.where_conditions:
            where_expr = self._parse_conditions(self._state.where_conditions)
            select.set("where", exp.Where(this=where_expr))

        # Add GROUP BY
        if self._state.group_by_columns:
            group_exprs = []
            for col in self._state.group_by_columns:
                try:
                    parsed = sqlglot.parse_one(f"SELECT {col}")
                    if isinstance(parsed, exp.Select) and parsed.expressions:
                        group_exprs.append(parsed.expressions[0])
                    else:
                        group_exprs.append(exp.Column(this=exp.to_identifier(col)))
                except Exception:
                    group_exprs.append(exp.Column(this=exp.to_identifier(col)))
            select.set("group", exp.Group(expressions=group_exprs))

        # Add HAVING
        if self._state.having_conditions:
            having_expr = self._parse_conditions(self._state.having_conditions)
            select.set("having", exp.Having(this=having_expr))

        # Add ORDER BY
        if self._state.order_by_columns:
            order_exprs = []
            for col, direction in self._state.order_by_columns:
                try:
                    parsed = sqlglot.parse_one(f"SELECT {col}")
                    if isinstance(parsed, exp.Select) and parsed.expressions:
                        col_expr = parsed.expressions[0]
                    else:
                        col_expr = exp.Column(this=exp.to_identifier(col))
                except Exception:
                    col_expr = exp.Column(this=exp.to_identifier(col))

                order_expr = exp.Ordered(this=col_expr, desc=direction == "DESC")
                order_exprs.append(order_expr)
            select.set("order", exp.Order(expressions=order_exprs))

        # Add LIMIT
        if self._state.limit_value is not None:
            select.set("limit", exp.Limit(expression=exp.Literal.number(self._state.limit_value)))

        # Add OFFSET
        if self._state.offset_value is not None:
            select.set(
                "offset", exp.Offset(expression=exp.Literal.number(self._state.offset_value))
            )

        return select

    def _parse_conditions(self, conditions: list[str]) -> exp.Expression:
        """Parse and AND-combine multiple conditions."""
        parsed_conditions: list[exp.Expression] = []

        for condition in conditions:
            try:
                parsed = sqlglot.parse_one(f"SELECT * FROM t WHERE {condition}")
                where = parsed.find(exp.Where)
                if where:
                    parsed_conditions.append(where.this)
                else:
                    # Try parsing as expression directly
                    parsed_conditions.append(exp.Literal.string(condition))
            except Exception:
                parsed_conditions.append(exp.Literal.string(condition))

        if len(parsed_conditions) == 1:
            return parsed_conditions[0]

        # Combine with AND
        result = parsed_conditions[0]
        for cond in parsed_conditions[1:]:
            result = exp.And(this=result, expression=cond)
        return result

    def copy(self) -> SQLBuilder:
        """Create a copy of this builder with the same state.

        Returns:
            A new SQLBuilder with copied state
        """
        new_builder = SQLBuilder(dialect=self._dialect)
        new_builder._state = SQLBuilderState(
            select_columns=list(self._state.select_columns),
            select_distinct=self._state.select_distinct,
            from_table=self._state.from_table,
            from_alias=self._state.from_alias,
            joins=list(self._state.joins),
            where_conditions=list(self._state.where_conditions),
            group_by_columns=list(self._state.group_by_columns),
            having_conditions=list(self._state.having_conditions),
            order_by_columns=list(self._state.order_by_columns),
            limit_value=self._state.limit_value,
            offset_value=self._state.offset_value,
        )
        return new_builder


def build_sql(dialect: str = "postgres") -> SQLBuilder:
    """Create a new SQLBuilder instance.

    Args:
        dialect: SQL dialect name

    Returns:
        A new SQLBuilder
    """
    return SQLBuilder(dialect=dialect)
