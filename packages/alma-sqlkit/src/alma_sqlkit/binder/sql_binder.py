"""SQL expression binder - thin adapter over alma_algebrakit's binding primitives.

Resolves all column references to stable bound identifiers,
expands SELECT *, and tracks expression lineage.

This is a thin adapter that:
1. Uses sqlglot to parse SQL
2. Walks the AST, building alma_algebrakit.Scope objects
3. Calls alma_algebrakit.resolve() for each column reference
4. Produces alma_algebrakit.BoundQuery

All the "smart" logic (scope rules, resolution algorithm) lives in alma_algebrakit.
SQLBinder just maps sqlglot AST → alma_algebrakit structures.
"""

from __future__ import annotations

from typing import Any

import sqlglot
from alma_algebrakit.bound import (
    AttributeRef,
    BoundBetween,
    BoundBinaryOp,
    BoundComparison,
    BoundExpression,
    BoundFunctionCall,
    BoundIn,
    BoundIsNull,
    BoundLike,
    BoundLiteral,
    BoundLogical,
    BoundOrderItem,
    BoundPredicate,
    BoundQuery,
    BoundSelectItem,
    RelationBinding,
)
from alma_algebrakit.naming import (
    DEFAULT_SUBQUERY_ALIAS,
    generate_cte_id,
    generate_subquery_id,
)
from alma_algebrakit.schema import Catalog, DataType, SQLDataType
from alma_algebrakit.scope import (
    AmbiguousColumnError,
    RelationInstance,
    Scope,
    UnresolvedColumnError,
    resolve,
)
from sqlglot import exp


class BindingError(Exception):
    """Error during binding process."""

    def __init__(self, message: str, node: Any = None):
        self.node = node
        super().__init__(message)


class SQLBinder:
    """Binds SQL expressions to resolved column references.

    This is a thin adapter that uses:
    - sqlglot for parsing SQL
    - alma_algebrakit.Scope for managing scoping
    - alma_algebrakit.resolve() for column resolution
    - alma_algebrakit.BoundQuery for the result

    Usage:
        catalog = Catalog.from_dict({...})
        binder = SQLBinder(catalog)
        bound_query = binder.bind("SELECT * FROM users WHERE id = 1")
    """

    # Functions known to be volatile (non-deterministic)
    VOLATILE_FUNCTIONS = frozenset(
        {
            "now",
            "current_timestamp",
            "current_date",
            "current_time",
            "random",
            "uuid_generate_v4",
            "gen_random_uuid",
            "clock_timestamp",
            "statement_timestamp",
            "transaction_timestamp",
        }
    )

    # Known aggregate functions
    AGGREGATE_FUNCTIONS = frozenset(
        {
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "array_agg",
            "string_agg",
            "bool_and",
            "bool_or",
            "stddev",
            "variance",
            "var_pop",
            "var_samp",
            "percentile_cont",
            "percentile_disc",
            "first",
            "last",
            "any_value",
        }
    )

    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self._current_scope: Scope | None = None

    def bind(self, sql: str, dialect: str = "postgres") -> BoundQuery:
        """Bind a SQL query string.

        Args:
            sql: SQL query string
            dialect: SQL dialect for parsing

        Returns:
            BoundQuery with all references resolved

        Raises:
            BindingError: If binding fails
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except Exception as e:
            raise BindingError(f"Parse error: {e}") from e

        if not isinstance(parsed, exp.Select):
            raise BindingError(f"Expected SELECT, got {type(parsed).__name__}")

        return self._bind_select(parsed)

    def _bind_select(self, select: exp.Select) -> BoundQuery:
        """Bind a SELECT statement."""
        # Create new scope using alma_algebrakit's Scope
        scope = Scope()
        self._current_scope = scope

        # Process CTEs first
        with_clause = select.find(exp.With)
        if with_clause:
            self._bind_ctes(with_clause, scope)

        # Process FROM clause to populate scope
        from_clause = select.find(exp.From)
        if from_clause:
            self._bind_from(from_clause, scope)

        # Process JOINs
        for join in select.find_all(exp.Join):
            self._bind_join(join, scope)

        # Now bind expressions in SELECT list
        bound_columns: list[BoundSelectItem] = []
        for expr in select.expressions:
            bound_item = self._bind_select_item(expr, scope)
            bound_columns.append(bound_item)

        # Bind WHERE clause
        bound_where: BoundPredicate | None = None
        where = select.find(exp.Where)
        if where:
            bound_where = self._bind_predicate(where.this, scope)

        # Bind GROUP BY
        bound_group_by: list[BoundExpression] = []
        group = select.find(exp.Group)
        if group:
            for expr in group.expressions:
                bound_group_by.append(self._bind_expression(expr, scope))

        # Bind HAVING
        bound_having: BoundPredicate | None = None
        having = select.find(exp.Having)
        if having:
            bound_having = self._bind_predicate(having.this, scope)

        # Bind ORDER BY
        bound_order_by: list[BoundOrderItem] = []
        order = select.find(exp.Order)
        if order:
            for sort_key in order.expressions:
                bound_order_by.append(self._bind_order_item(sort_key, scope))

        # Check for DISTINCT
        is_distinct = bool(select.args.get("distinct"))

        # Check for LIMIT
        limit_value: int | None = None
        limit = select.args.get("limit")
        if limit:
            try:
                if limit.expression:
                    limit_value = int(str(limit.expression))
                elif limit.this:
                    limit_value = int(str(limit.this))
            except (ValueError, AttributeError, TypeError):
                pass

        # Convert scope relations to RelationBinding
        from_relations = [
            RelationBinding.from_instance(instance) for instance in scope.relations.values()
        ]

        return BoundQuery(
            columns=bound_columns,
            from_relations=from_relations,
            where=bound_where,
            group_by=bound_group_by,
            having=bound_having,
            order_by=bound_order_by,
            distinct=is_distinct,
            limit=limit_value,
            scope=scope,
        )

    def _bind_ctes(self, with_clause: exp.With, scope: Scope) -> None:
        """Bind CTEs into scope using alma_algebrakit's Scope."""
        for cte in with_clause.expressions:
            if not isinstance(cte, exp.CTE):
                continue

            cte_name = cte.alias
            cte_query = cte.this

            if isinstance(cte_query, exp.Select):
                # Recursively bind CTE query
                self._bind_select(cte_query)

                # Create RelationInstance for CTE
                cte_instance = RelationInstance(
                    table_id=generate_cte_id(cte_name),
                    alias=cte_name,
                    is_cte=True,
                )

                # Add CTE to scope using alma_algebrakit's scope API
                scope.add_cte(cte_name, cte_instance)

    def _bind_from(self, from_clause: exp.From, scope: Scope) -> None:
        """Bind FROM clause relations into scope."""
        table_expr = from_clause.this
        self._bind_table_expr(table_expr, scope)

    def _bind_join(self, join: exp.Join, scope: Scope) -> None:
        """Bind JOIN clause into scope."""
        self._bind_table_expr(join.this, scope)

    def _bind_table_expr(self, table_expr: exp.Expression, scope: Scope) -> None:
        """Bind a table expression (table, subquery, etc.)."""
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            alias = table_expr.alias if table_expr.alias else table_name

            # Look up in catalog
            table_schema = self.catalog.get_table(table_name)

            # Create RelationInstance using alma_algebrakit's type
            instance = RelationInstance(
                table_id=table_schema.table_id if table_schema else table_name,
                alias=alias,
                schema=table_schema,
            )

            # Add to scope using alma_algebrakit's Scope API
            scope.add_relation(instance)

        elif isinstance(table_expr, exp.Subquery):
            alias = table_expr.alias if table_expr.alias else DEFAULT_SUBQUERY_ALIAS
            inner_select = table_expr.this

            if isinstance(inner_select, exp.Select):
                # Create child scope for subquery
                scope.create_child_scope()
                self._bind_select(inner_select)

                # Create RelationInstance for subquery
                instance = RelationInstance(
                    table_id=generate_subquery_id(alias),
                    alias=alias,
                    is_subquery=True,
                )

                scope.add_relation(instance)

    def _bind_select_item(self, expr: exp.Expression, scope: Scope) -> BoundSelectItem:
        """Bind a SELECT list item."""
        alias = expr.alias if hasattr(expr, "alias") and expr.alias else None

        if isinstance(expr, exp.Alias):
            alias = expr.alias
            expr = expr.this

        if isinstance(expr, exp.Star):
            # SELECT * - expand all columns using alma_algebrakit's resolution
            from alma_algebrakit.scope.resolution import resolve_star

            expanded = resolve_star(scope)
            return BoundSelectItem(
                expression=None,
                alias=None,
                is_star=True,
                expanded_columns=expanded,
            )

        if isinstance(expr, exp.Column) and expr.name == "*" and expr.table:
            # SELECT t.* - expand columns from specific table
            table_alias = expr.table
            from alma_algebrakit.scope.resolution import resolve_star

            expanded = resolve_star(scope, table_alias)
            return BoundSelectItem(
                expression=None,
                alias=None,
                is_star=True,
                star_table=table_alias,
                expanded_columns=expanded,
            )

        # Regular expression
        bound_expr = self._bind_expression(expr, scope)
        return BoundSelectItem(
            expression=bound_expr,
            alias=alias,
            is_star=False,
        )

    def _bind_expression(self, expr: exp.Expression, scope: Scope) -> BoundExpression:
        """Bind an expression, resolving all column references."""
        if isinstance(expr, exp.Column):
            return self._bind_column(expr, scope)

        if isinstance(expr, exp.Literal):
            return self._bind_literal(expr)

        if isinstance(expr, exp.Func):
            return self._bind_function(expr, scope)

        if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
            return self._bind_binary_op(expr, scope)

        if isinstance(expr, exp.Paren):
            return self._bind_expression(expr.this, scope)

        if isinstance(expr, exp.Alias):
            return self._bind_expression(expr.this, scope)

        if isinstance(expr, exp.Null):
            return BoundLiteral(
                value=None,
                data_type=DataType(base_type=SQLDataType.UNKNOWN, nullable=True),
                sql_text="NULL",
            )

        # Default: treat as unknown expression
        return BoundExpression(
            upstream_columns=[],
            result_type=DataType(base_type=SQLDataType.UNKNOWN),
            sql_text=str(expr),
        )

    def _bind_column(self, col: exp.Column, scope: Scope) -> AttributeRef:
        """Bind a column reference using alma_algebrakit's resolve()."""
        table_alias = col.table if col.table else None
        column_name = col.name

        # Use alma_algebrakit's resolve() function
        try:
            return resolve(scope, column_name, table_alias)
        except AmbiguousColumnError as err:
            raise BindingError(f"Ambiguous column: {column_name}") from err
        except UnresolvedColumnError as err:
            raise BindingError(
                f"Unknown column: {table_alias}.{column_name}"
                if table_alias
                else f"Unknown column: {column_name}"
            ) from err

    def _bind_literal(self, lit: exp.Literal) -> BoundLiteral:
        """Bind a literal value."""
        if lit.is_string:
            return BoundLiteral(
                value=lit.this,
                data_type=DataType(base_type=SQLDataType.TEXT),
                sql_text=f"'{lit.this}'",
            )
        if lit.is_int:
            return BoundLiteral(
                value=int(lit.this),
                data_type=DataType(base_type=SQLDataType.INTEGER),
                sql_text=str(lit.this),
            )
        if lit.is_number:
            return BoundLiteral(
                value=float(lit.this),
                data_type=DataType(base_type=SQLDataType.DECIMAL),
                sql_text=str(lit.this),
            )

        return BoundLiteral(
            value=lit.this,
            data_type=DataType(base_type=SQLDataType.UNKNOWN),
            sql_text=str(lit.this),
        )

    def _bind_function(self, func: exp.Func, scope: Scope) -> BoundFunctionCall:
        """Bind a function call."""
        func_name = func.key.lower() if hasattr(func, "key") else str(type(func).__name__).lower()

        bound_args: list[BoundExpression] = []
        upstream_cols: list[AttributeRef] = []

        if hasattr(func, "expressions") and func.expressions:
            for arg in func.expressions:
                bound_arg = self._bind_expression(arg, scope)
                bound_args.append(bound_arg)
                if hasattr(bound_arg, "upstream_columns"):
                    upstream_cols.extend(bound_arg.upstream_columns)
                elif isinstance(bound_arg, AttributeRef):
                    upstream_cols.append(bound_arg)

        if hasattr(func, "this") and func.this:
            bound_arg = self._bind_expression(func.this, scope)
            bound_args.insert(0, bound_arg)
            if hasattr(bound_arg, "upstream_columns"):
                upstream_cols.extend(bound_arg.upstream_columns)
            elif isinstance(bound_arg, AttributeRef):
                upstream_cols.append(bound_arg)

        is_volatile = func_name in self.VOLATILE_FUNCTIONS
        is_aggregate = func_name in self.AGGREGATE_FUNCTIONS

        return BoundFunctionCall(
            function_name=func_name,
            args=bound_args,
            is_aggregate=is_aggregate,
            is_window=False,
            distinct=getattr(func, "distinct", False),
            upstream_columns=upstream_cols,
            result_type=DataType(base_type=SQLDataType.UNKNOWN),
            deterministic=not is_volatile,
        )

    def _bind_binary_op(self, expr: exp.Expression, scope: Scope) -> BoundBinaryOp:
        """Bind a binary operation."""
        op_map = {
            exp.Add: "+",
            exp.Sub: "-",
            exp.Mul: "*",
            exp.Div: "/",
        }
        operator = op_map.get(type(expr), "?")

        left = self._bind_expression(expr.left, scope)
        right = self._bind_expression(expr.right, scope)

        upstream = []
        for side in (left, right):
            if hasattr(side, "upstream_columns"):
                upstream.extend(side.upstream_columns)
            elif isinstance(side, AttributeRef):
                upstream.append(side)

        return BoundBinaryOp(
            operator=operator,
            left=left,
            right=right,
            upstream_columns=upstream,
            result_type=DataType(base_type=SQLDataType.UNKNOWN),
            deterministic=getattr(left, "deterministic", True)
            and getattr(right, "deterministic", True),
        )

    def _bind_predicate(self, expr: exp.Expression, scope: Scope) -> BoundPredicate:
        """Bind a predicate expression."""
        if isinstance(expr, exp.And):
            left = self._bind_predicate(expr.left, scope)
            right = self._bind_predicate(expr.right, scope)
            return BoundLogical(
                operator="AND",
                operands=[left, right],
                upstream_columns=left.upstream_columns + right.upstream_columns,
            )

        if isinstance(expr, exp.Or):
            left = self._bind_predicate(expr.left, scope)
            right = self._bind_predicate(expr.right, scope)
            return BoundLogical(
                operator="OR",
                operands=[left, right],
                upstream_columns=left.upstream_columns + right.upstream_columns,
            )

        if isinstance(expr, exp.Not):
            inner = self._bind_predicate(expr.this, scope)
            return BoundLogical(
                operator="NOT",
                operands=[inner],
                upstream_columns=inner.upstream_columns,
            )

        if isinstance(expr, exp.Paren):
            return self._bind_predicate(expr.this, scope)

        if isinstance(expr, (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE)):
            return self._bind_comparison(expr, scope)

        if isinstance(expr, exp.Is):
            bound_expr = self._bind_expression(expr.this, scope)
            negated = expr.args.get("not", False)
            upstream = (
                bound_expr.upstream_columns if hasattr(bound_expr, "upstream_columns") else []
            )
            if isinstance(bound_expr, AttributeRef):
                upstream = [bound_expr]
            return BoundIsNull(
                expression=bound_expr,
                negated=bool(negated),
                upstream_columns=upstream,
            )

        if isinstance(expr, exp.In):
            bound_expr = self._bind_expression(expr.this, scope)
            values = []
            for v in expr.expressions:
                values.append(self._bind_expression(v, scope))
            upstream = (
                bound_expr.upstream_columns if hasattr(bound_expr, "upstream_columns") else []
            )
            if isinstance(bound_expr, AttributeRef):
                upstream = [bound_expr]
            return BoundIn(
                expression=bound_expr,
                values=values,
                negated=False,
                upstream_columns=upstream,
            )

        if isinstance(expr, exp.Between):
            bound_expr = self._bind_expression(expr.this, scope)
            low = self._bind_expression(expr.args["low"], scope)
            high = self._bind_expression(expr.args["high"], scope)
            upstream = []
            for e in (bound_expr, low, high):
                if hasattr(e, "upstream_columns"):
                    upstream.extend(e.upstream_columns)
                elif isinstance(e, AttributeRef):
                    upstream.append(e)
            return BoundBetween(
                expression=bound_expr,
                low=low,
                high=high,
                upstream_columns=upstream,
            )

        if isinstance(expr, (exp.Like, exp.ILike)):
            bound_expr = self._bind_expression(expr.this, scope)
            pattern = self._bind_expression(expr.expression, scope)
            upstream = []
            if hasattr(bound_expr, "upstream_columns"):
                upstream.extend(bound_expr.upstream_columns)
            elif isinstance(bound_expr, AttributeRef):
                upstream.append(bound_expr)
            return BoundLike(
                expression=bound_expr,
                pattern=pattern,
                case_insensitive=isinstance(expr, exp.ILike),
                upstream_columns=upstream,
            )

        # Default: wrap as comparison with TRUE
        bound_expr = self._bind_expression(expr, scope)
        upstream = bound_expr.upstream_columns if hasattr(bound_expr, "upstream_columns") else []
        return BoundComparison(
            left=bound_expr,
            operator="=",
            right=BoundLiteral(
                value=True, data_type=DataType(base_type=SQLDataType.BOOLEAN), sql_text="TRUE"
            ),
            upstream_columns=upstream,
        )

    def _bind_comparison(self, expr: exp.Expression, scope: Scope) -> BoundComparison:
        """Bind a comparison expression."""
        op_map = {
            exp.EQ: "=",
            exp.NEQ: "!=",
            exp.LT: "<",
            exp.LTE: "<=",
            exp.GT: ">",
            exp.GTE: ">=",
        }
        operator = op_map.get(type(expr), "=")

        left = self._bind_expression(expr.left, scope)
        right = self._bind_expression(expr.right, scope)

        upstream = []
        for side in (left, right):
            if hasattr(side, "upstream_columns"):
                upstream.extend(side.upstream_columns)
            elif isinstance(side, AttributeRef):
                upstream.append(side)

        return BoundComparison(
            left=left,
            operator=operator,
            right=right,
            upstream_columns=upstream,
        )

    def _bind_order_item(self, sort_key: exp.Expression, scope: Scope) -> BoundOrderItem:
        """Bind an ORDER BY item."""
        if isinstance(sort_key, exp.Ordered):
            bound_expr = self._bind_expression(sort_key.this, scope)
            descending = sort_key.args.get("desc", False)
            nulls_first = sort_key.args.get("nulls_first")
            return BoundOrderItem(
                expression=bound_expr,
                descending=bool(descending),
                nulls_first=nulls_first,
            )

        bound_expr = self._bind_expression(sort_key, scope)
        return BoundOrderItem(expression=bound_expr, descending=False)
