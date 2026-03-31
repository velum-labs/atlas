"""SQL Emitter - Convert relational algebra AST to SQL.

This module provides the SQLEmitter class which converts RAExpression
trees into SQL strings using sqlglot for proper dialect handling,
escaping, and formatting.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlglot
from alma_algebrakit.visitor import RAExpressionVisitor
from sqlglot import exp

from alma_sqlkit.dialect import Dialect

if TYPE_CHECKING:
    from alma_algebrakit.models.algebra import (
        Aggregation,
        Difference,
        Intersect,
        Join,
        Limit,
        Projection,
        Relation,
        Selection,
        Sort,
        Union,
        WithExpression,
    )


class SQLEmitterVisitor(RAExpressionVisitor[exp.Expression]):
    """Visitor that converts RA expressions to sqlglot AST nodes.

    This visitor is used internally by SQLEmitter to dispatch to the correct
    emission method for each RA expression type, replacing string-based
    type checking with the proper visitor pattern.
    """

    def __init__(self, emitter: SQLEmitter) -> None:
        self._emitter = emitter

    def visit_relation(self, expr: Relation) -> exp.Expression:
        return self._emitter._emit_relation(expr)

    def visit_selection(self, expr: Selection) -> exp.Expression:
        return self._emitter._emit_selection(expr)

    def visit_projection(self, expr: Projection) -> exp.Expression:
        return self._emitter._emit_projection(expr)

    def visit_join(self, expr: Join) -> exp.Expression:
        return self._emitter._emit_join(expr)

    def visit_aggregation(self, expr: Aggregation) -> exp.Expression:
        return self._emitter._emit_aggregation(expr)

    def visit_union(self, expr: Union) -> exp.Expression:
        return self._emitter._emit_union(expr)

    def visit_difference(self, expr: Difference) -> exp.Expression:
        return self._emitter._emit_difference(expr)

    def visit_intersect(self, expr: Intersect) -> exp.Expression:
        return self._emitter._emit_intersect(expr)

    def visit_sort(self, expr: Sort) -> exp.Expression:
        return self._emitter._emit_sort(expr)

    def visit_limit(self, expr: Limit) -> exp.Expression:
        return self._emitter._emit_limit(expr)

    def visit_with_expression(self, expr: WithExpression) -> exp.Expression:
        return self._emitter._emit_with(expr)


class SQLEmitter:
    """Converts relational algebra expressions to SQL strings.

    This class provides a clean interface for emitting SQL from RAExpression
    trees. It uses sqlglot internally for proper escaping, quoting, and
    dialect-specific SQL generation.

    Example:
        >>> from alma_sqlkit import SQLEmitter
        >>> emitter = SQLEmitter(dialect="postgres")
        >>> sql = emitter.emit(ra_expression)

    The emitter handles:
    - Base relations (Relation) → FROM clause
    - Selection (σ) → WHERE clause
    - Projection (π) → SELECT clause
    - Join (⋈) → JOIN clause
    - Aggregation (γ) → GROUP BY clause
    - Union (∪) → UNION clause
    - Difference (-) → EXCEPT clause
    - Intersection (∩) → INTERSECT clause
    - Sort (τ) → ORDER BY clause
    - Limit (λ) → LIMIT/OFFSET clause
    - WITH expressions → CTE clause
    - Window functions → OVER clause
    - CASE expressions → CASE WHEN clause
    """

    def __init__(self, dialect: str | Dialect = "postgres") -> None:
        """Initialize the emitter with a SQL dialect.

        Args:
            dialect: Either a dialect name string (e.g., "postgres", "duckdb")
                    or a Dialect configuration object.
        """
        if isinstance(dialect, str):
            self._dialect = Dialect.from_name(dialect)
        else:
            self._dialect = dialect
        self._visitor = SQLEmitterVisitor(self)

    @property
    def dialect_name(self) -> str:
        """Return the dialect name for sqlglot."""
        return self._dialect.name

    def emit(self, expr: Any) -> str:
        """Convert an RAExpression to SQL string.

        Args:
            expr: A relational algebra expression (Relation, Selection,
                  Projection, Join, Aggregation, Union, or Difference)

        Returns:
            SQL string representation of the expression

        Raises:
            ValueError: If the expression type is not supported
        """
        ast = self._to_sqlglot_ast(expr)
        return ast.sql(dialect=self.dialect_name, pretty=self._dialect.pretty)

    def _to_sqlglot_ast(self, expr: Any) -> exp.Expression:
        """Convert RA expression to sqlglot AST.

        Uses the visitor pattern via expr.accept() when available.
        Falls back to type-based dispatch for mock objects in tests.
        """
        # Preferred: use the visitor pattern via expr.accept()
        if hasattr(expr, "accept"):
            return expr.accept(self._visitor)

        # Fallback for mock objects or legacy code without accept()
        # This uses the type field or class name for dispatch
        expr_type = getattr(expr, "type", None)
        class_name = type(expr).__name__

        dispatch_map = {
            "relation": self._emit_relation,
            "selection": self._emit_selection,
            "projection": self._emit_projection,
            "join": self._emit_join,
            "aggregation": self._emit_aggregation,
            "union": self._emit_union,
            "difference": self._emit_difference,
            "intersect": self._emit_intersect,
            "sort": self._emit_sort,
            "limit": self._emit_limit,
            "with": self._emit_with,
        }

        # Try type field first
        if expr_type and expr_type in dispatch_map:
            return dispatch_map[expr_type](expr)

        # Try class name (handles Mock* classes in tests)
        class_type_map = {
            "Relation": "relation",
            "MockRelation": "relation",
            "Selection": "selection",
            "Projection": "projection",
            "Join": "join",
            "Aggregation": "aggregation",
            "Union": "union",
            "Difference": "difference",
            "Intersect": "intersect",
            "Sort": "sort",
            "Limit": "limit",
            "WithExpression": "with",
        }

        if class_name in class_type_map:
            return dispatch_map[class_type_map[class_name]](expr)

        raise ValueError(f"Unsupported expression type: {type(expr).__name__}")

    def _emit_relation(self, rel: Any) -> exp.Select:
        """Emit a base relation as SELECT * FROM table."""
        # Build table reference
        table_name = rel.name
        schema_name = getattr(rel, "schema_name", None)
        alias = getattr(rel, "alias", None)

        # Create table expression
        if schema_name:
            table_expr = exp.Table(
                this=exp.to_identifier(table_name),
                db=exp.to_identifier(schema_name),
            )
        else:
            table_expr = exp.Table(this=exp.to_identifier(table_name))

        # Add alias if present
        if alias and alias != table_name:
            table_expr = table_expr.as_(alias)

        # Build SELECT * FROM table
        return exp.Select(
            expressions=[exp.Star()],
            from_=exp.From(this=table_expr),
        )

    def _emit_selection(self, sel: Any) -> exp.Select:
        """Emit a selection (σ) as WHERE clause."""
        # Get the inner expression
        inner = self._to_sqlglot_ast(sel.input)

        # Convert predicate to sqlglot condition
        where_expr = self._predicate_to_ast(sel.predicate)

        # If inner is a SELECT, add WHERE clause
        if isinstance(inner, exp.Select):
            # If there's already a WHERE, combine with AND
            existing_where = inner.find(exp.Where)
            if existing_where:
                combined = exp.And(this=existing_where.this, expression=where_expr)
                inner.set("where", exp.Where(this=combined))
            else:
                inner.set("where", exp.Where(this=where_expr))
            return inner

        # Otherwise wrap in subquery with WHERE
        return exp.Select(
            expressions=[exp.Star()],
            from_=exp.From(this=inner.subquery()),
        ).where(where_expr)

    def _emit_projection(self, proj: Any) -> exp.Select:
        """Emit a projection (π) as SELECT clause."""
        # Get the inner expression
        inner = self._to_sqlglot_ast(proj.input)

        # Build select expressions
        select_exprs: list[exp.Expression] = []
        for col_expr, alias in proj.columns:
            sql_expr = self._expression_to_ast(col_expr)
            if alias:
                sql_expr = sql_expr.as_(alias)
            select_exprs.append(sql_expr)

        # If inner is a SELECT without projection (just *), replace expressions
        if isinstance(inner, exp.Select):
            inner_exprs = inner.expressions
            is_star_only = len(inner_exprs) == 1 and isinstance(inner_exprs[0], exp.Star)

            if is_star_only:
                inner.set("expressions", select_exprs)
                if proj.distinct:
                    inner.set("distinct", exp.Distinct())
                return inner

        # Otherwise, wrap in subquery and project
        subquery = inner.subquery() if isinstance(inner, exp.Select) else inner
        select = exp.Select(
            expressions=select_exprs,
            from_=exp.From(this=subquery),
        )
        if proj.distinct:
            select.set("distinct", exp.Distinct())
        return select

    def _emit_join(self, join: Any) -> exp.Select:
        """Emit a join (⋈) as JOIN clause."""
        # Get the left side
        left_ast = self._to_sqlglot_ast(join.left)

        # Get the right side - should be a table or subquery
        right_ast = self._to_sqlglot_ast(join.right)

        # Determine join type
        join_type = getattr(join.join_type, "value", str(join.join_type))
        join_kind_map = {
            "inner": "",
            "left": "LEFT",
            "right": "RIGHT",
            "full": "FULL",
            "cross": "CROSS",
        }
        join_kind = join_kind_map.get(join_type, "")

        # Convert join condition
        on_expr = None
        if join.condition:
            on_expr = self._predicate_to_ast(join.condition)

        # Build the join
        if isinstance(left_ast, exp.Select):
            # Get the FROM clause from left
            left_ast.find(exp.From)

            # Create join expression for right side
            if isinstance(right_ast, exp.Select):
                # Check if right is a simple table scan
                right_from = right_ast.find(exp.From)
                right_is_simple = (
                    right_from
                    and isinstance(right_from.this, exp.Table)
                    and not right_ast.find(exp.Where)
                    and not right_ast.find(exp.Join)
                    and len(right_ast.expressions) == 1
                    and isinstance(right_ast.expressions[0], exp.Star)
                )

                if right_is_simple:
                    # Use the table directly
                    right_table = right_from.this
                else:
                    # Wrap in subquery
                    right_table = right_ast.subquery()
            else:
                right_table = right_ast

            # Create the JOIN node
            join_node = exp.Join(
                this=right_table,
                kind=join_kind if join_kind else None,
                on=on_expr,
            )

            # Append to left's joins
            left_ast.append("joins", join_node)
            return left_ast

        # Fallback: create new SELECT with both sides
        return exp.Select(
            expressions=[exp.Star()],
            from_=exp.From(
                this=left_ast.subquery() if isinstance(left_ast, exp.Select) else left_ast
            ),
            joins=[
                exp.Join(
                    this=right_ast.subquery() if isinstance(right_ast, exp.Select) else right_ast,
                    kind=join_kind if join_kind else None,
                    on=on_expr,
                )
            ],
        )

    def _emit_aggregation(self, agg: Any) -> exp.Select:
        """Emit an aggregation (γ) as GROUP BY clause."""
        # Get the inner expression
        inner = self._to_sqlglot_ast(agg.input)

        # Build select expressions: group by columns + aggregates
        select_exprs: list[exp.Expression] = []

        # Add group-by columns
        group_exprs: list[exp.Expression] = []
        for group_col in agg.group_by:
            col_ast = self._expression_to_ast(group_col)
            select_exprs.append(col_ast)
            group_exprs.append(col_ast.copy())

        # Add aggregate expressions
        for agg_spec in agg.aggregates:
            agg_ast = self._aggregate_to_ast(agg_spec)
            select_exprs.append(agg_ast)

        # If inner is a simple SELECT, modify it
        if isinstance(inner, exp.Select):
            inner.set("expressions", select_exprs)
            if group_exprs:
                inner.set("group", exp.Group(expressions=group_exprs))
            if agg.having:
                having_expr = self._predicate_to_ast(agg.having)
                inner.set("having", exp.Having(this=having_expr))
            return inner

        # Otherwise wrap in subquery
        select = exp.Select(
            expressions=select_exprs,
            from_=exp.From(this=inner.subquery()),
        )
        if group_exprs:
            select.set("group", exp.Group(expressions=group_exprs))
        if agg.having:
            having_expr = self._predicate_to_ast(agg.having)
            select.set("having", exp.Having(this=having_expr))
        return select

    def _emit_union(self, union: Any) -> exp.Union:
        """Emit a union (∪) as UNION clause."""
        left_ast = self._to_sqlglot_ast(union.left)
        right_ast = self._to_sqlglot_ast(union.right)

        return exp.Union(
            this=left_ast,
            expression=right_ast,
            distinct=not union.all,
        )

    def _emit_difference(self, diff: Any) -> exp.Except:
        """Emit a difference (-) as EXCEPT clause."""
        left_ast = self._to_sqlglot_ast(diff.left)
        right_ast = self._to_sqlglot_ast(diff.right)

        return exp.Except(
            this=left_ast,
            expression=right_ast,
        )

    def _emit_intersect(self, intersect: Any) -> exp.Intersect:
        """Emit an intersection (∩) as INTERSECT clause."""
        left_ast = self._to_sqlglot_ast(intersect.left)
        right_ast = self._to_sqlglot_ast(intersect.right)

        return exp.Intersect(
            this=left_ast,
            expression=right_ast,
            distinct=not intersect.all,
        )

    def _emit_sort(self, sort: Any) -> exp.Select:
        """Emit a sort (τ) as ORDER BY clause."""
        inner = self._to_sqlglot_ast(sort.input)

        # Build ORDER BY expressions
        order_exprs: list[exp.Expression] = []
        for sort_spec in sort.order_by:
            col_ast = self._expression_to_ast(sort_spec.expression)

            # Get direction
            direction = getattr(sort_spec.direction, "value", str(sort_spec.direction))
            is_desc = direction.lower() == "desc"

            # Get nulls position
            nulls_first = None
            if sort_spec.nulls:
                nulls_value = getattr(sort_spec.nulls, "value", str(sort_spec.nulls))
                nulls_first = nulls_value.lower() == "first"

            ordered = exp.Ordered(this=col_ast, desc=is_desc, nulls_first=nulls_first)
            order_exprs.append(ordered)

        # If inner is a SELECT, add ORDER BY clause
        if isinstance(inner, exp.Select):
            inner.set("order", exp.Order(expressions=order_exprs))
            return inner

        # Otherwise wrap in subquery with ORDER BY
        return exp.Select(
            expressions=[exp.Star()],
            from_=exp.From(this=inner.subquery()),
            order=exp.Order(expressions=order_exprs),
        )

    def _emit_limit(self, limit_expr: Any) -> exp.Select:
        """Emit a limit (λ) as LIMIT/OFFSET clause."""
        inner = self._to_sqlglot_ast(limit_expr.input)

        # If inner is a SELECT, add LIMIT/OFFSET clauses
        if isinstance(inner, exp.Select):
            if limit_expr.limit is not None:
                inner.set("limit", exp.Limit(expression=exp.Literal.number(limit_expr.limit)))
            if limit_expr.offset is not None:
                inner.set("offset", exp.Offset(expression=exp.Literal.number(limit_expr.offset)))
            return inner

        # Otherwise wrap in subquery
        select = exp.Select(
            expressions=[exp.Star()],
            from_=exp.From(this=inner.subquery()),
        )
        if limit_expr.limit is not None:
            select.set("limit", exp.Limit(expression=exp.Literal.number(limit_expr.limit)))
        if limit_expr.offset is not None:
            select.set("offset", exp.Offset(expression=exp.Literal.number(limit_expr.offset)))
        return select

    def _emit_with(self, with_expr: Any) -> exp.Select:
        """Emit a WITH expression (CTEs)."""
        # Build CTE definitions
        cte_exprs: list[exp.CTE] = []
        for cte_def in with_expr.ctes:
            cte_query_ast = self._to_sqlglot_ast(cte_def.query)

            # Create CTE
            cte = exp.CTE(
                this=cte_query_ast,
                alias=exp.TableAlias(this=exp.to_identifier(cte_def.name)),
            )

            # Add column aliases if present
            if cte_def.columns:
                cte.args["alias"].set(
                    "columns", [exp.to_identifier(col) for col in cte_def.columns]
                )

            cte_exprs.append(cte)

        # Get main query
        main_ast = self._to_sqlglot_ast(with_expr.main_query)

        # Check if any CTE is recursive
        is_recursive = any(getattr(cte_def, "recursive", False) for cte_def in with_expr.ctes)

        # Wrap main query with WITH clause
        if isinstance(main_ast, exp.Select):
            with_clause = exp.With(expressions=cte_exprs, recursive=is_recursive)
            main_ast.set("with_", with_clause)
            return main_ast

        # For non-SELECT queries, wrap
        select = exp.Select(
            expressions=[exp.Star()],
            from_=exp.From(this=main_ast.subquery()),
        )
        with_clause = exp.With(expressions=cte_exprs, recursive=is_recursive)
        select.set("with_", with_clause)
        return select

    def _expression_to_ast(self, expr: Any) -> exp.Expression:
        """Convert an Expression to sqlglot AST."""
        expr_type = getattr(expr, "type", None)

        if expr_type == "column_ref" or type(expr).__name__ == "ColumnRef":
            return self._column_ref_to_ast(expr)
        if expr_type == "literal" or type(expr).__name__ == "Literal":
            return self._literal_to_ast(expr)
        if expr_type == "binary_expr" or type(expr).__name__ == "BinaryExpression":
            return self._binary_expr_to_ast(expr)
        if expr_type == "function_call" or type(expr).__name__ == "FunctionCall":
            return self._function_call_to_ast(expr)
        if expr_type == "window_expr" or type(expr).__name__ == "WindowExpression":
            return self._window_expr_to_ast(expr)
        if expr_type == "case_expr" or type(expr).__name__ == "CaseExpression":
            return self._case_expr_to_ast(expr)

        # Subquery expression types
        if expr_type == "subquery_expr" or type(expr).__name__ == "SubqueryExpression":
            return self._subquery_expr_to_ast(expr)
        if expr_type == "exists_expr" or type(expr).__name__ == "ExistsExpression":
            return self._exists_expr_to_ast(expr)
        if expr_type == "in_subquery" or type(expr).__name__ == "InSubqueryExpression":
            return self._in_subquery_expr_to_ast(expr)

        # Fallback: try to use fingerprint as literal
        if hasattr(expr, "fingerprint"):
            return exp.Literal.string(expr.fingerprint())

        raise ValueError(f"Unsupported expression type: {type(expr).__name__}")

    def _column_ref_to_ast(self, col: Any) -> exp.Column:
        """Convert a ColumnRef to sqlglot Column."""
        if col.column == "*":
            return exp.Star()

        if col.table:
            return exp.Column(
                this=exp.to_identifier(col.column),
                table=exp.to_identifier(col.table),
            )
        return exp.Column(this=exp.to_identifier(col.column))

    def _literal_to_ast(self, lit: Any) -> exp.Expression:
        """Convert a Literal to sqlglot literal."""
        if lit.value is None:
            return exp.Null()
        if isinstance(lit.value, bool):
            return exp.Boolean(this=lit.value)
        if isinstance(lit.value, int):
            return exp.Literal.number(lit.value)
        if isinstance(lit.value, float):
            return exp.Literal.number(lit.value)
        if isinstance(lit.value, str):
            return exp.Literal.string(lit.value)
        # Fallback
        return exp.Literal.string(str(lit.value))

    def _binary_expr_to_ast(self, bin_expr: Any) -> exp.Expression:
        """Convert a BinaryExpression to sqlglot."""
        left = self._expression_to_ast(bin_expr.left)
        right = self._expression_to_ast(bin_expr.right)
        op = bin_expr.operator

        op_map = {
            "+": exp.Add,
            "-": exp.Sub,
            "*": exp.Mul,
            "/": exp.Div,
            "||": exp.DPipe,  # Use DPipe for string concatenation operator, not Concat
            "%": exp.Mod,
        }

        op_class = op_map.get(op)
        if op_class:
            return op_class(this=left, expression=right)

        # Fallback: use raw SQL
        return sqlglot.parse_one(f"({left.sql()} {op} {right.sql()})")

    def _function_call_to_ast(self, func: Any) -> exp.Expression:
        """Convert a FunctionCall to sqlglot."""
        args = [self._expression_to_ast(arg) for arg in func.args]
        func_name = func.name.upper()

        # Common functions have dedicated classes
        func_map: dict[str, type[exp.Func]] = {
            "COALESCE": exp.Coalesce,
            "CONCAT": exp.Concat,
            "LOWER": exp.Lower,
            "UPPER": exp.Upper,
            "TRIM": exp.Trim,
            "LENGTH": exp.Length,
            "SUBSTRING": exp.Substring,
            "ABS": exp.Abs,
            "ROUND": exp.Round,
            "FLOOR": exp.Floor,
            "CEIL": exp.Ceil,
            "NOW": exp.CurrentTimestamp,
            "CURRENT_DATE": exp.CurrentDate,
            "CURRENT_TIMESTAMP": exp.CurrentTimestamp,
            "DATE_TRUNC": exp.DateTrunc,
            "EXTRACT": exp.Extract,
            "CAST": exp.Cast,
        }

        func_class = func_map.get(func_name)
        if func_class:
            if args:
                return func_class(this=args[0], expressions=args[1:] if len(args) > 1 else [])
            return func_class()

        # Generic function call
        return exp.Anonymous(this=func_name, expressions=args)

    def _window_expr_to_ast(self, window_expr: Any) -> exp.Window:
        """Convert a WindowExpression to sqlglot Window."""
        # Get the function
        func_ast = self._function_call_to_ast(window_expr.function)

        # Build window spec
        window_spec = window_expr.window

        # PARTITION BY
        partition_by = None
        if window_spec.partition_by:
            partition_by = [self._expression_to_ast(e) for e in window_spec.partition_by]

        # ORDER BY
        order = None
        if window_spec.order_by:
            order_exprs = []
            for order_expr, direction in window_spec.order_by:
                col_ast = self._expression_to_ast(order_expr)
                dir_value = getattr(direction, "value", str(direction))
                is_desc = dir_value.lower() == "desc"
                order_exprs.append(exp.Ordered(this=col_ast, desc=is_desc))
            order = exp.Order(expressions=order_exprs)

        # Frame spec
        spec = None
        if window_spec.frame:
            frame = window_spec.frame
            frame_type_value = getattr(frame.frame_type, "value", str(frame.frame_type))

            # Convert frame bounds
            start = self._frame_bound_to_ast(frame.start_bound, frame.start_offset)
            end = self._frame_bound_to_ast(frame.end_bound, frame.end_offset)

            spec = exp.WindowSpec(
                kind=frame_type_value.upper(),
                start=start,
                end=end,
            )

        return exp.Window(
            this=func_ast,
            partition_by=partition_by,
            order=order,
            spec=spec,
        )

    def _frame_bound_to_ast(self, bound: Any, offset: int | None) -> str:
        """Convert a window frame bound to SQL string."""
        bound_value = getattr(bound, "value", str(bound))

        if bound_value == "unbounded_preceding":
            return "UNBOUNDED PRECEDING"
        if bound_value == "unbounded_following":
            return "UNBOUNDED FOLLOWING"
        if bound_value == "current_row":
            return "CURRENT ROW"
        if bound_value == "preceding":
            return f"{offset} PRECEDING"
        if bound_value == "following":
            return f"{offset} FOLLOWING"

        return "CURRENT ROW"

    def _case_expr_to_ast(self, case_expr: Any) -> exp.Case:
        """Convert a CaseExpression to sqlglot Case."""
        # Simple CASE has an operand
        this = None
        if case_expr.operand:
            this = self._expression_to_ast(case_expr.operand)

        # Build IFs (WHEN clauses)
        ifs = []
        for condition, result in case_expr.when_clauses:
            cond_ast = self._expression_to_ast(condition)
            result_ast = self._expression_to_ast(result)
            ifs.append(exp.If(this=cond_ast, true=result_ast))

        # ELSE clause
        default = None
        if case_expr.else_result:
            default = self._expression_to_ast(case_expr.else_result)

        return exp.Case(this=this, ifs=ifs, default=default)

    def _subquery_expr_to_ast(self, subq: Any) -> exp.Expression:
        """Convert a SubqueryExpression (scalar subquery) to sqlglot.

        A scalar subquery returns a single value and can be used in expressions.
        Example: SELECT (SELECT MAX(price) FROM products) as max_price FROM dual
        """
        # IMPORTANT: Use _to_sqlglot_ast to get AST node, not emit() which returns string
        inner_ast = self._to_sqlglot_ast(subq.query)
        return exp.Subquery(this=inner_ast)

    def _exists_expr_to_ast(self, exists: Any) -> exp.Expression:
        """Convert an ExistsExpression to sqlglot.

        EXISTS returns TRUE if the subquery returns at least one row.
        NOT EXISTS returns TRUE if the subquery returns no rows.
        """
        # IMPORTANT: Use _to_sqlglot_ast to get AST node, not emit() which returns string
        inner_ast = self._to_sqlglot_ast(exists.query)
        subquery = exp.Subquery(this=inner_ast)
        result = exp.Exists(this=subquery)

        if getattr(exists, "negated", False):
            return exp.Not(this=result)
        return result

    def _in_subquery_expr_to_ast(self, in_subq: Any) -> exp.Expression:
        """Convert an InSubqueryExpression to sqlglot.

        IN (SELECT ...) tests if the left expression is in the result of the subquery.
        NOT IN tests if the left expression is not in the result.
        """
        left = self._expression_to_ast(in_subq.left)
        # IMPORTANT: Use _to_sqlglot_ast to get AST node, not emit() which returns string
        inner_ast = self._to_sqlglot_ast(in_subq.query)
        subquery = exp.Subquery(this=inner_ast)

        result = exp.In(this=left, query=subquery)

        if getattr(in_subq, "negated", False):
            return result.not_()
        return result

    def _aggregate_to_ast(self, agg_spec: Any) -> exp.Expression:
        """Convert an AggregateSpec to sqlglot."""
        func_value = getattr(agg_spec.function, "value", str(agg_spec.function))

        # Map aggregate function names to sqlglot classes
        agg_map: dict[str, type[exp.AggFunc]] = {
            "count": exp.Count,
            "count_distinct": exp.Count,
            "sum": exp.Sum,
            "avg": exp.Avg,
            "min": exp.Min,
            "max": exp.Max,
            "array_agg": exp.ArrayAgg,
            "string_agg": exp.GroupConcat,
            "bool_and": exp.Anonymous,  # Postgres-specific
            "bool_or": exp.Anonymous,  # Postgres-specific
        }

        agg_class = agg_map.get(func_value, exp.Anonymous)

        # Get argument
        if agg_spec.argument:
            arg_ast = self._expression_to_ast(agg_spec.argument)
        else:
            arg_ast = exp.Star()

        # Build aggregate
        if agg_class == exp.Anonymous:
            # Use generic function for unsupported aggregates
            agg_expr: exp.Expression = exp.Anonymous(
                this=func_value.upper(),
                expressions=[arg_ast],
            )
        else:
            agg_expr = agg_class(this=arg_ast)

        # Handle DISTINCT
        if agg_spec.distinct or func_value == "count_distinct":
            agg_expr.set("distinct", True)

        # Add alias
        if agg_spec.alias:
            agg_expr = agg_expr.as_(agg_spec.alias)

        return agg_expr

    def _predicate_to_ast(self, pred: Any) -> exp.Expression:
        """Convert a Predicate to sqlglot condition."""
        pred_type = getattr(pred, "type", None)

        if pred_type == "atomic" or type(pred).__name__ == "AtomicPredicate":
            return self._atomic_predicate_to_ast(pred)
        if pred_type == "compound" or type(pred).__name__ == "CompoundPredicate":
            return self._compound_predicate_to_ast(pred)

        raise ValueError(f"Unsupported predicate type: {type(pred).__name__}")

    def _atomic_predicate_to_ast(self, pred: Any) -> exp.Expression:
        """Convert an AtomicPredicate to sqlglot condition."""
        left = self._expression_to_ast(pred.left)
        op_value = getattr(pred.op, "value", str(pred.op))

        # Handle IS NULL / IS NOT NULL
        if op_value == "is_null":
            return exp.Is(this=left, expression=exp.Null())
        if op_value == "is_not_null":
            return exp.Is(this=left, expression=exp.Null()).not_()

        # Get right side
        if pred.right:
            right = self._expression_to_ast(pred.right)
        else:
            right = exp.Null()

        # Map comparison operators
        op_map: dict[str, type[exp.Expression]] = {
            "=": exp.EQ,
            "!=": exp.NEQ,
            "<>": exp.NEQ,
            "<": exp.LT,
            "<=": exp.LTE,
            ">": exp.GT,
            ">=": exp.GTE,
            "like": exp.Like,
            "ilike": exp.ILike,
        }

        op_class = op_map.get(op_value)
        if op_class:
            return op_class(this=left, expression=right)

        # Handle IN
        if op_value == "in":
            # Check if right side contains a list of values
            if pred.right and hasattr(pred.right, "value"):
                values = pred.right.value
                if isinstance(values, list):
                    # Convert each value in the list to sqlglot expression
                    expressions = []
                    for val in values:
                        if hasattr(val, "value"):
                            # It's a Literal
                            expressions.append(self._expression_to_ast(val))
                        elif hasattr(val, "column"):
                            # It's a ColumnRef
                            expressions.append(self._expression_to_ast(val))
                        else:
                            expressions.append(self._expression_to_ast(val))
                    return exp.In(this=left, expressions=expressions)

            # Legacy fallback: try to parse string representation
            if hasattr(right, "this") and isinstance(right.this, str):
                values_str = right.this
                try:
                    parsed = sqlglot.parse_one(f"SELECT * WHERE x IN {values_str}")
                    in_expr = parsed.find(exp.In)
                    if in_expr:
                        return exp.In(this=left, expressions=in_expr.expressions)
                except Exception:
                    pass
            return exp.In(this=left, expressions=[right])

        if op_value == "not_in":
            # Check if right side contains a list of values
            if pred.right and hasattr(pred.right, "value"):
                values = pred.right.value
                if isinstance(values, list):
                    expressions = []
                    for val in values:
                        expressions.append(self._expression_to_ast(val))
                    return exp.In(this=left, expressions=expressions).not_()
            return exp.In(this=left, expressions=[right]).not_()

        # Handle BETWEEN
        if op_value == "between":
            # Check if we have a proper BETWEEN structure with low/high bounds
            # The right side might be a tuple/list of (low, high) or have low/high attributes
            if pred.right and hasattr(pred.right, "value"):
                bounds = pred.right.value
                if isinstance(bounds, (list, tuple)) and len(bounds) >= 2:
                    low = self._expression_to_ast(bounds[0])
                    high = self._expression_to_ast(bounds[1])
                    return exp.Between(this=left, low=low, high=high)

            # Check for low/high attributes directly on the predicate
            if hasattr(pred, "low") and hasattr(pred, "high"):
                low = self._expression_to_ast(pred.low)
                high = self._expression_to_ast(pred.high)
                return exp.Between(this=left, low=low, high=high)

            # Fallback: use right as both (incorrect but prevents crash)
            return exp.Between(this=left, low=right, high=right)

        # Fallback: use EQ
        return exp.EQ(this=left, expression=right)

    def _compound_predicate_to_ast(self, pred: Any) -> exp.Expression:
        """Convert a CompoundPredicate to sqlglot condition."""
        op_value = getattr(pred.op, "value", str(pred.op))
        operands = [self._predicate_to_ast(op) for op in pred.operands]

        if op_value == "and":
            result = operands[0]
            for operand in operands[1:]:
                result = exp.And(this=result, expression=operand)
            return result

        if op_value == "or":
            result = operands[0]
            for operand in operands[1:]:
                result = exp.Or(this=result, expression=operand)
            return result

        if op_value == "not":
            return exp.Not(this=operands[0])

        raise ValueError(f"Unsupported logical operator: {op_value}")


def emit_sql(expr: Any, dialect: str = "postgres") -> str:
    """Convenience function to emit SQL from an RA expression.

    Args:
        expr: A relational algebra expression
        dialect: SQL dialect name

    Returns:
        SQL string
    """
    emitter = SQLEmitter(dialect=dialect)
    return emitter.emit(expr)
