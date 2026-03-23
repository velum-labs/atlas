"""SQL to Relational Algebra parser.

Parses SQL queries into canonical relational algebra (RA) expressions using sqlglot.
This is a thin adapter that uses sqlglot for parsing and produces alma_algebrakit types.
"""

from __future__ import annotations

import sqlglot
from alma_algebrakit import (
    AggregateFunction,
    AggregateSpec,
    Aggregation,
    AtomicPredicate,
    BinaryExpression,
    CaseExpression,
    ColumnRef,
    ComparisonOp,
    CompoundPredicate,
    CTEDefinition,
    Difference,
    Expression,
    FunctionCall,
    Intersect,
    Join,
    JoinType,
    Limit,
    Literal,
    LogicalOp,
    NullsPosition,
    Predicate,
    Projection,
    RAExpression,
    Relation,
    Selection,
    Sort,
    SortDirection,
    SortSpec,
    Union,
    WindowExpression,
    WindowFrameBound,
    WindowFrameSpec,
    WindowFrameType,
    WindowSpec,
    WithExpression,
)
from pydantic import BaseModel, Field
from sqlglot import exp
from sqlglot.errors import ParseError


class ParsingConfig(BaseModel):
    """Configuration for SQL parsing."""

    dialect: str = Field(default="postgres", description="SQL dialect for parsing")
    normalize: bool = Field(default=True, description="Normalize SQL before parsing")


class SQLParser:
    """Parser for converting SQL to relational algebra expressions.

    This is a thin adapter that uses sqlglot for parsing and produces
    alma_algebrakit RA expressions.

    Example:
        >>> parser = SQLParser()
        >>> ra_expr = parser.parse("SELECT * FROM users WHERE id = 1")
        >>> print(ra_expr.fingerprint())
    """

    def __init__(self, config: ParsingConfig | None = None) -> None:
        self.config = config or ParsingConfig()
        self._dialect = self.config.dialect

    def parse(self, sql: str) -> RAExpression:
        """Parse SQL string into a relational algebra expression.

        Args:
            sql: SQL query string

        Returns:
            RAExpression representing the query

        Raises:
            ValueError: If SQL cannot be parsed
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self._dialect)
        except ParseError as e:
            raise ValueError(f"Failed to parse SQL: {e}") from e

        # Handle WITH clause (CTEs) first
        if hasattr(parsed, "find") and parsed.find(exp.With):
            return self._convert_with(parsed)

        if isinstance(parsed, exp.Select):
            return self._convert_select_with_ordering(parsed)
        if isinstance(parsed, exp.Union):
            return self._convert_union(parsed)
        if isinstance(parsed, exp.Intersect):
            return self._convert_intersect(parsed)
        if isinstance(parsed, exp.Except):
            return self._convert_except(parsed)

        raise ValueError(
            f"Expected SELECT, UNION, INTERSECT, or EXCEPT statement, got {type(parsed).__name__}"
        )

    def _convert_select(self, select: exp.Select) -> RAExpression:
        """Convert a SELECT expression to RA."""
        from_clause = select.find(exp.From)
        if not from_clause:
            # SELECT without FROM (e.g. SELECT 1, SELECT CURRENT_DATE())
            # Create a virtual dual relation as the base
            base_expr: RAExpression = Relation(name="__dual__")
            base_expr = self._convert_projection(select, base_expr)
            return base_expr

        base_expr = self._convert_from(from_clause)

        for join in select.find_all(exp.Join):
            base_expr = self._convert_join(base_expr, join)

        where = select.find(exp.Where)
        if where:
            predicate = self._convert_predicate(where.this)
            base_expr = Selection(predicate=predicate, input=base_expr)

        group_by = select.find(exp.Group)
        having = select.find(exp.Having)

        if group_by:
            base_expr = self._convert_aggregation(select, group_by, having, base_expr)
        else:
            base_expr = self._convert_projection(select, base_expr)

        return base_expr

    def _convert_select_with_ordering(self, select: exp.Select) -> RAExpression:
        """Convert a SELECT expression with ORDER BY and LIMIT to RA."""
        # First, convert the core SELECT
        base_expr = self._convert_select(select)

        # Then wrap with Sort if ORDER BY exists
        order = select.find(exp.Order)
        if order:
            base_expr = self._convert_order_by(order, base_expr)

        # Then wrap with Limit if LIMIT/OFFSET exists
        limit_clause = select.find(exp.Limit)
        offset_clause = select.find(exp.Offset)
        if limit_clause or offset_clause:
            base_expr = self._convert_limit(limit_clause, offset_clause, base_expr)

        return base_expr

    def _convert_order_by(self, order: exp.Order, input_expr: RAExpression) -> Sort:
        """Convert ORDER BY clause to Sort RA expression."""
        sort_specs: list[SortSpec] = []

        for ordered_expr in order.expressions:
            if isinstance(ordered_expr, exp.Ordered):
                expr = self._convert_expression(ordered_expr.this)

                # Get sort direction
                direction = SortDirection.ASC
                if ordered_expr.args.get("desc"):
                    direction = SortDirection.DESC

                # Get NULLS position
                nulls = None
                nulls_first = ordered_expr.args.get("nulls_first")
                if nulls_first is True:
                    nulls = NullsPosition.FIRST
                elif nulls_first is False:
                    nulls = NullsPosition.LAST

                sort_specs.append(
                    SortSpec(
                        expression=expr,
                        direction=direction,
                        nulls=nulls,
                    )
                )
            else:
                # Plain expression without ORDER modifier
                expr = self._convert_expression(ordered_expr)
                sort_specs.append(SortSpec(expression=expr, direction=SortDirection.ASC))

        return Sort(input=input_expr, order_by=sort_specs)

    def _convert_limit(
        self,
        limit_clause: exp.Limit | None,
        offset_clause: exp.Offset | None,
        input_expr: RAExpression,
    ) -> Limit:
        """Convert LIMIT/OFFSET to Limit RA expression."""
        limit_val = None
        offset_val = None

        # In sqlglot, the limit value is in .expression, not .this
        if limit_clause:
            limit_expr = limit_clause.expression
            if isinstance(limit_expr, exp.Literal) and limit_expr.is_int:
                limit_val = int(limit_expr.this)

        if offset_clause:
            offset_expr = offset_clause.expression
            if isinstance(offset_expr, exp.Literal) and offset_expr.is_int:
                offset_val = int(offset_expr.this)

        return Limit(input=input_expr, limit=limit_val, offset=offset_val)

    def _convert_union(self, union_expr: exp.Union) -> RAExpression:
        """Convert a UNION expression to RA Union."""
        left_ra = self._convert_set_operation_branch(union_expr.left)
        right_ra = self._convert_set_operation_branch(union_expr.right)

        is_union_all = union_expr.args.get("distinct") is False

        return Union(left=left_ra, right=right_ra, all=is_union_all)

    def _convert_intersect(self, intersect_expr: exp.Intersect) -> RAExpression:
        """Convert an INTERSECT expression to RA Intersect."""
        left_ra = self._convert_set_operation_branch(intersect_expr.left)
        right_ra = self._convert_set_operation_branch(intersect_expr.right)

        is_all = intersect_expr.args.get("distinct") is False

        return Intersect(left=left_ra, right=right_ra, all=is_all)

    def _convert_except(self, except_expr: exp.Except) -> RAExpression:
        """Convert an EXCEPT expression to RA Difference."""
        left_ra = self._convert_set_operation_branch(except_expr.left)
        right_ra = self._convert_set_operation_branch(except_expr.right)

        # Note: Difference doesn't support ALL in our model
        return Difference(left=left_ra, right=right_ra)

    def _convert_set_operation_branch(self, expr: exp.Expression) -> RAExpression:
        """Convert a branch of a set operation (UNION/INTERSECT/EXCEPT)."""
        if isinstance(expr, exp.Select):
            return self._convert_select_with_ordering(expr)
        if isinstance(expr, exp.Union):
            return self._convert_union(expr)
        if isinstance(expr, exp.Intersect):
            return self._convert_intersect(expr)
        if isinstance(expr, exp.Except):
            return self._convert_except(expr)
        if isinstance(expr, exp.Subquery):
            return self._convert_set_operation_branch(expr.this)
        raise ValueError(f"Unexpected set operation branch: {type(expr)}")

    def _convert_with(self, parsed: exp.Expression) -> RAExpression:
        """Convert a query with WITH clause (CTEs) to WithExpression."""
        with_clause = parsed.find(exp.With)
        if not with_clause:
            raise ValueError("Expected WITH clause")

        cte_defs: list[CTEDefinition] = []
        is_recursive = bool(with_clause.args.get("recursive", False))

        for cte in with_clause.expressions:
            if isinstance(cte, exp.CTE):
                name = cte.alias

                # Get column aliases if specified
                columns = None
                if cte.args.get("alias") and hasattr(cte.args["alias"], "columns"):
                    columns = [col.name for col in cte.args["alias"].columns]

                # Parse the CTE query
                cte_query = cte.this
                if isinstance(cte_query, exp.Select):
                    query_ra = self._convert_select_with_ordering(cte_query)
                elif isinstance(cte_query, exp.Union):
                    query_ra = self._convert_union(cte_query)
                else:
                    query_ra = self._convert_set_operation_branch(cte_query)

                cte_defs.append(
                    CTEDefinition(
                        name=name,
                        columns=columns,
                        query=query_ra,
                        recursive=is_recursive,
                    )
                )

        # Parse the main query (strip the WITH clause for parsing)
        main_query = parsed
        if isinstance(main_query, exp.Select):
            main_ra = self._convert_select_with_ordering(main_query)
        elif isinstance(main_query, exp.Union):
            main_ra = self._convert_union(main_query)
        else:
            main_ra = self._convert_set_operation_branch(main_query)

        return WithExpression(ctes=cte_defs, main_query=main_ra)

    def _convert_from(self, from_clause: exp.From) -> RAExpression:
        """Convert FROM clause to RA expression."""
        table_expr = from_clause.this
        return self._convert_table_expr(table_expr)

    def _convert_table_expr(self, table_expr: exp.Expression) -> RAExpression:
        """Convert a table expression to RA."""
        if isinstance(table_expr, exp.Table):
            return Relation(
                name=table_expr.name,
                schema_name=table_expr.db if table_expr.db else None,
                alias=table_expr.alias if table_expr.alias else None,
            )
        if isinstance(table_expr, exp.Subquery):
            inner = table_expr.this
            if isinstance(inner, exp.Select):
                return self._convert_select(inner)
            if isinstance(inner, exp.Union):
                return self._convert_union(inner)
            if isinstance(inner, exp.Intersect):
                return self._convert_intersect(inner)
            if isinstance(inner, exp.Except):
                return self._convert_except(inner)
            raise ValueError(f"Unexpected subquery content: {type(inner)}")
        if isinstance(table_expr, exp.Unnest):
            # BigQuery UNNEST(array_expr) — treat as a virtual relation
            alias_node = table_expr.args.get("alias")
            name = str(alias_node.this) if alias_node else "unnest"
            return Relation(name=name, alias=name)
        raise ValueError(f"Unexpected table expression: {type(table_expr)}")

    def _convert_join(self, left: RAExpression, join: exp.Join) -> Join:
        """Convert a JOIN to RA Join expression."""
        right = self._convert_table_expr(join.this)

        join_type = JoinType.INNER
        if join.side:
            side = str(join.side).upper()
            if side == "LEFT":
                join_type = JoinType.LEFT
            elif side == "RIGHT":
                join_type = JoinType.RIGHT
            elif side == "FULL":
                join_type = JoinType.FULL
        if join.kind and str(join.kind).upper() == "CROSS":
            join_type = JoinType.CROSS

        condition = None
        on_expr = join.args.get("on")
        if on_expr:
            condition = self._convert_predicate(on_expr)

        return Join(
            left=left,
            right=right,
            join_type=join_type,
            condition=condition,
        )

    def _convert_predicate(self, expr: exp.Expression) -> Predicate:
        """Convert an expression to a Predicate."""
        if isinstance(expr, exp.And):
            operands = [self._convert_predicate(e) for e in [expr.left, expr.right]]
            return CompoundPredicate(op=LogicalOp.AND, operands=operands)

        if isinstance(expr, exp.Or):
            operands = [self._convert_predicate(e) for e in [expr.left, expr.right]]
            return CompoundPredicate(op=LogicalOp.OR, operands=operands)

        if isinstance(expr, exp.Not):
            operands = [self._convert_predicate(expr.this)]
            return CompoundPredicate(op=LogicalOp.NOT, operands=operands)

        if isinstance(expr, exp.Paren):
            return self._convert_predicate(expr.this)

        if isinstance(expr, (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE)):
            return self._convert_comparison(expr)

        if isinstance(expr, exp.Like):
            left = self._convert_expression(expr.this)
            right = self._convert_expression(expr.expression)
            return AtomicPredicate(left=left, op=ComparisonOp.LIKE, right=right)

        if isinstance(expr, exp.ILike):
            left = self._convert_expression(expr.this)
            right = self._convert_expression(expr.expression)
            return AtomicPredicate(left=left, op=ComparisonOp.ILIKE, right=right)

        if isinstance(expr, exp.In):
            left = self._convert_expression(expr.this)
            is_negated = expr.args.get("not", False)
            subquery_expr = expr.args.get("query")

            # Handle IN (subquery) vs IN (value_list)
            if subquery_expr is not None:
                # IN (SELECT ...) - subquery case
                # For now, convert to a placeholder - full subquery support is complex
                right = Literal(value="__subquery__", data_type="subquery")
                op = ComparisonOp.NOT_IN if is_negated else ComparisonOp.IN
                return AtomicPredicate(left=left, op=op, right=right)

            # IN (value1, value2, ...) - preserve as list of values
            values = []
            for val_expr in expr.expressions or []:
                converted_val = self._convert_expression(val_expr)
                values.append(converted_val)

            # Create a Literal with the actual list of converted expressions
            # Store the list of values, not a string representation
            right = Literal(value=values, data_type="list")
            op = ComparisonOp.NOT_IN if is_negated else ComparisonOp.IN
            return AtomicPredicate(left=left, op=op, right=right)

        if isinstance(expr, exp.Is):
            left = self._convert_expression(expr.this)
            if isinstance(expr.expression, exp.Null):
                op = ComparisonOp.IS_NOT_NULL if expr.args.get("not") else ComparisonOp.IS_NULL
                return AtomicPredicate(left=left, op=op, right=None)
            right = self._convert_expression(expr.expression)
            return AtomicPredicate(left=left, op=ComparisonOp.EQ, right=right)

        if isinstance(expr, exp.Between):
            left = self._convert_expression(expr.this)
            low_pred = AtomicPredicate(
                left=left,
                op=ComparisonOp.GE,
                right=self._convert_expression(expr.args["low"]),
            )
            high_pred = AtomicPredicate(
                left=left,
                op=ComparisonOp.LE,
                right=self._convert_expression(expr.args["high"]),
            )
            return CompoundPredicate(op=LogicalOp.AND, operands=[low_pred, high_pred])

        left = self._convert_expression(expr)
        return AtomicPredicate(left=left, op=ComparisonOp.EQ, right=Literal(value=True))

    def _convert_comparison(self, expr: exp.Expression) -> AtomicPredicate:
        """Convert a comparison expression to AtomicPredicate."""
        left = self._convert_expression(expr.left)
        right = self._convert_expression(expr.right)

        op_map = {
            exp.EQ: ComparisonOp.EQ,
            exp.NEQ: ComparisonOp.NE,
            exp.LT: ComparisonOp.LT,
            exp.LTE: ComparisonOp.LE,
            exp.GT: ComparisonOp.GT,
            exp.GTE: ComparisonOp.GE,
        }
        op = op_map.get(type(expr), ComparisonOp.EQ)

        return AtomicPredicate(left=left, op=op, right=right)

    def _convert_expression(self, expr: exp.Expression) -> Expression:
        """Convert a sqlglot expression to alma_algebrakit Expression type."""
        if isinstance(expr, exp.Column):
            return ColumnRef(
                table=expr.table if expr.table else None,
                column=expr.name,
            )

        if isinstance(expr, exp.Literal):
            if expr.is_string:
                return Literal(value=expr.this, data_type="string")
            if expr.is_int:
                return Literal(value=int(expr.this), data_type="integer")
            if expr.is_number:
                return Literal(value=float(expr.this), data_type="number")
            return Literal(value=expr.this)

        if isinstance(expr, exp.Null):
            return Literal(value=None, data_type="null")

        if isinstance(expr, exp.Boolean):
            return Literal(value=expr.this, data_type="boolean")

        if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
            op_map = {exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/"}
            return BinaryExpression(
                left=self._convert_expression(expr.left),
                operator=op_map[type(expr)],
                right=self._convert_expression(expr.right),
            )

        if isinstance(expr, exp.Concat):
            # Concat can be binary (expr.left/right) or variadic (expr.expressions)
            if expr.expressions:
                args = [self._convert_expression(arg) for arg in expr.expressions]
                if expr.this:
                    args = [self._convert_expression(expr.this)] + args
                return FunctionCall(name="CONCAT", args=args)
            # Binary form: left || right
            return BinaryExpression(
                left=self._convert_expression(expr.left),
                operator="||",
                right=self._convert_expression(expr.right),
            )

        if isinstance(expr, exp.Window):
            return self._convert_window_function(expr)

        if isinstance(expr, exp.Case):
            return self._convert_case_expression(expr)

        if isinstance(expr, exp.Func):
            args = (
                [self._convert_expression(arg) for arg in expr.expressions]
                if expr.expressions
                else []
            )
            if hasattr(expr, "this") and expr.this:
                args = [self._convert_expression(expr.this)] + args
            return FunctionCall(
                name=expr.key.upper(),
                args=args,
                distinct=getattr(expr, "distinct", False),
            )

        if isinstance(expr, exp.Paren):
            return self._convert_expression(expr.this)

        if isinstance(expr, exp.Star):
            return ColumnRef(column="*")

        if isinstance(expr, exp.Alias):
            return self._convert_expression(expr.this)

        return Literal(value=str(expr), data_type="unknown")

    def _convert_projection(self, select: exp.Select, input_expr: RAExpression) -> Projection:
        """Convert SELECT expressions to Projection."""
        columns: list[tuple[Expression, str | None]] = []
        distinct = select.args.get("distinct", False)

        for expr in select.expressions:
            converted = self._convert_expression(expr)
            alias = expr.alias if hasattr(expr, "alias") and expr.alias else None
            columns.append((converted, alias))

        return Projection(columns=columns, input=input_expr, distinct=bool(distinct))

    def _convert_aggregation(
        self,
        select: exp.Select,
        group_by: exp.Group,
        having: exp.Having | None,
        input_expr: RAExpression,
    ) -> Aggregation:
        """Convert GROUP BY with aggregates to Aggregation."""
        group_exprs: list[Expression] = []
        for expr in group_by.expressions:
            group_exprs.append(self._convert_expression(expr))

        aggregates: list[AggregateSpec] = []

        for select_expr in select.expressions:
            if self._is_aggregate(select_expr):
                agg_spec = self._convert_aggregate(select_expr)
                if agg_spec:
                    aggregates.append(agg_spec)

        having_pred = None
        if having:
            having_pred = self._convert_predicate(having.this)

        return Aggregation(
            group_by=group_exprs,
            aggregates=aggregates,
            input=input_expr,
            having=having_pred,
        )

    def _is_aggregate(self, expr: exp.Expression) -> bool:
        """Check if expression contains an aggregate function."""
        agg_funcs = {
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "ARRAY_AGG",
            "STRING_AGG",
            "BOOL_AND",
            "BOOL_OR",
        }

        if isinstance(expr, exp.Alias):
            return self._is_aggregate(expr.this)

        if isinstance(expr, exp.Func):
            if expr.key.upper() in agg_funcs:
                return True

        for child in expr.iter_expressions():
            if self._is_aggregate(child):
                return True

        return False

    def _convert_aggregate(self, expr: exp.Expression) -> AggregateSpec | None:
        """Convert an aggregate expression to AggregateSpec."""
        alias = expr.alias if hasattr(expr, "alias") and expr.alias else "agg"

        if isinstance(expr, exp.Alias):
            alias = expr.alias
            expr = expr.this

        if not isinstance(expr, exp.Func):
            return None

        func_name = expr.key.upper()
        func_map = {
            "COUNT": AggregateFunction.COUNT,
            "SUM": AggregateFunction.SUM,
            "AVG": AggregateFunction.AVG,
            "MIN": AggregateFunction.MIN,
            "MAX": AggregateFunction.MAX,
            "ARRAY_AGG": AggregateFunction.ARRAY_AGG,
            "STRING_AGG": AggregateFunction.STRING_AGG,
            "BOOL_AND": AggregateFunction.BOOL_AND,
            "BOOL_OR": AggregateFunction.BOOL_OR,
        }

        if func_name not in func_map:
            return None

        agg_func = func_map[func_name]
        distinct = getattr(expr, "distinct", False)

        arg = None
        if hasattr(expr, "this") and expr.this:
            if not isinstance(expr.this, exp.Star):
                arg = self._convert_expression(expr.this)

        if func_name == "COUNT" and distinct:
            agg_func = AggregateFunction.COUNT_DISTINCT

        return AggregateSpec(
            function=agg_func,
            argument=arg,
            alias=alias,
            distinct=bool(distinct),
        )

    def _convert_window_function(self, window_expr: exp.Window) -> WindowExpression:
        """Convert a window function expression to WindowExpression."""
        # Get the function being windowed
        func_expr = window_expr.this
        if isinstance(func_expr, exp.Func):
            args = (
                [self._convert_expression(arg) for arg in func_expr.expressions]
                if func_expr.expressions
                else []
            )
            if hasattr(func_expr, "this") and func_expr.this:
                args = [self._convert_expression(func_expr.this)] + args
            function = FunctionCall(
                name=func_expr.key.upper(),
                args=args,
                distinct=getattr(func_expr, "distinct", False),
            )
        else:
            function = FunctionCall(name="UNKNOWN", args=[self._convert_expression(func_expr)])

        # Parse PARTITION BY
        partition_by: list[Expression] = []
        partition_clause = window_expr.args.get("partition_by")
        if partition_clause:
            for part_expr in partition_clause:
                partition_by.append(self._convert_expression(part_expr))

        # Parse ORDER BY within window
        order_by: list[tuple[Expression, SortDirection]] = []
        order_clause = window_expr.args.get("order")
        if order_clause:
            for ordered_expr in order_clause.expressions:
                if isinstance(ordered_expr, exp.Ordered):
                    expr = self._convert_expression(ordered_expr.this)
                    direction = (
                        SortDirection.DESC if ordered_expr.args.get("desc") else SortDirection.ASC
                    )
                    order_by.append((expr, direction))
                else:
                    order_by.append((self._convert_expression(ordered_expr), SortDirection.ASC))

        # Parse window frame specification
        frame = None
        spec = window_expr.args.get("spec")
        if spec:
            frame = self._convert_window_frame(spec)

        window_spec = WindowSpec(
            partition_by=partition_by,
            order_by=order_by,
            frame=frame,
        )

        return WindowExpression(function=function, window=window_spec)

    def _convert_window_frame(self, spec: exp.WindowSpec) -> WindowFrameSpec | None:
        """Convert a window frame specification."""
        # Determine frame type
        frame_type = WindowFrameType.RANGE  # Default
        kind = spec.args.get("kind")
        if kind:
            kind_str = str(kind).upper()
            if kind_str == "ROWS":
                frame_type = WindowFrameType.ROWS
            elif kind_str == "GROUPS":
                frame_type = WindowFrameType.GROUPS

        # Parse start boundary
        start_bound = WindowFrameBound.UNBOUNDED_PRECEDING
        start_offset = None
        start = spec.args.get("start")
        if start:
            start_bound, start_offset = self._parse_frame_bound(start)

        # Parse end boundary
        end_bound = WindowFrameBound.CURRENT_ROW
        end_offset = None
        end = spec.args.get("end")
        if end:
            end_bound, end_offset = self._parse_frame_bound(end)

        return WindowFrameSpec(
            frame_type=frame_type,
            start_bound=start_bound,
            start_offset=start_offset,
            end_bound=end_bound,
            end_offset=end_offset,
        )

    def _parse_frame_bound(self, bound: exp.Expression) -> tuple[WindowFrameBound, int | None]:
        """Parse a window frame boundary expression."""
        if isinstance(bound, str):
            bound_upper = bound.upper()
            if "UNBOUNDED" in bound_upper and "PRECEDING" in bound_upper:
                return WindowFrameBound.UNBOUNDED_PRECEDING, None
            if "UNBOUNDED" in bound_upper and "FOLLOWING" in bound_upper:
                return WindowFrameBound.UNBOUNDED_FOLLOWING, None
            if "CURRENT" in bound_upper:
                return WindowFrameBound.CURRENT_ROW, None

        # Handle expression-based bounds (e.g., "5 PRECEDING")
        if hasattr(bound, "this"):
            if isinstance(bound.this, exp.Literal) and bound.this.is_int:
                offset = int(bound.this.this)
                bound_type = getattr(bound, "side", "PRECEDING")
                if str(bound_type).upper() == "FOLLOWING":
                    return WindowFrameBound.FOLLOWING, offset
                return WindowFrameBound.PRECEDING, offset

        return WindowFrameBound.CURRENT_ROW, None

    def _convert_case_expression(self, case_expr: exp.Case) -> CaseExpression:
        """Convert a CASE expression to CaseExpression."""
        # Simple CASE: CASE expr WHEN val THEN result ...
        # Searched CASE: CASE WHEN condition THEN result ...
        operand = None
        if case_expr.this:
            operand = self._convert_expression(case_expr.this)

        when_clauses: list[tuple[Expression, Expression]] = []
        ifs = case_expr.args.get("ifs")
        if ifs:
            for if_clause in ifs:
                if isinstance(if_clause, exp.If):
                    condition = self._convert_expression(if_clause.this)
                    result = self._convert_expression(if_clause.args.get("true"))
                    when_clauses.append((condition, result))

        else_result = None
        default = case_expr.args.get("default")
        if default:
            else_result = self._convert_expression(default)

        return CaseExpression(
            operand=operand,
            when_clauses=when_clauses,
            else_result=else_result,
        )

    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison."""
        try:
            parsed = sqlglot.parse_one(sql, dialect=self._dialect)
            normalized = parsed.transform(sqlglot.optimizer.normalize.normalize)
            return normalized.sql(dialect=self._dialect, pretty=False)
        except (ParseError, Exception):
            return sql
