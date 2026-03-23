"""Correlated subquery decorrelation transforms.

This module provides transformations to convert correlated subqueries into
equivalent join-based forms, enabling query folding that would otherwise
be blocked by correlation.

Theoretical Foundation:
    Correlated subqueries reference columns from outer queries, creating
    implicit row-by-row evaluation semantics. Many correlated subqueries
    can be transformed to equivalent joins:

    1. EXISTS → SEMI-JOIN
       SELECT * FROM T WHERE EXISTS (SELECT 1 FROM S WHERE S.a = T.b)
       ≡ SELECT * FROM T SEMI-JOIN S ON T.b = S.a

    2. NOT EXISTS → ANTI-JOIN
       SELECT * FROM T WHERE NOT EXISTS (SELECT 1 FROM S WHERE S.a = T.b)
       ≡ SELECT * FROM T ANTI-JOIN S ON T.b = S.a

    3. IN (subquery) → SEMI-JOIN
       SELECT * FROM T WHERE T.a IN (SELECT S.x FROM S WHERE S.y = T.b)
       ≡ SELECT * FROM T SEMI-JOIN S ON T.a = S.x AND T.b = S.y

    4. Scalar subquery → LEFT JOIN with aggregate (partial support)
       SELECT T.*, (SELECT MAX(S.x) FROM S WHERE S.a = T.b) as max_x FROM T
       ≡ SELECT T.*, agg.max_x FROM T LEFT JOIN (SELECT a, MAX(x) as max_x FROM S GROUP BY a) agg ON T.b = agg.a

Limitations:
    - Correlated subqueries with aggregation in complex positions may not decorrelate
    - NOT IN with nullable columns has different NULL semantics
    - Scalar subqueries with multiple correlation columns need careful handling

Why This Matters for Folding:
    Without decorrelation, a query like:
        SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)

    Cannot be folded if the view covers 'users' because the subquery correlates to u.id.

    After decorrelation:
        SELECT * FROM users u SEMI-JOIN orders o ON u.id = o.user_id

    Now this is a standard join and can potentially be folded.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from alma_algebrakit.models.algebra import (
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    CompoundPredicate,
    ExistsExpression,
    InSubqueryExpression,
    Join,
    JoinType,
    LogicalOp,
    Predicate,
    Projection,
    RAExpression,
    Relation,
    Selection,
    SubqueryExpression,
)


@dataclass
class CorrelationInfo:
    """Information about a correlation in a subquery.

    Attributes:
        outer_table: Table/alias from outer query
        outer_column: Column from outer query
        inner_table: Table/alias from inner subquery
        inner_column: Column from inner subquery
        predicate: The original correlation predicate
    """

    outer_table: str
    outer_column: str
    inner_table: str
    inner_column: str
    predicate: Predicate


@dataclass
class DecorrelationResult:
    """Result of attempting to decorrelate a query.

    Attributes:
        success: Whether decorrelation was successful
        decorrelated_ra: The decorrelated RA expression (if success)
        reason: Explanation of what happened
        transforms_applied: List of transforms that were applied
        correlations_found: List of correlations that were found
        non_decorrelatable: Correlations that could not be transformed
    """

    success: bool
    decorrelated_ra: RAExpression | None = None
    reason: str | None = None
    transforms_applied: list[str] = field(default_factory=list)
    correlations_found: list[CorrelationInfo] = field(default_factory=list)
    non_decorrelatable: list[CorrelationInfo] = field(default_factory=list)


def decorrelate_query(query_ra: RAExpression) -> DecorrelationResult:
    """Attempt to decorrelate all correlated subqueries in a query.

    This is the main entry point for decorrelation. It traverses the
    RA expression tree and transforms correlated subqueries to joins
    where possible.

    Supported transforms:
    1. EXISTS → SEMI-JOIN
    2. NOT EXISTS → ANTI-JOIN
    3. IN (subquery) → SEMI-JOIN
    4. Scalar subquery → LEFT JOIN + aggregate (limited)

    Args:
        query_ra: The query RA expression to decorrelate

    Returns:
        DecorrelationResult with the transformed query or failure info
    """
    transforms: list[str] = []
    correlations_found: list[CorrelationInfo] = []
    non_decorrelatable: list[CorrelationInfo] = []

    # Track outer tables as we descend
    outer_tables: set[str] = set()

    try:
        decorrelated = _decorrelate_recursive(
            query_ra,
            outer_tables,
            transforms,
            correlations_found,
            non_decorrelatable,
        )

        if non_decorrelatable:
            return DecorrelationResult(
                success=False,
                decorrelated_ra=decorrelated,
                reason=f"Could not decorrelate {len(non_decorrelatable)} correlation(s)",
                transforms_applied=transforms,
                correlations_found=correlations_found,
                non_decorrelatable=non_decorrelatable,
            )

        return DecorrelationResult(
            success=True,
            decorrelated_ra=decorrelated,
            reason=f"Applied {len(transforms)} transform(s)"
            if transforms
            else "No correlations found",
            transforms_applied=transforms,
            correlations_found=correlations_found,
        )

    except Exception as e:
        return DecorrelationResult(
            success=False,
            reason=f"Decorrelation failed: {e}",
            transforms_applied=transforms,
            correlations_found=correlations_found,
            non_decorrelatable=non_decorrelatable,
        )


def _decorrelate_recursive(
    expr: RAExpression,
    outer_tables: set[str],
    transforms: list[str],
    correlations_found: list[CorrelationInfo],
    non_decorrelatable: list[CorrelationInfo],
) -> RAExpression:
    """Recursively decorrelate an RA expression."""

    if isinstance(expr, Selection):
        # Check for EXISTS/NOT EXISTS in the predicate
        new_input = _decorrelate_recursive(
            expr.input, outer_tables, transforms, correlations_found, non_decorrelatable
        )

        # Get tables from new_input for correlation detection
        input_tables = _get_table_aliases(new_input)
        all_outer = outer_tables | input_tables

        # Try to decorrelate the predicate
        new_predicate, additional_joins = _decorrelate_predicate(
            expr.predicate,
            all_outer,
            transforms,
            correlations_found,
            non_decorrelatable,
        )

        # If we got additional joins from decorrelation, apply them
        result: RAExpression = new_input
        for join_info in additional_joins:
            result = Join(
                left=result,
                right=join_info["right"],
                join_type=join_info["join_type"],
                condition=join_info["condition"],
            )

        # Apply remaining predicate if any
        if new_predicate:
            result = Selection(predicate=new_predicate, input=result)

        return result

    elif isinstance(expr, Projection):
        new_input = _decorrelate_recursive(
            expr.input, outer_tables, transforms, correlations_found, non_decorrelatable
        )

        # Check columns for scalar subqueries (limited support)
        new_columns = []
        input_tables = _get_table_aliases(new_input)
        all_outer = outer_tables | input_tables

        for col_expr, alias in expr.columns:
            if isinstance(col_expr, SubqueryExpression):
                # Scalar subqueries in projection are complex to decorrelate
                # For now, mark them as non-decorrelatable (full implementation pending)
                # _decorrelate_scalar_subquery currently returns None
                non_decorrelatable.append(
                    CorrelationInfo(
                        outer_table="<scalar>",
                        outer_column="<subquery>",
                        inner_table="<subquery>",
                        inner_column="<subquery>",
                        predicate=AtomicPredicate(
                            left=ColumnRef(column="x"),
                            op=ComparisonOp.EQ,
                            right=ColumnRef(column="y"),
                        ),
                    )
                )
            new_columns.append((col_expr, alias))

        return Projection(
            columns=new_columns,
            input=new_input,
            distinct=expr.distinct,
            distinct_on=expr.distinct_on,
        )

    elif isinstance(expr, Join):
        # Recursively decorrelate both sides
        new_left = _decorrelate_recursive(
            expr.left, outer_tables, transforms, correlations_found, non_decorrelatable
        )

        # Left side tables are now outer for right side
        left_tables = _get_table_aliases(new_left)
        new_outer = outer_tables | left_tables

        new_right = _decorrelate_recursive(
            expr.right, new_outer, transforms, correlations_found, non_decorrelatable
        )

        return Join(
            left=new_left,
            right=new_right,
            join_type=expr.join_type,
            condition=expr.condition,
        )

    elif isinstance(expr, Relation):
        # Base case - no decorrelation needed
        return expr

    # For other expression types, return as-is for now
    return expr


def _decorrelate_predicate(
    pred: Predicate,
    outer_tables: set[str],
    transforms: list[str],
    correlations_found: list[CorrelationInfo],
    non_decorrelatable: list[CorrelationInfo],
) -> tuple[Predicate | None, list[dict]]:
    """Decorrelate a predicate, extracting EXISTS/IN subqueries to joins.

    Returns:
        (remaining_predicate, list_of_joins) where joins are dicts with
        {right, join_type, condition}
    """
    additional_joins: list[dict] = []

    if isinstance(pred, CompoundPredicate):
        if pred.op == LogicalOp.AND:
            # Process each conjunct
            remaining_operands = []
            for operand in pred.operands:
                if isinstance(operand, ExistsExpression):
                    # Decorrelate EXISTS
                    join_info = _decorrelate_exists(
                        operand, outer_tables, transforms, correlations_found, non_decorrelatable
                    )
                    if join_info:
                        additional_joins.append(join_info)
                    else:
                        remaining_operands.append(operand)
                elif isinstance(operand, InSubqueryExpression):
                    # Decorrelate IN
                    join_info = _decorrelate_in_subquery(
                        operand, outer_tables, transforms, correlations_found, non_decorrelatable
                    )
                    if join_info:
                        additional_joins.append(join_info)
                    else:
                        remaining_operands.append(operand)
                else:
                    # Recursively process
                    sub_pred, sub_joins = _decorrelate_predicate(
                        operand, outer_tables, transforms, correlations_found, non_decorrelatable
                    )
                    additional_joins.extend(sub_joins)
                    if sub_pred:
                        remaining_operands.append(sub_pred)

            # Build remaining predicate
            if not remaining_operands:
                return None, additional_joins
            elif len(remaining_operands) == 1:
                return remaining_operands[0], additional_joins
            else:
                return CompoundPredicate(
                    op=LogicalOp.AND, operands=remaining_operands
                ), additional_joins

        elif pred.op == LogicalOp.OR:
            # OR is tricky - can't easily extract to joins
            # For now, return as-is
            return pred, additional_joins

        elif pred.op == LogicalOp.NOT:
            # NOT EXISTS is handled specially
            inner = pred.operands[0] if pred.operands else None
            if isinstance(inner, ExistsExpression):
                # NOT EXISTS -> ANTI-JOIN
                inner_negated = ExistsExpression(query=inner.query, negated=True)
                join_info = _decorrelate_exists(
                    inner_negated, outer_tables, transforms, correlations_found, non_decorrelatable
                )
                if join_info:
                    additional_joins.append(join_info)
                    return None, additional_joins
            return pred, additional_joins

    elif isinstance(pred, ExistsExpression):
        join_info = _decorrelate_exists(
            pred, outer_tables, transforms, correlations_found, non_decorrelatable
        )
        if join_info:
            additional_joins.append(join_info)
            return None, additional_joins
        return pred, additional_joins

    elif isinstance(pred, InSubqueryExpression):
        join_info = _decorrelate_in_subquery(
            pred, outer_tables, transforms, correlations_found, non_decorrelatable
        )
        if join_info:
            additional_joins.append(join_info)
            return None, additional_joins
        return pred, additional_joins

    # For other predicates, return as-is
    return pred, additional_joins


def _decorrelate_exists(
    exists_expr: ExistsExpression,
    outer_tables: set[str],
    transforms: list[str],
    correlations_found: list[CorrelationInfo],
    _non_decorrelatable: list[CorrelationInfo],  # Unused, kept for API consistency
) -> dict | None:
    """Transform EXISTS subquery to SEMI-JOIN (or ANTI-JOIN for NOT EXISTS).

    EXISTS (SELECT ... FROM S WHERE S.a = T.b AND ...)
    → T SEMI-JOIN S ON T.b = S.a

    Returns:
        Join info dict {right, join_type, condition} or None if cannot decorrelate
    """
    subquery = exists_expr.query
    join_type = JoinType.ANTI if exists_expr.negated else JoinType.SEMI

    # Find correlations in the subquery
    correlations, inner_predicates, inner_ra = _extract_correlations(subquery, outer_tables)

    if not correlations:
        # No correlation - this is an uncorrelated EXISTS, can't transform to join
        return None

    # Record correlations
    correlations_found.extend(correlations)

    # Build the join condition from correlations
    join_condition = _build_join_condition(correlations)

    # Record the transform
    transform_name = "NOT EXISTS → ANTI-JOIN" if exists_expr.negated else "EXISTS → SEMI-JOIN"
    transforms.append(transform_name)

    # The right side is the inner query (possibly with remaining predicates)
    right_side = inner_ra
    if inner_predicates:
        right_side = Selection(predicate=inner_predicates, input=inner_ra)

    return {
        "right": right_side,
        "join_type": join_type,
        "condition": join_condition,
    }


def _decorrelate_in_subquery(
    in_expr: InSubqueryExpression,
    outer_tables: set[str],
    transforms: list[str],
    correlations_found: list[CorrelationInfo],
    non_decorrelatable: list[CorrelationInfo],
) -> dict | None:
    """Transform IN (subquery) to SEMI-JOIN.

    T.x IN (SELECT S.y FROM S WHERE S.a = T.b)
    → T SEMI-JOIN S ON T.x = S.y AND T.b = S.a

    Returns:
        Join info dict or None if cannot decorrelate
    """
    # For NOT IN, we'd need ANTI-JOIN, but NOT IN has tricky NULL semantics
    if in_expr.negated:
        # NOT IN is complex due to NULLs - mark as non-decorrelatable
        left_ref = in_expr.left if isinstance(in_expr.left, ColumnRef) else ColumnRef(column="?")
        non_decorrelatable.append(
            CorrelationInfo(
                outer_table=left_ref.table or "",
                outer_column=left_ref.column,
                inner_table="<not_in_subquery>",
                inner_column="<rejected_null_semantics>",
                predicate=AtomicPredicate(
                    left=left_ref,
                    op=ComparisonOp.EQ,
                    right=ColumnRef(column="y"),
                ),
            )
        )
        return None

    subquery = in_expr.query
    left_expr = in_expr.left

    # Find correlations and the projected column from subquery
    correlations, inner_predicates, inner_ra = _extract_correlations(subquery, outer_tables)

    # The subquery must project exactly one column for IN
    projected_col = _get_single_projected_column(subquery)
    if projected_col is None:
        return None

    # Build correlation from the IN itself: T.x = S.projected_col
    if isinstance(left_expr, ColumnRef):
        in_correlation = CorrelationInfo(
            outer_table=left_expr.table or "",
            outer_column=left_expr.column,
            inner_table=projected_col[0] or "",
            inner_column=projected_col[1],
            predicate=AtomicPredicate(
                left=left_expr,
                op=ComparisonOp.EQ,
                right=ColumnRef(table=projected_col[0], column=projected_col[1]),
            ),
        )
        correlations.append(in_correlation)

    if not correlations:
        return None

    correlations_found.extend(correlations)

    # Build join condition
    join_condition = _build_join_condition(correlations)

    transforms.append("IN (subquery) → SEMI-JOIN")

    # Right side
    right_side = inner_ra
    if inner_predicates:
        right_side = Selection(predicate=inner_predicates, input=inner_ra)

    return {
        "right": right_side,
        "join_type": JoinType.SEMI,
        "condition": join_condition,
    }


def _decorrelate_scalar_subquery(
    _scalar_expr: SubqueryExpression,
    _outer_tables: set[str],
    _transforms: list[str],
    _correlations_found: list[CorrelationInfo],
    _non_decorrelatable: list[CorrelationInfo],
) -> dict | None:
    """Transform scalar subquery to LEFT JOIN with aggregate.

    This is complex and only partially supported.

    (SELECT MAX(S.x) FROM S WHERE S.a = T.b)
    → LEFT JOIN (SELECT a, MAX(x) as _scalar FROM S GROUP BY a) ON T.b = a

    Returns:
        Join info dict or None if cannot decorrelate

    Note: Arguments prefixed with _ are currently unused as this function
    returns None (not implemented). They are kept for API consistency with
    other decorrelation functions.
    """
    # For now, mark as non-decorrelatable
    # Full implementation would need to:
    # 1. Extract correlation predicates
    # 2. Identify aggregate functions
    # 3. Generate GROUP BY on correlation columns
    # 4. Create LEFT JOIN with aggregate subquery
    return None


def _extract_correlations(
    subquery: RAExpression,
    outer_tables: set[str],
) -> tuple[list[CorrelationInfo], Predicate | None, RAExpression]:
    """Extract correlation predicates from a subquery.

    Scans the subquery for predicates that reference outer tables.

    Returns:
        (correlations, remaining_predicates, inner_ra)
    """
    correlations: list[CorrelationInfo] = []
    remaining_preds: list[Predicate] = []
    inner_ra = subquery

    # Unwrap selections and projections to find predicates
    while True:
        if isinstance(inner_ra, Selection):
            pred = inner_ra.predicate
            corr, remaining = _split_correlation_predicate(pred, outer_tables)
            correlations.extend(corr)
            if remaining:
                remaining_preds.append(remaining)
            inner_ra = inner_ra.input
        elif isinstance(inner_ra, Projection):
            inner_ra = inner_ra.input
        else:
            break

    # Combine remaining predicates
    combined_remaining: Predicate | None = None
    if remaining_preds:
        if len(remaining_preds) == 1:
            combined_remaining = remaining_preds[0]
        else:
            combined_remaining = CompoundPredicate(op=LogicalOp.AND, operands=remaining_preds)

    return correlations, combined_remaining, inner_ra


def _split_correlation_predicate(
    pred: Predicate,
    outer_tables: set[str],
) -> tuple[list[CorrelationInfo], Predicate | None]:
    """Split a predicate into correlation and non-correlation parts.

    A correlation predicate references both outer and inner tables.
    """
    correlations: list[CorrelationInfo] = []
    non_corr_preds: list[Predicate] = []

    if isinstance(pred, CompoundPredicate) and pred.op == LogicalOp.AND:
        for operand in pred.operands:
            corr, remaining = _split_correlation_predicate(operand, outer_tables)
            correlations.extend(corr)
            if remaining:
                non_corr_preds.append(remaining)
    elif isinstance(pred, AtomicPredicate):
        # Check if this predicate correlates outer to inner
        corr_info = _check_atomic_correlation(pred, outer_tables)
        if corr_info:
            correlations.append(corr_info)
        else:
            non_corr_preds.append(pred)
    else:
        # Complex predicate - keep as non-correlation
        non_corr_preds.append(pred)

    # Combine non-correlation predicates
    remaining: Predicate | None = None
    if non_corr_preds:
        if len(non_corr_preds) == 1:
            remaining = non_corr_preds[0]
        else:
            remaining = CompoundPredicate(op=LogicalOp.AND, operands=non_corr_preds)

    return correlations, remaining


def _check_atomic_correlation(
    pred: AtomicPredicate,
    outer_tables: set[str],
) -> CorrelationInfo | None:
    """Check if an atomic predicate is a correlation predicate.

    A correlation predicate has form: outer_table.col = inner_table.col
    """
    if pred.op != ComparisonOp.EQ:
        return None

    left = pred.left
    right = pred.right

    if not isinstance(left, ColumnRef) or not isinstance(right, ColumnRef):
        return None

    left_is_outer = (left.table or "").lower() in {t.lower() for t in outer_tables}
    right_is_outer = (right.table or "").lower() in {t.lower() for t in outer_tables}

    if left_is_outer and not right_is_outer:
        return CorrelationInfo(
            outer_table=left.table or "",
            outer_column=left.column,
            inner_table=right.table or "",
            inner_column=right.column,
            predicate=pred,
        )
    elif right_is_outer and not left_is_outer:
        return CorrelationInfo(
            outer_table=right.table or "",
            outer_column=right.column,
            inner_table=left.table or "",
            inner_column=left.column,
            predicate=pred,
        )

    return None


def _build_join_condition(correlations: list[CorrelationInfo]) -> Predicate:
    """Build a join condition from correlation info."""
    if len(correlations) == 1:
        return correlations[0].predicate

    predicates = [c.predicate for c in correlations]
    return CompoundPredicate(op=LogicalOp.AND, operands=predicates)


def _get_table_aliases(expr: RAExpression) -> set[str]:
    """Get all table aliases from an RA expression."""
    aliases: set[str] = set()

    if isinstance(expr, Relation):
        aliases.add(expr.alias or expr.name)
    elif isinstance(expr, Selection):
        aliases.update(_get_table_aliases(expr.input))
    elif isinstance(expr, Projection):
        aliases.update(_get_table_aliases(expr.input))
    elif isinstance(expr, Join):
        aliases.update(_get_table_aliases(expr.left))
        aliases.update(_get_table_aliases(expr.right))

    return aliases


def _get_single_projected_column(
    subquery: RAExpression,
) -> tuple[str | None, str] | None:
    """Get the single projected column from a subquery (for IN).

    Returns (table, column) or None if not a single column projection.
    """
    if isinstance(subquery, Projection):
        if len(subquery.columns) == 1:
            col_expr, _ = subquery.columns[0]
            if isinstance(col_expr, ColumnRef):
                return (col_expr.table, col_expr.column)

    return None
