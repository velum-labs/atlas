"""Visitor pattern for relational algebra expressions.

This module provides the visitor pattern infrastructure to traverse and
transform RA expressions without using isinstance() checks throughout
the codebase.

Usage:
    class MyVisitor(RAExpressionVisitor[str]):
        def visit_relation(self, expr: Relation) -> str:
            return expr.name

        def visit_selection(self, expr: Selection) -> str:
            return f"σ({expr.input.accept(self)})"
        # ... implement other visit methods

    visitor = MyVisitor()
    result = some_expr.accept(visitor)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from alma_algebrakit.models.algebra import (
        Aggregation,
        Difference,
        Intersect,
        Join,
        Limit,
        Projection,
        RAExpression,
        Relation,
        Selection,
        Sort,
        Union,
        WithExpression,
    )

T = TypeVar("T")


class RAExpressionVisitor[T](ABC):
    """Abstract visitor for relational algebra expressions.

    Implement the visit_* methods for each RA expression type you need
    to handle. The accept() method on RAExpression will dispatch to the
    appropriate visitor method.

    Type parameter T is the return type of all visit methods.
    """

    @abstractmethod
    def visit_relation(self, expr: Relation) -> T:
        """Visit a base relation (table scan)."""
        ...

    @abstractmethod
    def visit_selection(self, expr: Selection) -> T:
        """Visit a selection operation σ_φ(E)."""
        ...

    @abstractmethod
    def visit_projection(self, expr: Projection) -> T:
        """Visit a projection operation π_A(E)."""
        ...

    @abstractmethod
    def visit_join(self, expr: Join) -> T:
        """Visit a join operation E_1 ⋈_θ E_2."""
        ...

    @abstractmethod
    def visit_aggregation(self, expr: Aggregation) -> T:
        """Visit a grouping/aggregation operation γ_{G; aggs}(E)."""
        ...

    @abstractmethod
    def visit_union(self, expr: Union) -> T:
        """Visit a union operation E_1 ∪ E_2."""
        ...

    @abstractmethod
    def visit_difference(self, expr: Difference) -> T:
        """Visit a difference operation E_1 - E_2."""
        ...

    @abstractmethod
    def visit_intersect(self, expr: Intersect) -> T:
        """Visit an intersection operation E_1 ∩ E_2."""
        ...

    @abstractmethod
    def visit_sort(self, expr: Sort) -> T:
        """Visit a sort operation (ORDER BY)."""
        ...

    @abstractmethod
    def visit_limit(self, expr: Limit) -> T:
        """Visit a limit operation (LIMIT/OFFSET)."""
        ...

    @abstractmethod
    def visit_with_expression(self, expr: WithExpression) -> T:
        """Visit a WITH clause (CTE container)."""
        ...


class TransformationVisitor(RAExpressionVisitor["RAExpression"]):
    """Visitor that transforms RA expressions into new RA expressions.

    Override specific visit methods to transform certain expression types.
    Default implementations return a copy of the expression with transformed
    children.
    """

    def visit_relation(self, expr: Relation) -> RAExpression:
        """Default: return the relation unchanged."""
        return expr

    def visit_selection(self, expr: Selection) -> RAExpression:
        """Default: transform the input, keep the predicate."""
        from alma_algebrakit.models.algebra import Selection

        transformed_input = expr.input.accept(self)
        if transformed_input is expr.input:
            return expr
        return Selection(predicate=expr.predicate, input=transformed_input)

    def visit_projection(self, expr: Projection) -> RAExpression:
        """Default: transform the input, keep the columns."""
        from alma_algebrakit.models.algebra import Projection

        transformed_input = expr.input.accept(self)
        if transformed_input is expr.input:
            return expr
        return Projection(
            columns=expr.columns,
            input=transformed_input,
            distinct=expr.distinct,
            distinct_on=expr.distinct_on,
        )

    def visit_join(self, expr: Join) -> RAExpression:
        """Default: transform both sides, keep the condition."""
        from alma_algebrakit.models.algebra import Join

        transformed_left = expr.left.accept(self)
        transformed_right = expr.right.accept(self)
        if transformed_left is expr.left and transformed_right is expr.right:
            return expr
        return Join(
            left=transformed_left,
            right=transformed_right,
            join_type=expr.join_type,
            condition=expr.condition,
            is_lateral=expr.is_lateral,
        )

    def visit_aggregation(self, expr: Aggregation) -> RAExpression:
        """Default: transform the input, keep grouping and aggregates."""
        from alma_algebrakit.models.algebra import Aggregation

        transformed_input = expr.input.accept(self)
        if transformed_input is expr.input:
            return expr
        return Aggregation(
            group_by=expr.group_by,
            aggregates=expr.aggregates,
            input=transformed_input,
            having=expr.having,
        )

    def visit_union(self, expr: Union) -> RAExpression:
        """Default: transform both sides."""
        from alma_algebrakit.models.algebra import Union

        transformed_left = expr.left.accept(self)
        transformed_right = expr.right.accept(self)
        if transformed_left is expr.left and transformed_right is expr.right:
            return expr
        return Union(left=transformed_left, right=transformed_right)

    def visit_difference(self, expr: Difference) -> RAExpression:
        """Default: transform both sides."""
        from alma_algebrakit.models.algebra import Difference

        transformed_left = expr.left.accept(self)
        transformed_right = expr.right.accept(self)
        if transformed_left is expr.left and transformed_right is expr.right:
            return expr
        return Difference(left=transformed_left, right=transformed_right)

    def visit_intersect(self, expr: Intersect) -> RAExpression:
        """Default: transform both sides."""
        from alma_algebrakit.models.algebra import Intersect

        transformed_left = expr.left.accept(self)
        transformed_right = expr.right.accept(self)
        if transformed_left is expr.left and transformed_right is expr.right:
            return expr
        return Intersect(left=transformed_left, right=transformed_right, all=expr.all)

    def visit_sort(self, expr: Sort) -> RAExpression:
        """Default: transform the input, keep the sort spec."""
        from alma_algebrakit.models.algebra import Sort

        transformed_input = expr.input.accept(self)
        if transformed_input is expr.input:
            return expr
        return Sort(order_by=expr.order_by, input=transformed_input)

    def visit_limit(self, expr: Limit) -> RAExpression:
        """Default: transform the input, keep limit/offset."""
        from alma_algebrakit.models.algebra import Limit

        transformed_input = expr.input.accept(self)
        if transformed_input is expr.input:
            return expr
        return Limit(limit=expr.limit, offset=expr.offset, input=transformed_input)

    def visit_with_expression(self, expr: WithExpression) -> RAExpression:
        """Default: transform all CTEs and the main query."""
        from alma_algebrakit.models.algebra import CTEDefinition, WithExpression

        transformed_ctes = []
        changed = False
        for cte in expr.ctes:
            transformed_query = cte.query.accept(self)
            if transformed_query is not cte.query:
                changed = True
                transformed_ctes.append(
                    CTEDefinition(
                        name=cte.name,
                        columns=cte.columns,
                        query=transformed_query,
                        recursive=cte.recursive,
                    )
                )
            else:
                transformed_ctes.append(cte)

        transformed_main_query = expr.main_query.accept(self)
        if transformed_main_query is not expr.main_query:
            changed = True

        if not changed:
            return expr
        return WithExpression(ctes=transformed_ctes, main_query=transformed_main_query)


class CollectionVisitor(RAExpressionVisitor[None]):
    """Visitor that collects information from RA expressions.

    Override specific visit methods to collect information.
    Default implementations just visit children recursively.
    """

    def visit_relation(self, expr: Relation) -> None:
        """Default: no children to visit."""
        pass

    def visit_selection(self, expr: Selection) -> None:
        """Default: visit input."""
        expr.input.accept(self)

    def visit_projection(self, expr: Projection) -> None:
        """Default: visit input."""
        expr.input.accept(self)

    def visit_join(self, expr: Join) -> None:
        """Default: visit both sides."""
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_aggregation(self, expr: Aggregation) -> None:
        """Default: visit input."""
        expr.input.accept(self)

    def visit_union(self, expr: Union) -> None:
        """Default: visit both sides."""
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_difference(self, expr: Difference) -> None:
        """Default: visit both sides."""
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_intersect(self, expr: Intersect) -> None:
        """Default: visit both sides."""
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_sort(self, expr: Sort) -> None:
        """Default: visit input."""
        expr.input.accept(self)

    def visit_limit(self, expr: Limit) -> None:
        """Default: visit input."""
        expr.input.accept(self)

    def visit_with_expression(self, expr: WithExpression) -> None:
        """Default: visit all CTEs and main query."""
        for cte in expr.ctes:
            cte.query.accept(self)
        expr.main_query.accept(self)


def dispatch_accept[T](expr: RAExpression, visitor: RAExpressionVisitor[T]) -> T:
    """Dispatch to the correct visitor method based on expression type.

    This function contains the single isinstance() chain for the entire
    visitor pattern, avoiding scattered isinstance() checks.

    Args:
        expr: The RA expression to visit
        visitor: The visitor to use

    Returns:
        Result of the visitor method

    Raises:
        TypeError: If the expression type is not recognized
    """
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

    if isinstance(expr, Relation):
        return visitor.visit_relation(expr)
    if isinstance(expr, Selection):
        return visitor.visit_selection(expr)
    if isinstance(expr, Projection):
        return visitor.visit_projection(expr)
    if isinstance(expr, Join):
        return visitor.visit_join(expr)
    if isinstance(expr, Aggregation):
        return visitor.visit_aggregation(expr)
    if isinstance(expr, Union):
        return visitor.visit_union(expr)
    if isinstance(expr, Difference):
        return visitor.visit_difference(expr)
    if isinstance(expr, Intersect):
        return visitor.visit_intersect(expr)
    if isinstance(expr, Sort):
        return visitor.visit_sort(expr)
    if isinstance(expr, Limit):
        return visitor.visit_limit(expr)
    if isinstance(expr, WithExpression):
        return visitor.visit_with_expression(expr)

    # Fallback: check the type field for extension types (e.g., WithClause from sqlkit)
    # This allows packages that extend algebrakit to add new RAExpression types
    # while still participating in the visitor pattern
    expr_type = getattr(expr, "type", None)
    if expr_type == "with_clause":
        # WithClause is functionally similar to WithExpression
        return visitor.visit_with_expression(expr)

    raise TypeError(f"Unknown RAExpression type: {type(expr).__name__}")
