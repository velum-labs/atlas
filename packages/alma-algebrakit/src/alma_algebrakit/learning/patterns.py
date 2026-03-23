"""Pattern extraction from relational algebra expressions.

Extracts "pattern signature" from RA expressions:
- base relations used (scans)
- join graph (relations + join predicates)
- selection predicates (conjunctive form)
- projected attributes
- group-by keys and aggregate functions
- computed expressions (fingerprints)

This module provides SQL-agnostic pattern extraction operating purely on
algebrakit's RA primitives.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from alma_algebrakit.models.algebra import (
    Aggregation,
    ColumnRef,
    Difference,
    Join,
    JoinType,
    Predicate,
    Projection,
    Relation,
    Selection,
    Union,
)
from alma_algebrakit.visitor import RAExpressionVisitor

if TYPE_CHECKING:
    from alma_algebrakit.models.algebra import (
        Intersect,
        Limit,
        RAExpression,
        Sort,
        WithExpression,
    )


class JoinEdgePattern(BaseModel):
    """A join edge pattern in the RA expression's join graph."""

    left_table: str = Field(description="Left table name/alias")
    right_table: str = Field(description="Right table name/alias")
    left_column: str = Field(description="Left join column")
    right_column: str = Field(description="Right join column")
    join_type: str = Field(default="INNER", description="Type of join")
    predicate_fingerprint: str = Field(description="Canonical predicate representation")

    def fingerprint(self) -> str:
        """Return a canonical fingerprint for this join edge."""
        # Normalize order for comparison
        if (self.left_table, self.left_column) > (self.right_table, self.right_column):
            return f"{self.right_table}.{self.right_column}={self.left_table}.{self.left_column}"
        return f"{self.left_table}.{self.left_column}={self.right_table}.{self.right_column}"

    def tables(self) -> tuple[str, str]:
        """Return the tables involved in this join."""
        return (self.left_table, self.right_table)


class PatternSignature(BaseModel):
    """Pattern signature for an RA expression.

    This is a SQL-agnostic representation of the "shape" of a query,
    capturing its structural features for similarity computation and
    pattern learning.

    Extracted features:
    - base relations used (scans)
    - join graph (relations + join predicates)
    - selection predicates (conjunctive form)
    - projected attributes
    - group-by keys and aggregate functions
    - computed expressions (fingerprints)
    """

    relations: list[str] = Field(
        default_factory=list, description="Base relations (table names or aliases)"
    )
    table_aliases: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from alias/effective name to actual table name",
    )
    join_edges: list[JoinEdgePattern] = Field(default_factory=list, description="Join graph edges")
    predicates: list[str] = Field(
        default_factory=list, description="Selection predicate fingerprints"
    )
    projected_columns: list[str] = Field(
        default_factory=list, description="Projected column references"
    )
    group_by_columns: list[str] = Field(
        default_factory=list, description="GROUP BY column references"
    )
    aggregates: list[str] = Field(
        default_factory=list, description="Aggregate function fingerprints"
    )
    computed_expressions: list[str] = Field(
        default_factory=list, description="Computed expression fingerprints"
    )
    computed_column_defs: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of computed column aliases to their expression definitions",
    )
    has_distinct: bool = Field(default=False, description="Whether query uses DISTINCT")
    has_outer_join: bool = Field(default=False, description="Whether query uses outer joins")
    has_subquery: bool = Field(default=False, description="Whether query has subqueries")
    has_union: bool = Field(default=False, description="Whether query uses UNION")
    has_window: bool = Field(default=False, description="Whether query uses window functions")

    def feature_set(self) -> set[str]:
        """Return all features as a set for similarity computation."""
        features: set[str] = set()
        # Relations
        for r in self.relations:
            features.add(f"rel:{r}")
        # Joins
        for j in self.join_edges:
            features.add(f"join:{j.fingerprint()}")
        # Predicates
        for p in self.predicates:
            features.add(f"pred:{p}")
        # Projected columns
        for c in self.projected_columns:
            features.add(f"proj:{c}")
        # Group by
        for g in self.group_by_columns:
            features.add(f"group:{g}")
        # Aggregates
        for a in self.aggregates:
            features.add(f"agg:{a}")
        return features

    def weighted_features(self, weights: dict[str, float]) -> dict[str, float]:
        """Return features with weights for similarity computation."""
        result: dict[str, float] = {}
        rel_weight = weights.get("relation", 1.0)
        join_weight = weights.get("join", 2.0)
        pred_weight = weights.get("predicate", 1.5)
        col_weight = weights.get("column", 1.0)
        group_weight = weights.get("groupby", 2.0)

        for r in self.relations:
            result[f"rel:{r}"] = rel_weight
        for j in self.join_edges:
            result[f"join:{j.fingerprint()}"] = join_weight
        for p in self.predicates:
            result[f"pred:{p}"] = pred_weight
        for c in self.projected_columns:
            result[f"proj:{c}"] = col_weight
        for g in self.group_by_columns:
            result[f"group:{g}"] = group_weight
        for a in self.aggregates:
            result[f"agg:{a}"] = group_weight

        return result


class PatternExtractionVisitor(RAExpressionVisitor[None]):
    """Visitor that extracts pattern signature components from RA expressions.

    This visitor traverses the RA expression tree and accumulates:
    - Relations used
    - Table aliases
    - Join edges
    - Predicates
    - Projected columns
    - Group-by columns
    - Aggregates
    - Computed expressions
    - Computed column definitions (alias -> expression)
    """

    def __init__(self, extractor: PatternExtractor) -> None:
        self._extractor = extractor
        self.relations: list[str] = []
        self.table_aliases: dict[str, str] = {}
        self.join_edges: list[JoinEdgePattern] = []
        self.predicates: list[str] = []
        self.projected_columns: list[str] = []
        self.group_by_columns: list[str] = []
        self.aggregates: list[str] = []
        self.computed_expressions: list[str] = []
        self.computed_column_defs: dict[str, str] = {}

    def visit_relation(self, expr: Relation) -> None:
        effective_name = expr.alias or expr.name
        self.relations.append(effective_name)
        # Store mapping from effective name (alias) to fully qualified table name
        if expr.schema_name:
            self.table_aliases[effective_name] = f"{expr.schema_name}.{expr.name}"
        else:
            self.table_aliases[effective_name] = expr.name

    def visit_selection(self, expr: Selection) -> None:
        # Extract predicates
        pred_fingerprints = self._extractor._extract_predicate_fingerprints(expr.predicate)
        self.predicates.extend(pred_fingerprints)
        # Recurse
        expr.input.accept(self)

    def visit_projection(self, expr: Projection) -> None:
        # Extract projected columns
        for col_expr, alias in expr.columns:
            if isinstance(col_expr, ColumnRef):
                self.projected_columns.append(col_expr.qualified_name())
            else:
                # This is a computed expression (e.g., concatenation, arithmetic)
                expr_fingerprint = col_expr.fingerprint()
                self.computed_expressions.append(expr_fingerprint)
                if alias:
                    # Track the alias and its expression definition
                    self.projected_columns.append(alias)
                    # Store the mapping from alias to the expression SQL
                    self.computed_column_defs[alias] = expr_fingerprint
        # Recurse
        expr.input.accept(self)

    def visit_join(self, expr: Join) -> None:
        # Extract join edge
        if expr.condition:
            edge = self._extractor._extract_join_edge(expr)
            if edge:
                self.join_edges.append(edge)
        # Recurse both sides
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_aggregation(self, expr: Aggregation) -> None:
        # Extract group-by columns
        for g in expr.group_by:
            if isinstance(g, ColumnRef):
                col_name = g.qualified_name()
                self.group_by_columns.append(col_name)
                # GROUP BY columns are also projected (they appear in SELECT output)
                self.projected_columns.append(col_name)
            else:
                fingerprint = g.fingerprint()
                self.group_by_columns.append(fingerprint)
                self.projected_columns.append(fingerprint)
        # Extract aggregates - these are also output columns
        for agg in expr.aggregates:
            self.aggregates.append(agg.fingerprint())
            # If the aggregate has an alias, that's the output column name
            if agg.alias:
                self.projected_columns.append(agg.alias)
                # Track as computed column (aggregate expression)
                self.computed_column_defs[agg.alias] = agg.fingerprint()
        # Extract HAVING predicates
        if expr.having:
            pred_fingerprints = self._extractor._extract_predicate_fingerprints(expr.having)
            self.predicates.extend(pred_fingerprints)
        # Recurse
        expr.input.accept(self)

    def visit_union(self, expr: Union) -> None:
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_difference(self, expr: Difference) -> None:
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_intersect(self, expr: Intersect) -> None:
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_sort(self, expr: Sort) -> None:
        # Sort doesn't add signature components, just recurse
        expr.input.accept(self)

    def visit_limit(self, expr: Limit) -> None:
        # Limit doesn't add signature components, just recurse
        expr.input.accept(self)

    def visit_with_expression(self, expr: WithExpression) -> None:
        # Visit CTE definitions and main query
        # Collect CTE names to filter out later (they're not base tables)
        cte_names = {cte_def.name for cte_def in expr.ctes}

        # Save current state before visiting CTEs
        # We only want to extract RELATIONS from CTEs, not their internal structure
        pre_cte_group_by = list(self.group_by_columns)
        pre_cte_predicates = list(self.predicates)
        pre_cte_aggregates = list(self.aggregates)
        pre_cte_projected = list(self.projected_columns)
        pre_cte_computed = list(self.computed_expressions)
        pre_cte_computed_defs = dict(self.computed_column_defs)

        # Visit CTE definitions (to get their base tables only)
        for cte_def in expr.ctes:
            cte_def.query.accept(self)

        # Restore non-relation state - CTE internals should not affect the signature
        self.group_by_columns = pre_cte_group_by
        self.predicates = pre_cte_predicates
        self.aggregates = pre_cte_aggregates
        self.projected_columns = pre_cte_projected
        self.computed_expressions = pre_cte_computed
        self.computed_column_defs = pre_cte_computed_defs

        # WithExpression uses "main_query"
        if expr.main_query:
            expr.main_query.accept(self)

        # Filter out CTE references from relations (they're not base tables)
        cte_refs = set()
        for rel in self.relations:
            if rel in cte_names:
                cte_refs.add(rel)
            elif self.table_aliases.get(rel) in cte_names:
                cte_refs.add(rel)

        # Filter out CTE references from relations
        self.relations = [r for r in self.relations if r not in cte_refs]

        # Filter out join edges that reference CTEs
        self.join_edges = [
            edge
            for edge in self.join_edges
            if edge.left_table not in cte_refs and edge.right_table not in cte_refs
        ]

        # Also remove CTE entries from table_aliases
        for cte_ref in cte_refs:
            self.table_aliases.pop(cte_ref, None)


class PatternExtractor:
    """Extracts pattern signatures from relational algebra expressions.

    This is the SQL-agnostic core of pattern extraction, operating purely
    on algebrakit primitives.
    """

    def extract(self, expr: RAExpression) -> PatternSignature:
        """Extract a pattern signature from an RA expression.

        Args:
            expr: The RA expression

        Returns:
            PatternSignature with extracted features
        """
        # Use visitor pattern to traverse and extract components
        visitor = PatternExtractionVisitor(self)
        expr.accept(visitor)

        # Check for outer joins
        has_outer_join = self._has_outer_join(expr)

        # Check for DISTINCT
        has_distinct = self._has_distinct(expr)

        # Check for UNION
        has_union = self._has_union(expr)

        return PatternSignature(
            relations=sorted(set(visitor.relations)),
            table_aliases=visitor.table_aliases,
            join_edges=visitor.join_edges,
            predicates=sorted(set(visitor.predicates)),
            projected_columns=sorted(set(visitor.projected_columns)),
            group_by_columns=sorted(set(visitor.group_by_columns)),
            aggregates=sorted(set(visitor.aggregates)),
            computed_expressions=sorted(set(visitor.computed_expressions)),
            computed_column_defs=visitor.computed_column_defs,
            has_distinct=has_distinct,
            has_outer_join=has_outer_join,
            has_subquery=False,  # TODO: detect subqueries
            has_union=has_union,
        )

    def _extract_predicate_fingerprints(self, pred: Predicate) -> list[str]:
        """Extract fingerprints from a predicate."""
        atoms = pred.to_cnf_atoms()
        return [a.fingerprint() for a in atoms]

    def _extract_join_edge(self, join: Join) -> JoinEdgePattern | None:
        """Extract a JoinEdgePattern from a Join expression."""
        if not join.condition:
            return None

        # Get atomic predicates from the join condition
        atoms = join.condition.to_cnf_atoms()
        if not atoms:
            return None

        # Find the first equality predicate between two tables
        for atom in atoms:
            if atom.op.value == "=":
                left_cols = atom.left.referenced_columns()
                right_cols = atom.right.referenced_columns() if atom.right else set()

                # Try to extract table.column from each side
                left_parts = self._parse_qualified_column(left_cols)
                right_parts = self._parse_qualified_column(right_cols)

                if left_parts and right_parts:
                    return JoinEdgePattern(
                        left_table=left_parts[0],
                        right_table=right_parts[0],
                        left_column=left_parts[1],
                        right_column=right_parts[1],
                        join_type=join.join_type.value,
                        predicate_fingerprint=atom.fingerprint(),
                    )

        return None

    def _parse_qualified_column(self, cols: set[str]) -> tuple[str, str] | None:
        """Parse a qualified column name into (table, column)."""
        for col in cols:
            if "." in col:
                parts = col.split(".")
                return (parts[0], parts[1])
        return None

    def _has_outer_join(self, expr: RAExpression) -> bool:
        """Check if expression contains outer joins."""
        if isinstance(expr, Join):
            if expr.join_type in (JoinType.LEFT, JoinType.RIGHT, JoinType.FULL):
                return True
            return self._has_outer_join(expr.left) or self._has_outer_join(expr.right)
        if isinstance(expr, (Selection, Projection, Aggregation)):
            return self._has_outer_join(expr.input)
        if isinstance(expr, (Union, Difference)):
            return self._has_outer_join(expr.left) or self._has_outer_join(expr.right)
        return False

    def _has_distinct(self, expr: RAExpression) -> bool:
        """Check if expression uses DISTINCT."""
        if isinstance(expr, Projection):
            return expr.distinct or self._has_distinct(expr.input)
        if isinstance(expr, (Selection, Aggregation)):
            return self._has_distinct(expr.input)
        if isinstance(expr, Join):
            return self._has_distinct(expr.left) or self._has_distinct(expr.right)
        if isinstance(expr, (Union, Difference)):
            return self._has_distinct(expr.left) or self._has_distinct(expr.right)
        return False

    def _has_union(self, expr: RAExpression) -> bool:
        """Check if expression uses UNION."""
        if isinstance(expr, Union):
            return True
        if isinstance(expr, (Selection, Projection, Aggregation)):
            return self._has_union(expr.input)
        if isinstance(expr, Join):
            return self._has_union(expr.left) or self._has_union(expr.right)
        if isinstance(expr, Difference):
            return self._has_union(expr.left) or self._has_union(expr.right)
        return False
