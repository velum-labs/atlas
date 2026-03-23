"""Core types for RA-level query folding.

This module provides SQL-agnostic types for folding (factoring out) views
from relational algebra expressions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from alma_algebrakit.models.algebra import (
    JoinType,
    Predicate,
    RAExpression,
    RelationRef,
)
from alma_algebrakit.rewriting.predicates import PredicateClassification

if TYPE_CHECKING:
    from alma_algebrakit.schema.constraints import SchemaConstraints


class FoldCoverage(Enum):
    """Type of coverage when folding a query against a view."""

    FULL = "full"  # View covers all query tables
    PARTIAL = "partial"  # View covers some query tables
    NONE = "none"  # No overlap between view and query


@dataclass(frozen=True)
class ViewSpecification:
    """Specification of a view for RA-level folding.

    This is SQL-agnostic - it contains only the information needed to perform
    algebraic folding operations. SQL-specific details (parsing, emission)
    are handled by higher-level layers.

    Attributes:
        name: View name (for reference in rewritten RA)
        ra_expression: Parsed RA expression of the view (optional)
        base_tables: Normalized table names the view is built from
        column_lineage: Maps (source_table, source_col) -> view_output_col
        exposed_columns: Set of column names exposed by the view
        is_select_star: Whether view is SELECT * (passthrough mapping)
        schema_constraints: Schema constraints for extended reasoning (PK, FK, NOT NULL)
    """

    name: str
    ra_expression: RAExpression | None
    base_tables: frozenset[str]
    column_lineage: dict[tuple[str, str], str]
    exposed_columns: frozenset[str]
    is_select_star: bool = False
    schema_constraints: SchemaConstraints | None = None


@dataclass
class JoinStep:
    """A single join step in the rewritten query.

    Represents one JOIN clause in the final rewritten expression.

    Attributes:
        join_type: Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
        right_rel: The relation being joined (alias-safe)
        on_predicate: Original ON predicate (for reference)
        on_sql: SQL string for ON condition (with column refs rewritten)
        is_boundary: True if this connects view to remaining relations
    """

    join_type: JoinType
    right_rel: RelationRef
    on_predicate: Predicate | None
    on_sql: str = ""
    is_boundary: bool = False


@dataclass
class BoundaryAnalysis:
    """Complete analysis for partial coverage rewrite.

    Captures all information needed to construct a semantically equivalent
    rewritten query when a view covers only a subset of query tables.

    Attributes:
        covered_rels: Relations covered by the view
        remaining_rels: Relations not covered by the view
        join_plan: Ordered list of joins to construct
        predicates: Classification of predicates (keep vs drop)
        required_cv_columns: View columns needed by boundary joins
        missing_cv_columns: Required columns not available in view
        column_rewrite_map: Maps original columns to view columns
        is_valid: Whether the partial rewrite is valid
        rejection_reasons: Why the rewrite was rejected (if invalid)
        self_join_instances: Groups of self-join instances by physical table
            For multi-instance rewriting: {"users": [u1, u2]} means both u1 and u2
            reference "users" and need separate view instances.
        effective_join_types: Mapping of join edges to their effective types
            after constraint-based inference (e.g., LEFT→INNER when FK+NOT NULL)
    """

    covered_rels: set[RelationRef] = field(default_factory=set)
    remaining_rels: set[RelationRef] = field(default_factory=set)
    join_plan: list[JoinStep] = field(default_factory=list)
    # PredicateClassification is imported from alma_algebrakit.rewriting.predicates
    predicates: PredicateClassification | None = None
    required_cv_columns: set[str] = field(default_factory=set)
    missing_cv_columns: set[str] = field(default_factory=set)
    column_rewrite_map: dict[str, str] = field(default_factory=dict)
    is_valid: bool = True
    rejection_reasons: list[str] = field(default_factory=list)
    # Extended fields for decidable extensions
    self_join_instances: dict[str, list[RelationRef]] = field(default_factory=dict)
    effective_join_types: dict[str, JoinType] = field(default_factory=dict)


@dataclass
class FoldResult:
    """Result of attempting to fold a query using a view.

    For FULL coverage, the rewritten_ra contains the complete rewritten
    expression with the view replacing base tables.

    For PARTIAL coverage, boundary_analysis contains the information
    needed for SQL-level rewriting (join plan, column mappings, etc.).

    Attributes:
        success: Whether folding succeeded
        coverage: Type of coverage achieved
        rewritten_ra: Rewritten RA expression (for FULL coverage)
        boundary_analysis: Analysis for partial coverage
        column_rewrite_map: Maps original columns to view columns
        kept_predicates: Predicates that must remain in WHERE
        dropped_predicates: Predicates absorbed by the view
        rejection_reasons: Why folding failed or was limited
    """

    success: bool
    coverage: FoldCoverage
    rewritten_ra: RAExpression | None = None
    boundary_analysis: BoundaryAnalysis | None = None
    column_rewrite_map: dict[str, str] = field(default_factory=dict)
    kept_predicates: list[Predicate] = field(default_factory=list)
    dropped_predicates: list[Predicate] = field(default_factory=list)
    rejection_reasons: list[str] = field(default_factory=list)
