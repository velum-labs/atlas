"""Greatest Common Subexpression (GCS) computation for RA expressions.

The GCS of multiple queries is the most general query that is contained
in all of them - the "greatest lower bound" in the containment lattice.

For SPJ (Selection-Projection-Join) queries, this is computable via:
1. Relations: Intersection of all relation atoms
2. Joins: Common join predicates
3. Predicates: Relaxed to common implications (weakest predicates implied by all)
4. Projections: Intersection of projected columns

Usage:
    from alma_algebrakit.learning.gcs import greatest_common_subexpression, gcs_all

    # Two expressions
    result = greatest_common_subexpression(expr1, expr2)
    if result.success:
        common_view = result.expression

    # Multiple expressions
    result = gcs_all([expr1, expr2, expr3])
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from alma_algebrakit.models.algebra import (
    Aggregation,
    ColumnRef,
    Difference,
    Intersect,
    Join,
    JoinType,
    Predicate,
    Projection,
    RAExpression,
    Relation,
    Selection,
    Union,
)
from alma_algebrakit.proof.containment import (
    CQAtom,
    CQRepresentation,
    QueryGeneralizer,
)
from alma_algebrakit.visitor import RAExpressionVisitor

# =============================================================================
# Result Types
# =============================================================================


class GCSResult(BaseModel):
    """Result of GCS computation."""

    model_config = {"arbitrary_types_allowed": True}

    success: bool = Field(description="Whether GCS was successfully computed")
    expression: RAExpression | None = Field(
        default=None, description="The GCS expression if successful"
    )
    cq: CQRepresentation | None = Field(
        default=None, description="The CQ representation of the GCS"
    )
    explanation: str = Field(default="")
    common_relations: list[str] = Field(default_factory=list, description="Relations in the GCS")
    common_joins: list[str] = Field(default_factory=list, description="Join predicates in the GCS")
    common_predicates: list[str] = Field(
        default_factory=list, description="Selection predicates in the GCS"
    )
    dropped_predicates: list[str] = Field(
        default_factory=list, description="Predicates that couldn't be generalized"
    )
    input_count: int = Field(default=0, description="Number of input expressions")
    is_spj: bool = Field(default=True, description="Whether all inputs were SPJ queries")


class CQExtractionResult(BaseModel):
    """Result of extracting CQ from an RA expression."""

    model_config = {"arbitrary_types_allowed": True}

    success: bool = Field(description="Whether extraction succeeded")
    cq: CQRepresentation | None = Field(default=None)
    explanation: str = Field(default="")
    is_spj: bool = Field(default=True, description="Whether the expression is SPJ")


# =============================================================================
# RA to CQ Conversion
# =============================================================================


class CQExtractionVisitor(RAExpressionVisitor[None]):
    """Visitor that extracts CQ components from an RA expression.

    Collects:
    - Relations (atoms)
    - Join predicates
    - Selection predicates
    - Projected columns
    """

    def __init__(self) -> None:
        self.relations: list[tuple[str, str]] = []  # (table_name, alias)
        self.join_predicates: list[Predicate] = []
        self.selection_predicates: list[Predicate] = []
        self.projected_columns: list[str] = []
        self.is_spj = True
        self.failure_reason: str | None = None

    def visit_relation(self, expr: Relation) -> None:
        alias = expr.alias or expr.name
        self.relations.append((expr.name, alias))

    def visit_selection(self, expr: Selection) -> None:
        self.selection_predicates.append(expr.predicate)
        expr.input.accept(self)

    def visit_projection(self, expr: Projection) -> None:
        for col_expr, alias in expr.columns:
            if isinstance(col_expr, ColumnRef):
                if col_expr.table:
                    self.projected_columns.append(f"{col_expr.table}.{col_expr.column}")
                else:
                    self.projected_columns.append(col_expr.column)
            elif alias:
                self.projected_columns.append(alias)
        expr.input.accept(self)

    def visit_join(self, expr: Join) -> None:
        if expr.join_type not in (JoinType.INNER, JoinType.CROSS):
            # Outer joins make GCS more complex
            self.is_spj = False
            self.failure_reason = f"Non-inner join type: {expr.join_type}"

        if expr.condition:
            self.join_predicates.append(expr.condition)

        expr.left.accept(self)
        expr.right.accept(self)

    def visit_aggregation(self, expr: Aggregation) -> None:
        self.is_spj = False
        self.failure_reason = "Aggregation not supported in GCS"
        expr.input.accept(self)

    def visit_union(self, expr: Union) -> None:
        self.is_spj = False
        self.failure_reason = "UNION not supported in GCS"

    def visit_difference(self, expr: Difference) -> None:
        self.is_spj = False
        self.failure_reason = "EXCEPT/DIFFERENCE not supported in GCS"

    def visit_intersect(self, expr: Intersect) -> None:
        self.is_spj = False
        self.failure_reason = "INTERSECT not supported in GCS"

    def visit_sort(self, expr: Any) -> None:
        # Sort doesn't affect containment semantics
        expr.input.accept(self)

    def visit_limit(self, expr: Any) -> None:
        self.is_spj = False
        self.failure_reason = "LIMIT not supported in GCS"

    def visit_with_expression(self, expr: Any) -> None:
        self.is_spj = False
        self.failure_reason = "CTE/WITH not supported in GCS"


def ra_to_cq(expr: RAExpression) -> CQExtractionResult:
    """Convert an RA expression to CQ representation.

    Only succeeds for SPJ (Selection-Projection-Join) queries.

    Args:
        expr: The RA expression to convert

    Returns:
        CQExtractionResult with the CQ representation if successful
    """
    visitor = CQExtractionVisitor()
    expr.accept(visitor)

    if not visitor.is_spj:
        return CQExtractionResult(
            success=False,
            is_spj=False,
            explanation=visitor.failure_reason or "Not an SPJ query",
        )

    if not visitor.relations:
        return CQExtractionResult(
            success=False,
            explanation="No relations found",
        )

    # Build CQ atoms from relations
    atoms: list[CQAtom] = []
    variables: set[str] = set()
    bound_columns: dict[str, Any] = {}

    for _table_name, alias in visitor.relations:
        # Create variables for all columns of this relation
        # We use alias_col as variable name
        rel_vars: list[str] = []
        var_name = f"{alias}_*"  # Placeholder for all columns
        rel_vars.append(var_name)
        variables.add(var_name)

        atoms.append(
            CQAtom(
                relation_id=alias,  # Use alias as relation_id for matching
                variables=rel_vars,
            )
        )

    # Build head from projected columns
    head = (
        visitor.projected_columns
        if visitor.projected_columns
        else [f"{a}_*" for _, a in visitor.relations]
    )

    # Combine all predicates
    all_predicates: list[Any] = []
    for pred in visitor.selection_predicates:
        all_predicates.append(pred)
    for pred in visitor.join_predicates:
        all_predicates.append(pred)

    cq = CQRepresentation(
        head=head,
        atoms=atoms,
        predicates=all_predicates,
        variables=variables,
        bound_columns=bound_columns,
    )

    return CQExtractionResult(
        success=True,
        cq=cq,
        is_spj=True,
        explanation=f"Extracted CQ with {len(atoms)} relations",
    )


# =============================================================================
# CQ to RA Conversion
# =============================================================================


def cq_to_ra(cq: CQRepresentation) -> RAExpression:
    """Convert a CQ representation back to an RA expression.

    Builds: π_head(σ_predicates(R1 ⋈ R2 ⋈ ... ⋈ Rn))

    Args:
        cq: The CQ representation

    Returns:
        An RA expression equivalent to the CQ
    """
    if not cq.atoms:
        raise ValueError("CQ has no relation atoms")

    # Build base relations
    relations: list[RAExpression] = []
    for atom in cq.atoms:
        rel = Relation(name=atom.relation_id, alias=atom.relation_id)
        relations.append(rel)

    # Build join tree (left-deep)
    if len(relations) == 1:
        result: RAExpression = relations[0]
    else:
        result = relations[0]
        for rel in relations[1:]:
            # Cross join - predicates added as selection
            result = Join(
                left=result,
                right=rel,
                join_type=JoinType.CROSS,
                condition=None,
            )

    # Add selection predicates
    if cq.predicates:
        from alma_algebrakit.models.algebra import CompoundPredicate, LogicalOp

        if len(cq.predicates) == 1:
            result = Selection(predicate=cq.predicates[0], input=result)
        else:
            # Combine with AND
            combined = CompoundPredicate(
                op=LogicalOp.AND,
                operands=list(cq.predicates),
            )
            result = Selection(predicate=combined, input=result)

    # Add projection
    if cq.head:
        columns: list[tuple[Any, str | None]] = []
        for var in cq.head:
            # Parse variable name to column reference
            if "." in var:
                parts = var.split(".", 1)
                col_ref = ColumnRef(table=parts[0], column=parts[1])
            else:
                col_ref = ColumnRef(column=var)
            columns.append((col_ref, None))

        if columns:
            result = Projection(columns=columns, input=result)

    return result


# =============================================================================
# GCS Computation
# =============================================================================


def greatest_common_subexpression(
    a: RAExpression,
    b: RAExpression,
) -> GCSResult:
    """Compute the Greatest Common Subexpression of two RA expressions.

    The GCS is the most general query that is contained in both a and b.
    This is the "meet" (greatest lower bound) in the containment lattice.

    For SPJ queries:
    - Relations: a.relations ∩ b.relations
    - Joins: Common join predicates
    - Predicates: Relaxed to weakest common implications
    - Projections: a.projections ∩ b.projections

    Args:
        a: First RA expression
        b: Second RA expression

    Returns:
        GCSResult with the GCS expression if successful
    """
    return gcs_all([a, b])


def gcs_all(expressions: list[RAExpression]) -> GCSResult:
    """Compute the Greatest Common Subexpression of multiple RA expressions.

    Args:
        expressions: List of RA expressions

    Returns:
        GCSResult with the GCS expression if successful
    """
    if not expressions:
        return GCSResult(
            success=False,
            explanation="No input expressions",
            input_count=0,
        )

    if len(expressions) == 1:
        # GCS of single expression is itself
        cq_result = ra_to_cq(expressions[0])
        return GCSResult(
            success=True,
            expression=expressions[0],
            cq=cq_result.cq,
            explanation="Single expression, GCS is itself",
            common_relations=[atom.relation_id for atom in cq_result.cq.atoms]
            if cq_result.cq
            else [],
            input_count=1,
            is_spj=cq_result.is_spj,
        )

    # Convert all expressions to CQ
    cqs: list[CQRepresentation] = []
    all_spj = True

    for _i, expr in enumerate(expressions):
        cq_result = ra_to_cq(expr)
        if not cq_result.success:
            all_spj = False
            # Continue anyway - we'll try to extract what we can
            if cq_result.cq:
                cqs.append(cq_result.cq)
        else:
            if cq_result.cq:
                cqs.append(cq_result.cq)
        if not cq_result.is_spj:
            all_spj = False

    if not cqs:
        return GCSResult(
            success=False,
            explanation="Could not convert any expression to CQ",
            input_count=len(expressions),
            is_spj=False,
        )

    # Use QueryGeneralizer to compute GLB
    generalizer = QueryGeneralizer()
    glb_result = generalizer.compute_glb(cqs)

    if not glb_result.success or glb_result.cq is None:
        return GCSResult(
            success=False,
            explanation=glb_result.explanation,
            input_count=len(expressions),
            is_spj=all_spj,
            dropped_predicates=glb_result.dropped_predicates,
        )

    # Convert back to RA
    try:
        ra_expr = cq_to_ra(glb_result.cq)
    except (ValueError, KeyError) as e:
        return GCSResult(
            success=False,
            cq=glb_result.cq,
            explanation=f"Could not convert GCS back to RA: {e}",
            common_relations=glb_result.common_relations,
            common_predicates=glb_result.common_predicates,
            dropped_predicates=glb_result.dropped_predicates,
            input_count=len(expressions),
            is_spj=all_spj,
        )

    # Extract join fingerprints for reporting
    common_joins: list[str] = []
    for pred in glb_result.cq.predicates:
        if hasattr(pred, "fingerprint"):
            common_joins.append(pred.fingerprint())

    return GCSResult(
        success=True,
        expression=ra_expr,
        cq=glb_result.cq,
        explanation=glb_result.explanation,
        common_relations=glb_result.common_relations,
        common_joins=common_joins,
        common_predicates=glb_result.common_predicates,
        dropped_predicates=glb_result.dropped_predicates,
        input_count=len(expressions),
        is_spj=all_spj,
    )


# =============================================================================
# Utilities
# =============================================================================


def is_spj_query(expr: RAExpression) -> bool:
    """Check if an RA expression is an SPJ (Selection-Projection-Join) query.

    SPJ queries consist only of:
    - Base relations
    - Inner joins (with equi-join predicates)
    - Selections
    - Projections

    Args:
        expr: The RA expression to check

    Returns:
        True if the expression is SPJ
    """
    result = ra_to_cq(expr)
    return result.is_spj


def extract_common_structure(
    expressions: list[RAExpression],
    relation_threshold: float = 0.5,
) -> GCSResult:
    """Extract common structure from expressions with a support threshold.

    Unlike strict GCS, this allows relations that appear in at least
    `relation_threshold` fraction of the expressions.

    This is more flexible for workload analysis where exact GCS
    may be too restrictive.

    Args:
        expressions: List of RA expressions
        relation_threshold: Minimum fraction of expressions a relation must appear in

    Returns:
        GCSResult with the common structure
    """
    if not expressions:
        return GCSResult(
            success=False,
            explanation="No input expressions",
            input_count=0,
        )

    # Convert to CQ
    cqs: list[CQRepresentation] = []
    for expr in expressions:
        cq_result = ra_to_cq(expr)
        if cq_result.cq:
            cqs.append(cq_result.cq)

    if not cqs:
        return GCSResult(
            success=False,
            explanation="Could not convert any expression to CQ",
            input_count=len(expressions),
        )

    # Use QueryGeneralizer's common structure computation
    generalizer = QueryGeneralizer()
    result = generalizer.compute_common_structure(cqs)

    if not result.success or result.cq is None:
        return GCSResult(
            success=False,
            explanation=result.explanation,
            input_count=len(expressions),
        )

    # Convert back to RA
    try:
        ra_expr = cq_to_ra(result.cq)
    except (ValueError, KeyError) as e:
        return GCSResult(
            success=False,
            cq=result.cq,
            explanation=f"Could not convert to RA: {e}",
            common_relations=result.common_relations,
            input_count=len(expressions),
        )

    return GCSResult(
        success=True,
        expression=ra_expr,
        cq=result.cq,
        explanation=result.explanation,
        common_relations=result.common_relations,
        common_predicates=result.common_predicates,
        dropped_predicates=result.dropped_predicates,
        input_count=len(expressions),
    )
