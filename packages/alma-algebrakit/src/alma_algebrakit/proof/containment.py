"""Conjunctive query containment checking.

For SPJ (Select-Project-Join) queries, containment is decidable via
homomorphism: Q1 ⊆ Q2 iff there exists a homomorphism h: Q2 → Q1.

For SPJG (with GROUP BY), additional conditions on aggregates apply.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from alma_algebrakit.bound.types import (
    AttributeRef,
    BoundBetween,
    BoundLiteral,
    BoundLogical,
    BoundPredicate,
)
from alma_algebrakit.proof.implication import ImplicationResult, PredicateImplicationChecker


class ContainmentMethod(StrEnum):
    """Method used to prove containment."""

    HOMOMORPHISM = "homomorphism"  # Classic CQ containment
    STRUCTURAL = "structural"  # Exact structure match
    VALIDATED = "validated"  # Empirically validated
    UNKNOWN = "unknown"


class ContainmentResult(BaseModel):
    """Result of containment checking."""

    contained: bool | None = Field(
        description="True if Q1 ⊆ Q2 proven, False if disproven, None if unknown"
    )
    method: ContainmentMethod = Field(description="Method used")
    explanation: str = Field(default="")
    homomorphism: dict[str, str] | None = Field(
        default=None, description="Variable mapping if found"
    )
    predicate_proofs: list[ImplicationResult] = Field(
        default_factory=list, description="Predicate implication proofs"
    )
    blocking_reasons: list[str] = Field(
        default_factory=list, description="Reasons containment couldn't be proven"
    )


@dataclass
class CQAtom:
    """A relation atom in a conjunctive query representation."""

    relation_id: str  # Table identifier
    variables: list[str]  # Variable names for each column position


@dataclass
class CQRepresentation:
    """Conjunctive query representation for containment checking.

    A CQ has the form:
        Q(x1, ..., xk) :- R1(y1), R2(y2), ..., φ(predicates)

    Where:
    - x1..xk are the head variables (projected columns)
    - R1, R2, ... are relation atoms
    - φ is a conjunction of comparison predicates
    """

    head: list[str]  # Output variable names
    atoms: list[CQAtom]  # Relation atoms
    predicates: list[BoundPredicate]  # Comparison predicates
    variables: set[str]  # All variables
    bound_columns: dict[str, AttributeRef]  # Variable -> bound column mapping


class ContainmentChecker:
    """Checks query containment using homomorphism-based approach.

    For SPJ queries (conjunctive queries), Q1 ⊆ Q2 iff there exists
    a homomorphism h: Q2 → Q1 such that:
    1. For each atom R(x1..xn) in Q2, R(h(x1)..h(xn)) is in Q1
    2. h maps head variables of Q2 to head variables of Q1
    3. For each predicate p in Q2, Q1's predicates imply p under h

    Usage:
        checker = ContainmentChecker()
        result = checker.check_containment(query_cq, view_cq)
    """

    def __init__(self) -> None:
        self.implication_checker = PredicateImplicationChecker()

    def check_containment(
        self,
        q1: CQRepresentation,
        q2: CQRepresentation,
    ) -> ContainmentResult:
        """Check if Q1 ⊆ Q2 (Q1 is contained in Q2).

        This means every tuple in Q1 is also in Q2.
        For view-based rewriting, we need Q ⊆ V (query contained in view).

        Args:
            q1: The potentially contained query
            q2: The potentially containing query/view

        Returns:
            ContainmentResult indicating whether Q1 ⊆ Q2
        """
        blocking = []

        # Step 1: Check relation atoms compatibility
        q1_relations = {atom.relation_id for atom in q1.atoms}
        q2_relations = {atom.relation_id for atom in q2.atoms}

        if not q1_relations <= q2_relations:
            missing = q1_relations - q2_relations
            blocking.append(f"Q1 uses relations not in Q2: {missing}")
            return ContainmentResult(
                contained=None,
                method=ContainmentMethod.UNKNOWN,
                explanation="Relation mismatch",
                blocking_reasons=blocking,
            )

        # Step 2: Try to find homomorphism h: Q2 → Q1
        homomorphism = self._find_homomorphism(q1, q2)

        if homomorphism is None:
            blocking.append("No homomorphism found from Q2 to Q1")
            return ContainmentResult(
                contained=None,
                method=ContainmentMethod.UNKNOWN,
                explanation="Homomorphism search failed",
                blocking_reasons=blocking,
            )

        # Step 3: Check head variable mapping
        head_valid, head_reason = self._check_head_mapping(q1, q2, homomorphism)
        if not head_valid:
            blocking.append(head_reason)
            return ContainmentResult(
                contained=None,
                method=ContainmentMethod.UNKNOWN,
                explanation="Head mapping invalid",
                blocking_reasons=blocking,
                homomorphism=homomorphism,
            )

        # Step 4: Check predicate implications
        pred_proofs = []
        for pred in q2.predicates:
            mapped_pred = self._apply_mapping_to_predicate(pred, homomorphism)
            if mapped_pred is None:
                blocking.append(f"Could not map predicate: {pred}")
                continue

            impl_result = self._check_predicate_implied(q1.predicates, mapped_pred)
            pred_proofs.append(impl_result)

            if not impl_result.is_proven():
                blocking.append(f"Predicate not implied: {mapped_pred}")

        if blocking:
            return ContainmentResult(
                contained=None,
                method=ContainmentMethod.HOMOMORPHISM,
                explanation="Predicate implications not proven",
                homomorphism=homomorphism,
                predicate_proofs=pred_proofs,
                blocking_reasons=blocking,
            )

        return ContainmentResult(
            contained=True,
            method=ContainmentMethod.HOMOMORPHISM,
            explanation="Containment proven via homomorphism",
            homomorphism=homomorphism,
            predicate_proofs=pred_proofs,
        )

    def _find_homomorphism(
        self,
        q1: CQRepresentation,
        q2: CQRepresentation,
    ) -> dict[str, str] | None:
        """Find a homomorphism h: Q2 → Q1 using backtracking search."""
        q1_atoms_by_rel: dict[str, list[CQAtom]] = {}
        for atom in q1.atoms:
            if atom.relation_id not in q1_atoms_by_rel:
                q1_atoms_by_rel[atom.relation_id] = []
            q1_atoms_by_rel[atom.relation_id].append(atom)

        mapping: dict[str, str] = {}

        def backtrack(atom_idx: int) -> bool:
            if atom_idx >= len(q2.atoms):
                return True

            atom = q2.atoms[atom_idx]
            rel = atom.relation_id

            if rel not in q1_atoms_by_rel:
                return False

            for q1_atom in q1_atoms_by_rel[rel]:
                if len(q1_atom.variables) != len(atom.variables):
                    continue

                old_mapping = mapping.copy()
                valid = True

                for q2_var, q1_var in zip(atom.variables, q1_atom.variables, strict=False):
                    if q2_var in mapping:
                        if mapping[q2_var] != q1_var:
                            valid = False
                            break
                    else:
                        mapping[q2_var] = q1_var

                if valid and backtrack(atom_idx + 1):
                    return True

                mapping.clear()
                mapping.update(old_mapping)

            return False

        if backtrack(0):
            return mapping

        return None

    def _check_head_mapping(
        self,
        q1: CQRepresentation,
        q2: CQRepresentation,
        h: dict[str, str],
    ) -> tuple[bool, str]:
        """Check if homomorphism maps Q2's head to Q1's head correctly."""
        if len(q2.head) != len(q1.head):
            return False, f"Head arity mismatch: {len(q2.head)} vs {len(q1.head)}"

        for i, (q2_var, q1_var) in enumerate(zip(q2.head, q1.head, strict=False)):
            if q2_var in h:
                if h[q2_var] != q1_var:
                    return False, f"Head variable {i} maps to wrong position"
            else:
                return False, f"Head variable {q2_var} not in homomorphism"

        return True, ""

    def _apply_mapping_to_predicate(
        self,
        pred: BoundPredicate,
        h: dict[str, str],
    ) -> BoundPredicate | None:
        """Apply homomorphism mapping to a predicate's variables.

        Substitutes variable references according to the homomorphism mapping.
        For example, if h = {x: y}, then predicate "x > 5" becomes "y > 5".
        """
        from alma_algebrakit.bound.types import (
            BoundBetween,
            BoundComparison,
            BoundExpression,
            BoundIn,
            BoundIsNull,
            BoundLike,
            BoundLogical,
        )

        def apply_to_expr(expr: Any) -> Any:
            """Apply mapping to an expression."""
            if isinstance(expr, AttributeRef):
                # Map the column reference
                qual_id = expr.qualified_id()
                if qual_id in h:
                    # Create new AttributeRef with mapped ID
                    new_id = h[qual_id]
                    # Extract table and column from new_id
                    if "." in new_id:
                        parts = new_id.split(".", 1)
                        return AttributeRef(
                            table_id=parts[0],
                            column_id=parts[1],
                            sql_name=expr.sql_name,
                            table_alias=parts[0],
                            data_type=expr.data_type,
                        )
                    return expr  # Keep original if can't parse
                return expr  # Keep if not in mapping

            if isinstance(expr, BoundLiteral):
                return expr  # Literals don't change

            if isinstance(expr, BoundExpression):
                # Map upstream columns
                new_upstream = [apply_to_expr(c) for c in expr.upstream_columns]
                # Create new expression with mapped columns
                return BoundExpression(
                    upstream_columns=[c for c in new_upstream if isinstance(c, AttributeRef)],
                    result_type=expr.result_type,
                    sql_text=expr.sql_text,
                )

            return expr

        def apply_to_pred(p: BoundPredicate) -> BoundPredicate | None:
            """Apply mapping recursively to a predicate."""
            if isinstance(p, BoundComparison):
                new_left = apply_to_expr(p.left)
                new_right = apply_to_expr(p.right)
                new_upstream = [apply_to_expr(c) for c in p.upstream_columns]
                return BoundComparison(
                    left=new_left,
                    operator=p.operator,
                    right=new_right,
                    upstream_columns=[c for c in new_upstream if isinstance(c, AttributeRef)],
                )

            if isinstance(p, BoundIsNull):
                new_expr = apply_to_expr(p.expression)
                new_upstream = [apply_to_expr(c) for c in p.upstream_columns]
                return BoundIsNull(
                    expression=new_expr,
                    negated=p.negated,
                    upstream_columns=[c for c in new_upstream if isinstance(c, AttributeRef)],
                )

            if isinstance(p, BoundIn):
                new_expr = apply_to_expr(p.expression)
                new_values = [apply_to_expr(v) for v in p.values]
                new_upstream = [apply_to_expr(c) for c in p.upstream_columns]
                return BoundIn(
                    expression=new_expr,
                    values=new_values,
                    negated=p.negated,
                    upstream_columns=[c for c in new_upstream if isinstance(c, AttributeRef)],
                )

            if isinstance(p, BoundBetween):
                new_expr = apply_to_expr(p.expression)
                new_low = apply_to_expr(p.low)
                new_high = apply_to_expr(p.high)
                new_upstream = [apply_to_expr(c) for c in p.upstream_columns]
                return BoundBetween(
                    expression=new_expr,
                    low=new_low,
                    high=new_high,
                    upstream_columns=[c for c in new_upstream if isinstance(c, AttributeRef)],
                )

            if isinstance(p, BoundLike):
                new_expr = apply_to_expr(p.expression)
                new_pattern = apply_to_expr(p.pattern)
                new_upstream = [apply_to_expr(c) for c in p.upstream_columns]
                return BoundLike(
                    expression=new_expr,
                    pattern=new_pattern,
                    case_insensitive=p.case_insensitive,
                    negated=p.negated,
                    upstream_columns=[c for c in new_upstream if isinstance(c, AttributeRef)],
                )

            if isinstance(p, BoundLogical):
                new_operands = []
                for op in p.operands:
                    mapped = apply_to_pred(op)
                    if mapped is None:
                        return None
                    new_operands.append(mapped)
                new_upstream = [apply_to_expr(c) for c in p.upstream_columns]
                return BoundLogical(
                    operator=p.operator,
                    operands=new_operands,
                    upstream_columns=[c for c in new_upstream if isinstance(c, AttributeRef)],
                )

            return None

        return apply_to_pred(pred)

    def _check_predicate_implied(
        self,
        q1_predicates: list[BoundPredicate],
        target_pred: BoundPredicate,
    ) -> ImplicationResult:
        """Check if Q1's predicates imply the target predicate."""
        if not q1_predicates:
            return ImplicationResult(
                implies=None,
                method="unknown",
                explanation="Q1 has no predicates",
            )

        if len(q1_predicates) == 1:
            combined = q1_predicates[0]
        else:
            combined = BoundLogical(
                operator="AND",
                operands=q1_predicates,
                upstream_columns=[c for p in q1_predicates for c in p.upstream_columns],
            )

        return self.implication_checker.check_implication(combined, target_pred)


def cq_from_bound_query(query: Any) -> CQRepresentation | None:
    """Convert a BoundQuery to CQ representation.

    Only succeeds for SPJ queries without subqueries, DISTINCT, etc.
    """
    from alma_algebrakit.bound.query import BoundQuery

    if not isinstance(query, BoundQuery):
        return None

    if query.distinct:
        return None

    if query.limit is not None:
        return None

    atoms = []
    variables: set[str] = set()
    bound_columns: dict[str, AttributeRef] = {}

    for rel in query.from_relations:
        rel_vars = []
        for col_name, col_ref in rel.columns.items():
            var_name = f"{rel.alias}_{col_name}"
            rel_vars.append(var_name)
            variables.add(var_name)
            bound_columns[var_name] = col_ref

        atoms.append(
            CQAtom(
                relation_id=rel.table_id,
                variables=rel_vars,
            )
        )

    head = []
    for item in query.columns:
        if item.is_star:
            for col in item.expanded_columns:
                var_name = f"{col.table_alias or col.table_id}_{col.sql_name}"
                if var_name in variables:
                    head.append(var_name)
        elif isinstance(item.expression, AttributeRef):
            col = item.expression
            var_name = f"{col.table_alias or col.table_id}_{col.sql_name}"
            if var_name in variables:
                head.append(var_name)
            else:
                head.append(item.alias or col.sql_name)

    predicates = []
    if query.where:
        predicates.append(query.where)

    return CQRepresentation(
        head=head,
        atoms=atoms,
        predicates=predicates,
        variables=variables,
        bound_columns=bound_columns,
    )


# =============================================================================
# Query Generalization Operations
# =============================================================================


class GeneralizationResult(BaseModel):
    """Result of query generalization (LUB/GLB)."""

    success: bool = Field(description="Whether generalization succeeded")
    cq: CQRepresentation | None = Field(default=None, description="Resulting CQ if successful")
    explanation: str = Field(default="")
    common_relations: list[str] = Field(default_factory=list)
    common_predicates: list[str] = Field(
        default_factory=list, description="Fingerprints of common predicates"
    )
    dropped_predicates: list[str] = Field(
        default_factory=list, description="Fingerprints of predicates not included"
    )
    input_count: int = Field(default=0, description="Number of input queries")


class QueryGeneralizer:
    """Computes generalizations of multiple queries for contract derivation.

    Supports two operations:
    1. LUB (Least Upper Bound): Most specific query that contains all inputs
       - Relations: Union of all relation atoms
       - Predicates: Intersection (conjunction) of predicates
       - Head: Intersection of projected columns

    2. GLB (Greatest Lower Bound): Most general query contained in all inputs
       - Relations: Intersection of all relation atoms
       - Predicates: Relaxed to common implications
       - Head: Intersection of projected columns

    Usage:
        generalizer = QueryGeneralizer()
        lub = generalizer.compute_lub([q1_cq, q2_cq, q3_cq])
        glb = generalizer.compute_glb([q1_cq, q2_cq, q3_cq])
    """

    def __init__(self) -> None:
        self.implication_checker = PredicateImplicationChecker()
        self.containment_checker = ContainmentChecker()

    def compute_lub(
        self,
        queries: list[CQRepresentation],
    ) -> GeneralizationResult:
        """Compute Least Upper Bound (LUB) of multiple queries.

        LUB(Q1, Q2, ...) is the most general query that contains all Qi.
        Every tuple in any Qi is also in LUB.

        For conjunctive queries, a query is *more general* (higher in the lattice)
        when it has fewer atoms (fewer joins → more answers). Adding atoms makes a
        CQ more restrictive, not more general. Therefore:
        - Relations: R1 ∩ R2 ∩ ... (intersection — keep only atoms common to all)
        - Predicates: keep only predicates shared by all queries
        - Head: A1 ∩ A2 ∩ ... (intersection of projections)

        Args:
            queries: List of CQ representations

        Returns:
            GeneralizationResult with the LUB if successful
        """
        if not queries:
            return GeneralizationResult(
                success=False,
                explanation="No input queries",
            )

        if len(queries) == 1:
            return GeneralizationResult(
                success=True,
                cq=queries[0],
                explanation="Single query, LUB is itself",
                common_relations=[a.relation_id for a in queries[0].atoms],
                input_count=1,
            )

        # Step 1: Compute intersection of relation atoms
        common_relation_ids: set[str] = {a.relation_id for a in queries[0].atoms}
        for q in queries[1:]:
            q_ids = {a.relation_id for a in q.atoms}
            common_relation_ids &= q_ids

        if not common_relation_ids:
            return GeneralizationResult(
                success=False,
                explanation="No common relations across queries",
                input_count=len(queries),
            )

        # Build atoms for common relations (use first query as template)
        common_atoms: list[CQAtom] = [
            a for a in queries[0].atoms if a.relation_id in common_relation_ids
        ]
        common_variables: set[str] = set()
        common_bound_columns: dict[str, AttributeRef] = {}
        for atom in common_atoms:
            common_variables.update(atom.variables)
            for var in atom.variables:
                if var in queries[0].bound_columns:
                    common_bound_columns[var] = queries[0].bound_columns[var]

        # Step 2: Compute intersection of heads (common projected columns)
        common_head = set(queries[0].head)
        for q in queries[1:]:
            common_head &= set(q.head)
        common_head = common_head & common_variables
        common_head_list = sorted(common_head)

        # Step 3: Keep only predicates shared by ALL queries
        # A predicate is "shared" if its fingerprint appears in every query's predicate list
        shared_fps: set[str] | None = None
        for q in queries:
            q_fps = {self._predicate_fingerprint(p) for p in q.predicates}
            if shared_fps is None:
                shared_fps = q_fps
            else:
                shared_fps &= q_fps

        first_pred_map = {self._predicate_fingerprint(p): p for p in queries[0].predicates}
        shared_predicates: list[BoundPredicate] = []
        shared_predicate_fps: list[str] = []
        dropped_fps: list[str] = []

        for fp, pred in first_pred_map.items():
            if shared_fps and fp in shared_fps and self._predicate_uses_only(pred, common_variables):
                shared_predicates.append(pred)
                shared_predicate_fps.append(fp)
            else:
                dropped_fps.append(fp)

        # Build the LUB CQ
        lub_cq = CQRepresentation(
            head=common_head_list,
            atoms=common_atoms,
            predicates=shared_predicates,
            variables=common_variables,
            bound_columns=common_bound_columns,
        )

        return GeneralizationResult(
            success=True,
            cq=lub_cq,
            explanation=f"LUB of {len(queries)} queries with {len(common_atoms)} common atoms",
            common_relations=list(common_relation_ids),
            common_predicates=shared_predicate_fps,
            dropped_predicates=dropped_fps,
            input_count=len(queries),
        )

    def compute_glb(
        self,
        queries: list[CQRepresentation],
    ) -> GeneralizationResult:
        """Compute Greatest Lower Bound (GLB) of multiple queries.

        GLB(Q1, Q2, ...) is the most general query that is contained in all Qi.
        Every tuple in GLB is also in all Qi.

        For conjunctive queries:
        - Relations: R1 ∩ R2 ∩ ... (intersection of all atoms)
        - Predicates: Relaxed predicates that hold in all queries
        - Head: A1 ∩ A2 ∩ ... (intersection of projections)

        Args:
            queries: List of CQ representations

        Returns:
            GeneralizationResult with the GLB if successful, or None if no common structure
        """
        if not queries:
            return GeneralizationResult(
                success=False,
                explanation="No input queries",
            )

        if len(queries) == 1:
            return GeneralizationResult(
                success=True,
                cq=queries[0],
                explanation="Single query, GLB is itself",
                common_relations=[a.relation_id for a in queries[0].atoms],
                input_count=1,
            )

        # Step 1: Compute intersection of relations
        common_relations = {a.relation_id for a in queries[0].atoms}
        for q in queries[1:]:
            q_relations = {a.relation_id for a in q.atoms}
            common_relations &= q_relations

        if not common_relations:
            return GeneralizationResult(
                success=False,
                explanation="No common relations across queries",
                input_count=len(queries),
            )

        # Step 2: Build atoms for common relations
        # Use the first query's atoms for common relations as template
        common_atoms: list[CQAtom] = []
        common_variables: set[str] = set()
        common_bound_columns: dict[str, AttributeRef] = {}

        for atom in queries[0].atoms:
            if atom.relation_id in common_relations:
                common_atoms.append(atom)
                common_variables.update(atom.variables)
                for var in atom.variables:
                    if var in queries[0].bound_columns:
                        common_bound_columns[var] = queries[0].bound_columns[var]

        # Step 3: Compute intersection of heads
        common_head = set(queries[0].head)
        for q in queries[1:]:
            common_head &= set(q.head)

        # Filter head to only include variables from common atoms
        common_head = common_head & common_variables
        common_head_list = sorted(common_head)

        # Step 4: Find and relax predicates
        # Use predicate relaxation to find weakest common predicates
        pred_sets = [q.predicates for q in queries]
        common_predicates, dropped_predicates = self._relax_predicates(pred_sets, common_variables)

        if not common_head_list:
            return GeneralizationResult(
                success=False,
                explanation="No common projected columns",
                common_relations=list(common_relations),
                dropped_predicates=dropped_predicates,
                input_count=len(queries),
            )

        # Build the GLB CQ
        glb_cq = CQRepresentation(
            head=common_head_list,
            atoms=common_atoms,
            predicates=common_predicates,
            variables=common_variables,
            bound_columns=common_bound_columns,
        )

        return GeneralizationResult(
            success=True,
            cq=glb_cq,
            explanation=f"GLB of {len(queries)} queries with {len(common_atoms)} common atoms",
            common_relations=list(common_relations),
            common_predicates=[self._predicate_fingerprint(p) for p in common_predicates],
            dropped_predicates=dropped_predicates,
            input_count=len(queries),
        )

    def compute_common_structure(
        self,
        queries: list[CQRepresentation],
    ) -> GeneralizationResult:
        """Compute the common structure across queries.

        This is more flexible than GLB - it finds:
        - Relations that appear in at least a threshold of queries
        - Predicates that appear in most queries
        - Common join patterns

        Useful for contract derivation where exact GLB may be too restrictive.

        Args:
            queries: List of CQ representations

        Returns:
            GeneralizationResult with common structure
        """
        if not queries:
            return GeneralizationResult(
                success=False,
                explanation="No input queries",
            )

        # Count relation frequency
        relation_count: dict[str, int] = {}
        for q in queries:
            for atom in q.atoms:
                relation_count[atom.relation_id] = relation_count.get(atom.relation_id, 0) + 1

        # Relations appearing in at least 50% of queries
        threshold = len(queries) / 2
        common_relations = [r for r, c in relation_count.items() if c >= threshold]

        if not common_relations:
            return GeneralizationResult(
                success=False,
                explanation="No relations meet threshold",
                input_count=len(queries),
            )

        # Build atoms from template (first query that has them)
        common_atoms: list[CQAtom] = []
        common_variables: set[str] = set()
        common_bound_columns: dict[str, AttributeRef] = {}

        for rel_id in common_relations:
            # Find first query with this relation
            for q in queries:
                for atom in q.atoms:
                    if atom.relation_id == rel_id:
                        common_atoms.append(atom)
                        common_variables.update(atom.variables)
                        for var in atom.variables:
                            if var in q.bound_columns:
                                common_bound_columns[var] = q.bound_columns[var]
                        break
                else:
                    continue
                break

        # Find common head variables
        all_heads = [set(q.head) for q in queries]
        if all_heads:
            common_head = set.intersection(*all_heads) if len(all_heads) > 1 else all_heads[0]
            common_head = common_head & common_variables
        else:
            common_head = set()

        # Find predicates appearing in most queries
        predicate_counts: dict[str, tuple[int, BoundPredicate]] = {}
        for q in queries:
            for pred in q.predicates:
                fp = self._predicate_fingerprint(pred)
                if fp in predicate_counts:
                    predicate_counts[fp] = (predicate_counts[fp][0] + 1, pred)
                else:
                    predicate_counts[fp] = (1, pred)

        common_predicates = [
            pred
            for count, pred in predicate_counts.values()
            if count >= threshold and self._predicate_uses_only(pred, common_variables)
        ]

        common_cq = CQRepresentation(
            head=sorted(common_head),
            atoms=common_atoms,
            predicates=common_predicates,
            variables=common_variables,
            bound_columns=common_bound_columns,
        )

        return GeneralizationResult(
            success=True,
            cq=common_cq,
            explanation=f"Common structure from {len(queries)} queries",
            common_relations=common_relations,
            common_predicates=[self._predicate_fingerprint(p) for p in common_predicates],
            input_count=len(queries),
        )

    @staticmethod
    def _predicate_fingerprint(pred: BoundPredicate) -> str:
        """Generate a fingerprint for a predicate.

        Delegates to the canonical implementation in bound.fingerprint.
        """
        from alma_algebrakit.bound.fingerprint import bound_predicate_fingerprint

        return bound_predicate_fingerprint(pred)

    @staticmethod
    def _expr_fingerprint(expr: Any) -> str:
        """Generate a fingerprint for an expression.

        Delegates to the canonical implementation in bound.fingerprint.
        """
        from alma_algebrakit.bound.fingerprint import bound_expr_fingerprint

        return bound_expr_fingerprint(expr)

    def _predicates_equivalent(
        self,
        p1: BoundPredicate,
        p2: BoundPredicate,
    ) -> bool:
        """Check if two predicates are equivalent (simplified)."""
        return self._predicate_fingerprint(p1) == self._predicate_fingerprint(p2)

    def _relax_predicates(
        self,
        pred_sets: list[list[BoundPredicate]],
        common_variables: set[str],
    ) -> tuple[list[BoundPredicate], list[str]]:
        """Find the weakest predicates implied by all input predicate sets.

        For GCS, we want predicates that are implied by ALL input queries.
        This means finding the "relaxed" version:
        - Range: x > 10 and x > 5 → x > 5 (weaker bound)
        - Interval: x BETWEEN 1 AND 10, x BETWEEN 5 AND 15 → x BETWEEN 1 AND 15
        - Equality: x = 'a' and x = 'b' → (dropped, incompatible)

        Args:
            pred_sets: List of predicate sets from each query
            common_variables: Set of valid variable names

        Returns:
            Tuple of (relaxed predicates, dropped predicate fingerprints)
        """

        if not pred_sets:
            return [], []

        relaxed: list[BoundPredicate] = []
        dropped: list[str] = []

        # Group predicates by the column they constrain
        # Key: column fingerprint, Value: list of (predicate, query_index)
        column_predicates: dict[str, list[tuple[BoundPredicate, int]]] = {}

        for query_idx, preds in enumerate(pred_sets):
            for pred in preds:
                col_key = self._get_predicate_column_key(pred)
                if col_key:
                    if col_key not in column_predicates:
                        column_predicates[col_key] = []
                    column_predicates[col_key].append((pred, query_idx))

        n_queries = len(pred_sets)

        for _col_key, pred_list in column_predicates.items():
            # Check if this column is constrained in all queries
            query_indices = {idx for _, idx in pred_list}

            if len(query_indices) < n_queries:
                # Not present in all queries - cannot include in GCS
                for pred, _ in pred_list:
                    dropped.append(self._predicate_fingerprint(pred))
                continue

            # Try to relax the predicates
            predicates = [p for p, _ in pred_list]
            relaxed_pred = self._try_relax_column_predicates(predicates)

            if relaxed_pred is not None:
                if self._predicate_uses_only(relaxed_pred, common_variables):
                    relaxed.append(relaxed_pred)
                else:
                    dropped.append(self._predicate_fingerprint(relaxed_pred))
            else:
                # Incompatible predicates
                for pred in predicates:
                    dropped.append(self._predicate_fingerprint(pred))

        return relaxed, dropped

    def _get_predicate_column_key(self, pred: BoundPredicate) -> str | None:
        """Get a key identifying the column(s) constrained by a predicate."""
        from alma_algebrakit.bound.types import (
            BoundBetween,
            BoundComparison,
            BoundIn,
        )

        if isinstance(pred, BoundComparison):
            # For comparisons, use the column side (not the literal)
            if isinstance(pred.left, AttributeRef):
                return pred.left.qualified_id()
            if isinstance(pred.right, AttributeRef):
                return pred.right.qualified_id()

        if isinstance(pred, BoundBetween):
            if isinstance(pred.expression, AttributeRef):
                return pred.expression.qualified_id()

        if isinstance(pred, BoundIn):
            if isinstance(pred.expression, AttributeRef):
                return pred.expression.qualified_id()

        return None

    def _try_relax_column_predicates(
        self,
        predicates: list[BoundPredicate],
    ) -> BoundPredicate | None:
        """Try to relax predicates on the same column to a weaker form.

        Returns the relaxed predicate, or None if predicates are incompatible.
        """
        from alma_algebrakit.bound.types import (
            BoundBetween,
            BoundComparison,
        )

        if not predicates:
            return None

        # If all predicates are identical, return one of them
        fps = [self._predicate_fingerprint(p) for p in predicates]
        if len(set(fps)) == 1:
            return predicates[0]

        # Try to relax comparison predicates
        comparisons = [p for p in predicates if isinstance(p, BoundComparison)]
        if len(comparisons) == len(predicates):
            return self._relax_comparisons(comparisons)

        # Try to relax BETWEEN predicates
        betweens = [p for p in predicates if isinstance(p, BoundBetween)]
        if len(betweens) == len(predicates):
            return self._relax_betweens(betweens)

        # Mixed types - try to convert comparisons to ranges
        # For now, fall back to requiring exact match
        return None

    def _relax_comparisons(
        self,
        comparisons: list[BoundPredicate],
    ) -> BoundPredicate | None:
        """Relax comparison predicates to the weakest common bound.

        For the same column:
        - x > 10 and x > 5 → x > 5 (min of lower bounds)
        - x < 10 and x < 5 → x < 10 (max of upper bounds)
        - x = 'a' and x = 'b' → None (incompatible)
        """
        from alma_algebrakit.bound.types import BoundComparison

        if not comparisons:
            return None

        first = comparisons[0]
        if not isinstance(first, BoundComparison):
            return None

        # Group by operator type
        operators = {c.operator for c in comparisons if isinstance(c, BoundComparison)}

        # If all are equality with same value
        if operators == {"="}:
            values = set()
            for c in comparisons:
                if isinstance(c, BoundComparison) and isinstance(c.right, BoundLiteral):
                    values.add(c.right.value)
                elif isinstance(c, BoundComparison) and isinstance(c.left, BoundLiteral):
                    values.add(c.left.value)
            if len(values) == 1:
                return first  # All same equality
            return None  # Different equalities are incompatible

        # Handle range predicates (>, >=, <, <=)
        lower_bounds: list[tuple[float, bool]] = []  # (value, is_inclusive)
        upper_bounds: list[tuple[float, bool]] = []

        for c in comparisons:
            if not isinstance(c, BoundComparison):
                continue

            # Determine which side is the column and which is the literal
            if isinstance(c.right, BoundLiteral) and isinstance(c.right.value, (int, float)):
                lit_val = float(c.right.value)
                op = c.operator
            elif isinstance(c.left, BoundLiteral) and isinstance(c.left.value, (int, float)):
                lit_val = float(c.left.value)
                # Flip operator for reversed comparison
                op = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "=", "!=": "!="}.get(
                    c.operator, c.operator
                )
            else:
                # Non-numeric comparison
                return None

            if op in (">", ">="):
                lower_bounds.append((lit_val, op == ">="))
            elif op in ("<", "<="):
                upper_bounds.append((lit_val, op == "<="))
            elif op == "=":
                lower_bounds.append((lit_val, True))
                upper_bounds.append((lit_val, True))

        # Relax: take min of lower bounds, max of upper bounds
        # Get upstream_columns if available, otherwise empty list
        upstream_cols = getattr(first, "upstream_columns", [])

        if lower_bounds and not upper_bounds:
            # All are lower bound constraints - take the minimum (weakest)
            min_val, is_incl = min(lower_bounds, key=lambda x: (x[0], not x[1]))
            op = ">=" if is_incl else ">"
            return BoundComparison(
                left=first.left if isinstance(first.left, AttributeRef) else first.right,
                operator=op,
                right=BoundLiteral(value=min_val if min_val == int(min_val) else min_val),
                upstream_columns=upstream_cols,
            )

        if upper_bounds and not lower_bounds:
            # All are upper bound constraints - take the maximum (weakest)
            max_val, is_incl = max(upper_bounds, key=lambda x: (x[0], x[1]))
            op = "<=" if is_incl else "<"
            return BoundComparison(
                left=first.left if isinstance(first.left, AttributeRef) else first.right,
                operator=op,
                right=BoundLiteral(value=max_val if max_val == int(max_val) else max_val),
                upstream_columns=upstream_cols,
            )

        if lower_bounds and upper_bounds:
            # Mixed bounds - create a BETWEEN if compatible
            min_lower = min(lb[0] for lb in lower_bounds)
            max_upper = max(ub[0] for ub in upper_bounds)
            if min_lower <= max_upper:
                return BoundBetween(
                    expression=first.left if isinstance(first.left, AttributeRef) else first.right,
                    low=BoundLiteral(value=min_lower if min_lower == int(min_lower) else min_lower),
                    high=BoundLiteral(
                        value=max_upper if max_upper == int(max_upper) else max_upper
                    ),
                    upstream_columns=upstream_cols,
                )

        return None

    def _relax_betweens(
        self,
        betweens: list[BoundPredicate],
    ) -> BoundPredicate | None:
        """Relax BETWEEN predicates to the bounding interval.

        x BETWEEN 1 AND 10 and x BETWEEN 5 AND 15 → x BETWEEN 1 AND 15
        """
        from alma_algebrakit.bound.types import BoundBetween

        if not betweens:
            return None

        first = betweens[0]
        if not isinstance(first, BoundBetween):
            return None

        min_low = None
        max_high = None

        for b in betweens:
            if not isinstance(b, BoundBetween):
                return None

            if isinstance(b.low, BoundLiteral) and isinstance(b.low.value, (int, float)):
                low_val = float(b.low.value)
                if min_low is None or low_val < min_low:
                    min_low = low_val

            if isinstance(b.high, BoundLiteral) and isinstance(b.high.value, (int, float)):
                high_val = float(b.high.value)
                if max_high is None or high_val > max_high:
                    max_high = high_val

        if min_low is not None and max_high is not None:
            upstream_cols = getattr(first, "upstream_columns", [])
            return BoundBetween(
                expression=first.expression,
                low=BoundLiteral(value=min_low if min_low == int(min_low) else min_low),
                high=BoundLiteral(value=max_high if max_high == int(max_high) else max_high),
                upstream_columns=upstream_cols,
            )

        return None

    def _predicate_uses_only(
        self,
        pred: BoundPredicate,
        allowed_vars: set[str],
    ) -> bool:
        """Check if predicate only references allowed variables."""
        # Handle predicates without upstream_columns (e.g., AtomicPredicate)
        if not hasattr(pred, "upstream_columns"):
            return True  # Can't check, assume OK
        for col in pred.upstream_columns:
            var_name = f"{col.table_alias or col.table_id}_{col.sql_name}"
            if var_name not in allowed_vars:
                # Also try qualified ID format
                if col.qualified_id() not in allowed_vars:
                    return False
        return True

    def suggest_contract_from_queries(
        self,
        queries: list[CQRepresentation],
        prefer_restrictive: bool = True,
    ) -> GeneralizationResult:
        """Suggest a contract CQ from multiple queries.

        This combines LUB and GLB approaches to find an optimal contract:
        - Uses GLB relations (intersection) to avoid missing data
        - Uses common predicates that appear in most queries
        - Exposes head variables that are commonly projected

        Args:
            queries: List of CQ representations
            prefer_restrictive: If True, prefer more restrictive (LUB-like) contracts;
                              if False, prefer more permissive (GLB-like) contracts

        Returns:
            GeneralizationResult with suggested contract
        """
        if prefer_restrictive:
            # Start with GLB structure, add common predicates
            glb = self.compute_glb(queries)
            if not glb.success or glb.cq is None:
                # Fall back to common structure
                return self.compute_common_structure(queries)
            return glb
        else:
            # Use common structure approach
            return self.compute_common_structure(queries)
