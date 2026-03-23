"""Relational algebra expression normalizer.

Applies algebraic transformations to canonicalize RA expressions for comparison
and pattern matching.

Core RA equivalences for inner-join SPJ:

Selection:
- σ_φ1(σ_φ2(E)) ≡ σ_φ1∧φ2(E)
- σ_φ(E_1 ⋈ E_2) ≡ (σ_φ1(E_1)) ⋈ (σ_φ2(E_2)) if φ splits

Projection:
- π_A(π_B(E)) ≡ π_A(E) if A ⊆ B
- π_A(σ_φ(E)) ≡ σ_φ(π_A(E)) if φ references only A

Join reordering (inner joins):
- E_1 ⋈ E_2 ≡ E_2 ⋈ E_1 (commutativity)
- (E_1 ⋈ E_2) ⋈ E_3 ≡ E_1 ⋈ (E_2 ⋈ E_3) (associativity)
"""

from __future__ import annotations

from dataclasses import dataclass

from alma_algebrakit.models.algebra import (
    Aggregation,
    AtomicPredicate,
    CompoundPredicate,
    Difference,
    Expression,
    Intersect,
    Join,
    JoinType,
    Limit,
    LogicalOp,
    Predicate,
    Projection,
    RAExpression,
    Relation,
    RelationRef,
    Selection,
    Sort,
    Union,
    WithExpression,
)

# =============================================================================
# Alias-Safe Join Edge for Rewriting
# =============================================================================


@dataclass(frozen=True)
class JoinEdge:
    """A join edge with full metadata for alias-safe rewriting.

    Unlike the simple (left_table, right_table, condition) tuple from
    extract_join_graph(), this captures complete information needed for
    correct partial-coverage rewriting:

    - Relation instances (not just table names) for alias safety
    - Join type for outer join handling
    - Full ON predicate plus decomposed atoms for multi-column joins
    - Tree path for reconstructing join order

    Attributes:
        join_type: Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
        left_rels: Frozenset of RelationRef instances in the left subtree
        right_rels: Frozenset of RelationRef instances in the right subtree
        on_predicate: Full ON predicate (may be compound)
        on_atoms: Tuple of atomic predicates if ON is a conjunction
        tree_path: Path in join tree for ordering (e.g., "", "L", "R", "L-R")
    """

    join_type: JoinType
    left_rels: frozenset[RelationRef]
    right_rels: frozenset[RelationRef]
    on_predicate: Predicate | None
    on_atoms: tuple[AtomicPredicate, ...] = ()
    tree_path: str = ""

    def crosses_partition(
        self,
        covered: set[RelationRef],
        remaining: set[RelationRef],
    ) -> bool:
        """Check if this edge crosses the covered/remaining boundary.

        A join edge crosses the partition if one side has covered relations
        and the other side has remaining relations.

        Args:
            covered: Set of relation instances covered by a contract
            remaining: Set of relation instances not covered

        Returns:
            True if this is a boundary join
        """
        left_has_covered = bool(self.left_rels & covered)
        left_has_remaining = bool(self.left_rels & remaining)
        right_has_covered = bool(self.right_rels & covered)
        right_has_remaining = bool(self.right_rels & remaining)

        # Boundary if one side is covered-only and other is remaining-only,
        # or if mixed (which would require more complex handling)
        return (left_has_covered and right_has_remaining) or (
            left_has_remaining and right_has_covered
        )

    def is_remaining_to_remaining(
        self,
        covered: set[RelationRef],
        remaining: set[RelationRef],
    ) -> bool:
        """Check if this edge connects only remaining relations.

        These joins must be preserved exactly in the rewritten query.
        """
        left_only_remaining = self.left_rels <= remaining and not (self.left_rels & covered)
        right_only_remaining = self.right_rels <= remaining and not (self.right_rels & covered)
        return left_only_remaining and right_only_remaining

    def is_covered_to_covered(
        self,
        covered: set[RelationRef],
        remaining: set[RelationRef],
    ) -> bool:
        """Check if this edge connects only covered relations.

        These joins are absorbed into the contract view.
        """
        left_only_covered = self.left_rels <= covered and not (self.left_rels & remaining)
        right_only_covered = self.right_rels <= covered and not (self.right_rels & remaining)
        return left_only_covered and right_only_covered

    def get_covered_side_rels(
        self,
        covered: set[RelationRef],
    ) -> frozenset[RelationRef]:
        """Get the relation instances on the covered side of a boundary join."""
        left_covered = self.left_rels & covered
        right_covered = self.right_rels & covered
        return frozenset(left_covered | right_covered)

    def get_remaining_side_rels(
        self,
        remaining: set[RelationRef],
    ) -> frozenset[RelationRef]:
        """Get the relation instances on the remaining side of a boundary join."""
        left_remaining = self.left_rels & remaining
        right_remaining = self.right_rels & remaining
        return frozenset(left_remaining | right_remaining)

    def columns_from_covered_side(
        self,
        covered: set[RelationRef],
    ) -> set[str]:
        """Get column references from the covered side of the join condition.

        Returns qualified column names (alias.column) that are used in the
        ON predicate from relations on the covered side.
        """
        if not self.on_predicate:
            return set()

        covered_aliases = {r.effective_name() for r in covered}
        columns: set[str] = set()

        for col_ref in self.on_predicate.referenced_columns():
            if "." in col_ref:
                alias, _ = col_ref.split(".", 1)
                if alias in covered_aliases:
                    columns.add(col_ref)

        return columns


def extract_detailed_join_graph(expr: RAExpression) -> list[JoinEdge]:
    """Extract a detailed join graph with full metadata for rewriting.

    Unlike the simpler extract_join_graph() which returns tuples, this
    function extracts JoinEdge objects with:
    - Alias-safe relation instances (RelationRef)
    - Join types for outer join handling
    - Decomposed ON predicates for multi-column joins
    - Tree paths for correct reconstruction

    Args:
        expr: An RA expression

    Returns:
        List of JoinEdge objects representing the join structure
    """
    edges: list[JoinEdge] = []
    _extract_detailed_joins_recursive(expr, edges, "")
    return edges


def _extract_detailed_joins_recursive(
    expr: RAExpression,
    edges: list[JoinEdge],
    path: str,
) -> None:
    """Recursively extract detailed join edges."""
    if isinstance(expr, Join):
        # Get relation instances from each subtree
        left_rels = frozenset(expr.left.relation_instances())
        right_rels = frozenset(expr.right.relation_instances())

        # Decompose ON predicate into atoms if it's a conjunction
        on_atoms: tuple[AtomicPredicate, ...] = ()
        if expr.condition:
            atoms = expr.condition.to_cnf_atoms()
            on_atoms = tuple(atoms)

        edge = JoinEdge(
            join_type=expr.join_type,
            left_rels=left_rels,
            right_rels=right_rels,
            on_predicate=expr.condition,
            on_atoms=on_atoms,
            tree_path=path,
        )
        edges.append(edge)

        # Recurse into children with updated paths
        _extract_detailed_joins_recursive(expr.left, edges, f"{path}L" if path else "L")
        _extract_detailed_joins_recursive(expr.right, edges, f"{path}R" if path else "R")

    elif isinstance(expr, (Selection, Projection, Aggregation, Sort, Limit)):
        _extract_detailed_joins_recursive(expr.input, edges, path)

    elif isinstance(expr, (Union, Difference, Intersect)):
        # For set operations, extract from both branches
        _extract_detailed_joins_recursive(expr.left, edges, f"{path}L" if path else "L")
        _extract_detailed_joins_recursive(expr.right, edges, f"{path}R" if path else "R")

    elif isinstance(expr, WithExpression):
        # Extract from CTEs and main query
        for i, cte in enumerate(expr.ctes):
            _extract_detailed_joins_recursive(
                cte.query, edges, f"{path}CTE{i}" if path else f"CTE{i}"
            )
        _extract_detailed_joins_recursive(expr.main_query, edges, f"{path}M" if path else "M")


class RANormalizer:
    """Normalizer for relational algebra expressions."""

    def __init__(
        self,
        merge_selections: bool = True,
        flatten_joins: bool = True,
        canonicalize_join_order: bool = True,
    ) -> None:
        """Initialize the normalizer.

        Args:
            merge_selections: Merge cascading selections into one
            flatten_joins: Flatten nested joins (for inner joins only)
            canonicalize_join_order: Sort joins by table name for canonical form
        """
        self.merge_selections = merge_selections
        self.flatten_joins = flatten_joins
        self.canonicalize_join_order = canonicalize_join_order

    def normalize(self, expr: RAExpression) -> RAExpression:
        """Apply normalization transformations to an RA expression.

        Args:
            expr: The RA expression to normalize

        Returns:
            Normalized RA expression
        """
        result = expr

        if self.merge_selections:
            result = self._merge_cascading_selections(result)

        if self.flatten_joins:
            result = self._flatten_inner_joins(result)

        if self.canonicalize_join_order:
            result = self._canonicalize_join_order(result)

        return result

    def _merge_cascading_selections(self, expr: RAExpression) -> RAExpression:
        """Merge cascading selections: σ_φ1(σ_φ2(E)) → σ_φ1∧φ2(E)."""
        if isinstance(expr, Selection):
            # Recursively normalize input first
            normalized_input = self._merge_cascading_selections(expr.input)

            # If input is also a selection, merge them
            if isinstance(normalized_input, Selection):
                merged_pred = CompoundPredicate(
                    op=LogicalOp.AND,
                    operands=[expr.predicate, normalized_input.predicate],
                )
                return Selection(
                    predicate=merged_pred,
                    input=normalized_input.input,
                )

            return Selection(predicate=expr.predicate, input=normalized_input)

        if isinstance(expr, Projection):
            return Projection(
                columns=expr.columns,
                input=self._merge_cascading_selections(expr.input),
                distinct=expr.distinct,
            )

        if isinstance(expr, Join):
            return Join(
                left=self._merge_cascading_selections(expr.left),
                right=self._merge_cascading_selections(expr.right),
                join_type=expr.join_type,
                condition=expr.condition,
            )

        if isinstance(expr, Aggregation):
            return Aggregation(
                group_by=expr.group_by,
                aggregates=expr.aggregates,
                input=self._merge_cascading_selections(expr.input),
                having=expr.having,
            )

        if isinstance(expr, Union):
            return Union(
                left=self._merge_cascading_selections(expr.left),
                right=self._merge_cascading_selections(expr.right),
                all=expr.all,
            )

        if isinstance(expr, Difference):
            return Difference(
                left=self._merge_cascading_selections(expr.left),
                right=self._merge_cascading_selections(expr.right),
            )

        if isinstance(expr, Intersect):
            return Intersect(
                left=self._merge_cascading_selections(expr.left),
                right=self._merge_cascading_selections(expr.right),
                all=expr.all,
            )

        if isinstance(expr, Sort):
            return Sort(
                input=self._merge_cascading_selections(expr.input),
                order_by=expr.order_by,
            )

        if isinstance(expr, Limit):
            return Limit(
                input=self._merge_cascading_selections(expr.input),
                limit=expr.limit,
                offset=expr.offset,
            )

        if isinstance(expr, WithExpression):
            # Normalize each CTE definition and the main query
            from alma_algebrakit.models.algebra import CTEDefinition

            normalized_ctes = []
            for cte in expr.ctes:
                normalized_ctes.append(
                    CTEDefinition(
                        name=cte.name,
                        columns=cte.columns,
                        query=self._merge_cascading_selections(cte.query),
                        recursive=cte.recursive,
                    )
                )
            return WithExpression(
                ctes=normalized_ctes,
                main_query=self._merge_cascading_selections(expr.main_query),
            )

        # Relation or other base case
        return expr

    def _flatten_inner_joins(self, expr: RAExpression) -> RAExpression:
        """Flatten nested inner joins into a single multi-way join representation.

        Note: This is only safe for inner equi-joins. Outer joins cannot be reordered.
        """
        if isinstance(expr, Join):
            # Recursively flatten children first
            left = self._flatten_inner_joins(expr.left)
            right = self._flatten_inner_joins(expr.right)

            # Only flatten inner joins
            if expr.join_type != JoinType.INNER:
                return Join(
                    left=left,
                    right=right,
                    join_type=expr.join_type,
                    condition=expr.condition,
                )

            # For inner joins, we could flatten into a list representation
            # For now, just return with flattened children
            return Join(
                left=left,
                right=right,
                join_type=expr.join_type,
                condition=expr.condition,
            )

        if isinstance(expr, Selection):
            return Selection(
                predicate=expr.predicate,
                input=self._flatten_inner_joins(expr.input),
            )

        if isinstance(expr, Projection):
            return Projection(
                columns=expr.columns,
                input=self._flatten_inner_joins(expr.input),
                distinct=expr.distinct,
            )

        if isinstance(expr, Aggregation):
            return Aggregation(
                group_by=expr.group_by,
                aggregates=expr.aggregates,
                input=self._flatten_inner_joins(expr.input),
                having=expr.having,
            )

        if isinstance(expr, Union):
            return Union(
                left=self._flatten_inner_joins(expr.left),
                right=self._flatten_inner_joins(expr.right),
                all=expr.all,
            )

        if isinstance(expr, Difference):
            return Difference(
                left=self._flatten_inner_joins(expr.left),
                right=self._flatten_inner_joins(expr.right),
            )

        if isinstance(expr, Intersect):
            return Intersect(
                left=self._flatten_inner_joins(expr.left),
                right=self._flatten_inner_joins(expr.right),
                all=expr.all,
            )

        if isinstance(expr, Sort):
            return Sort(
                input=self._flatten_inner_joins(expr.input),
                order_by=expr.order_by,
            )

        if isinstance(expr, Limit):
            return Limit(
                input=self._flatten_inner_joins(expr.input),
                limit=expr.limit,
                offset=expr.offset,
            )

        if isinstance(expr, WithExpression):
            from alma_algebrakit.models.algebra import CTEDefinition

            normalized_ctes = []
            for cte in expr.ctes:
                normalized_ctes.append(
                    CTEDefinition(
                        name=cte.name,
                        columns=cte.columns,
                        query=self._flatten_inner_joins(cte.query),
                        recursive=cte.recursive,
                    )
                )
            return WithExpression(
                ctes=normalized_ctes,
                main_query=self._flatten_inner_joins(expr.main_query),
            )

        return expr

    def _canonicalize_join_order(self, expr: RAExpression) -> RAExpression:
        """Canonicalize join order by sorting tables alphabetically.

        This ensures the same query written with different join orders produces
        the same canonical form (for inner equi-joins only).
        """
        if isinstance(expr, Join):
            left = self._canonicalize_join_order(expr.left)
            right = self._canonicalize_join_order(expr.right)

            # Only reorder inner equi-joins (safe for reordering)
            # Non-equi-joins (OR/NOT predicates, non-equality conditions) are NOT safe
            if expr.is_inner_equijoin():
                # Get the "name" of each side for ordering
                left_name = self._get_ordering_key(left)
                right_name = self._get_ordering_key(right)

                if left_name > right_name:
                    # Swap left and right
                    return Join(
                        left=right,
                        right=left,
                        join_type=expr.join_type,
                        condition=expr.condition,
                    )

            return Join(
                left=left,
                right=right,
                join_type=expr.join_type,
                condition=expr.condition,
            )

        if isinstance(expr, Selection):
            return Selection(
                predicate=expr.predicate,
                input=self._canonicalize_join_order(expr.input),
            )

        if isinstance(expr, Projection):
            return Projection(
                columns=expr.columns,
                input=self._canonicalize_join_order(expr.input),
                distinct=expr.distinct,
            )

        if isinstance(expr, Aggregation):
            return Aggregation(
                group_by=expr.group_by,
                aggregates=expr.aggregates,
                input=self._canonicalize_join_order(expr.input),
                having=expr.having,
            )

        if isinstance(expr, Union):
            return Union(
                left=self._canonicalize_join_order(expr.left),
                right=self._canonicalize_join_order(expr.right),
                all=expr.all,
            )

        if isinstance(expr, Difference):
            return Difference(
                left=self._canonicalize_join_order(expr.left),
                right=self._canonicalize_join_order(expr.right),
            )

        if isinstance(expr, Intersect):
            return Intersect(
                left=self._canonicalize_join_order(expr.left),
                right=self._canonicalize_join_order(expr.right),
                all=expr.all,
            )

        if isinstance(expr, Sort):
            return Sort(
                input=self._canonicalize_join_order(expr.input),
                order_by=expr.order_by,
            )

        if isinstance(expr, Limit):
            return Limit(
                input=self._canonicalize_join_order(expr.input),
                limit=expr.limit,
                offset=expr.offset,
            )

        if isinstance(expr, WithExpression):
            from alma_algebrakit.models.algebra import CTEDefinition

            normalized_ctes = []
            for cte in expr.ctes:
                normalized_ctes.append(
                    CTEDefinition(
                        name=cte.name,
                        columns=cte.columns,
                        query=self._canonicalize_join_order(cte.query),
                        recursive=cte.recursive,
                    )
                )
            return WithExpression(
                ctes=normalized_ctes,
                main_query=self._canonicalize_join_order(expr.main_query),
            )

        return expr

    def _get_ordering_key(self, expr: RAExpression) -> str:
        """Get a key for ordering expressions (primarily by table names)."""
        if isinstance(expr, Relation):
            return expr.alias or expr.name

        if isinstance(expr, Join):
            # For joins, concatenate all table names
            tables = sorted(expr.referenced_tables())
            return ",".join(tables)

        if isinstance(expr, (Selection, Projection, Aggregation, Sort, Limit)):
            return self._get_ordering_key(expr.input)

        if isinstance(expr, (Union, Difference, Intersect)):
            # For set operations, use tables from both sides
            tables = sorted(expr.referenced_tables())
            return ",".join(tables)

        if isinstance(expr, WithExpression):
            return self._get_ordering_key(expr.main_query)

        return ""

    def extract_join_graph(self, expr: RAExpression) -> list[tuple[str, str, Predicate | None]]:
        """Extract the join graph as a list of (left_table, right_table, condition) tuples."""
        edges: list[tuple[str, str, Predicate | None]] = []
        self._extract_joins_recursive(expr, edges)
        return edges

    def _extract_joins_recursive(
        self,
        expr: RAExpression,
        edges: list[tuple[str, str, Predicate | None]],
    ) -> None:
        """Recursively extract join edges."""
        if isinstance(expr, Join):
            # Get representative table names
            left_tables = expr.left.referenced_tables()
            right_tables = expr.right.referenced_tables()

            # Use first table as representative
            left_rep = sorted(left_tables)[0] if left_tables else ""
            right_rep = sorted(right_tables)[0] if right_tables else ""

            edges.append((left_rep, right_rep, expr.condition))

            # Recurse into children
            self._extract_joins_recursive(expr.left, edges)
            self._extract_joins_recursive(expr.right, edges)

        elif isinstance(expr, (Selection, Projection, Aggregation, Sort, Limit)):
            self._extract_joins_recursive(expr.input, edges)

        elif isinstance(expr, (Union, Difference, Intersect)):
            self._extract_joins_recursive(expr.left, edges)
            self._extract_joins_recursive(expr.right, edges)

        elif isinstance(expr, WithExpression):
            for cte in expr.ctes:
                self._extract_joins_recursive(cte.query, edges)
            self._extract_joins_recursive(expr.main_query, edges)

    def extract_all_predicates(self, expr: RAExpression) -> list[Predicate]:
        """Extract ALL predicates including JOIN ON conditions.

        This includes Selection (WHERE), HAVING, and JOIN ON predicates.

        For only Selection/HAVING predicates (excluding JOIN ON conditions),
        use ``algebrakit.rewriting.predicates.extract_selection_predicates()``
        instead. That function is appropriate for partial-coverage rewriting
        where JOIN conditions are handled separately.
        """
        predicates: list[Predicate] = []
        self._extract_predicates_recursive(expr, predicates)
        return predicates

    def extract_predicates(self, expr: RAExpression) -> list[Predicate]:
        """Extract ALL predicates including JOIN ON conditions.

        .. deprecated:: Use ``extract_all_predicates()`` instead for clarity.
            This method is kept for backward compatibility.
        """
        return self.extract_all_predicates(expr)

    def _extract_predicates_recursive(
        self,
        expr: RAExpression,
        predicates: list[Predicate],
    ) -> None:
        """Recursively extract predicates."""
        if isinstance(expr, Selection):
            predicates.append(expr.predicate)
            self._extract_predicates_recursive(expr.input, predicates)

        elif isinstance(expr, Projection):
            self._extract_predicates_recursive(expr.input, predicates)

        elif isinstance(expr, Join):
            if expr.condition:
                predicates.append(expr.condition)
            self._extract_predicates_recursive(expr.left, predicates)
            self._extract_predicates_recursive(expr.right, predicates)

        elif isinstance(expr, Aggregation):
            if expr.having:
                predicates.append(expr.having)
            self._extract_predicates_recursive(expr.input, predicates)

        elif isinstance(expr, (Sort, Limit)):
            self._extract_predicates_recursive(expr.input, predicates)

        elif isinstance(expr, (Union, Difference, Intersect)):
            self._extract_predicates_recursive(expr.left, predicates)
            self._extract_predicates_recursive(expr.right, predicates)

        elif isinstance(expr, WithExpression):
            for cte in expr.ctes:
                self._extract_predicates_recursive(cte.query, predicates)
            self._extract_predicates_recursive(expr.main_query, predicates)

    def predicate_implies(self, p1: Predicate, p2: Predicate) -> bool:
        """Check if predicate p1 implies p2 (p1 ⇒ p2).

        This is the RA-level quick check operating on unbound Predicate types.
        It handles common syntactic cases:
        - Same predicate (fingerprint equality)
        - p1 is a conjunction containing p2
        - p2 is a disjunction containing p1

        For full implication checking with type-aware reasoning (range containment,
        linear arithmetic, SMT), use ``proof.PredicateImplicationChecker`` which
        operates on bound predicates (``BoundPredicate``).
        """
        # Same predicate
        if p1.fingerprint() == p2.fingerprint():
            return True

        # p1 is a conjunction containing p2
        if isinstance(p1, CompoundPredicate) and p1.op == LogicalOp.AND:
            for operand in p1.operands:
                if operand.fingerprint() == p2.fingerprint():
                    return True
                if self.predicate_implies(operand, p2):
                    return True

        # p2 is a disjunction - p1 implies p2 if p1 implies any disjunct
        if isinstance(p2, CompoundPredicate) and p2.op == LogicalOp.OR:
            for operand in p2.operands:
                if self.predicate_implies(p1, operand):
                    return True

        # For atomic predicates, check range implications
        if isinstance(p1, AtomicPredicate) and isinstance(p2, AtomicPredicate):
            return self._atomic_implies(p1, p2)

        return False

    def _atomic_implies(self, p1: AtomicPredicate, p2: AtomicPredicate) -> bool:
        """Check if atomic predicate p1 implies p2."""
        # Same predicate
        if p1.fingerprint() == p2.fingerprint():
            return True

        # Must be on same column
        if p1.left.fingerprint() != p2.left.fingerprint():
            return False

        # Range implications - simplified
        # x > 5 implies x > 3, x >= 4, x != 3
        # This is a simplified check; a real implementation would be more thorough

        return False


# =============================================================================
# Top-Level Operator Extraction (Peel and Re-wrap Pattern)
# =============================================================================


@dataclass
class TopLevelOperators:
    """Extracted top-level operators from an RA tree.

    The parser creates RA trees with this nesting order (outermost first):
    - Limit (LIMIT/OFFSET)
    - Sort (ORDER BY)
    - Aggregation (GROUP BY)
    - Projection (SELECT list)
    - Selection (WHERE)
    - Join / Relation (FROM)

    This class captures the "top-level" operators (Limit, Sort, Aggregation)
    that wrap the core query. This enables clean query rewriting by:
    1. Peeling off top-level operators
    2. Rewriting the core query (replacing tables with views)
    3. Re-wrapping with the original operators (with column rewrites)

    Attributes:
        sort: The Sort operator if present (ORDER BY clause)
        limit: The Limit operator if present (LIMIT/OFFSET clause)
        aggregation: The Aggregation operator if present (GROUP BY clause)
        core: The inner expression without top-level operators
    """

    sort: Sort | None = None
    limit: Limit | None = None
    aggregation: Aggregation | None = None
    core: RAExpression | None = None


def extract_top_level_operators(expr: RAExpression) -> TopLevelOperators:
    """Peel off Sort, Limit, and Aggregation from the outermost layers of an RA tree.

    This function extracts top-level operators to enable clean query rewriting.
    The operators are peeled in order: Limit (outermost), Sort, Aggregation.

    Args:
        expr: The RA expression to peel operators from

    Returns:
        TopLevelOperators containing the extracted operators and the core expression

    Example:
        For a query "SELECT * FROM t ORDER BY x LIMIT 10":
        - RA tree: Limit(Sort(Projection(Relation)))
        - Result: TopLevelOperators(limit=Limit, sort=Sort, core=Projection)
    """
    result = TopLevelOperators()
    current = expr

    # Peel Limit (outermost - LIMIT/OFFSET applies last)
    if isinstance(current, Limit):
        result.limit = current
        current = current.input

    # Peel Sort (ORDER BY applies before LIMIT)
    if isinstance(current, Sort):
        result.sort = current
        current = current.input

    # Peel Aggregation (GROUP BY - but only if it's at the top)
    # Note: Aggregation replaces Projection in the tree when present
    if isinstance(current, Aggregation):
        result.aggregation = current
        current = current.input

    result.core = current
    return result


def wrap_with_operators(
    core: RAExpression,
    ops: TopLevelOperators,
    column_rewrite_map: dict[str, str] | None = None,
) -> RAExpression:
    """Re-wrap a core expression with top-level operators.

    This function re-wraps a (possibly rewritten) core expression with the
    original top-level operators, applying column rewrites as needed.

    The wrapping order is: Aggregation (innermost), Sort, Limit (outermost).
    This matches the SQL evaluation order: GROUP BY → ORDER BY → LIMIT.

    Args:
        core: The core RA expression to wrap
        ops: The extracted top-level operators from extract_top_level_operators()
        column_rewrite_map: Optional mapping of old column references to new ones
            (e.g., {"orders.id": "v_orders.id"} when replacing tables with views)

    Returns:
        The wrapped RA expression with all top-level operators applied

    Example:
        wrap_with_operators(
            core=Projection(Relation("v_orders")),
            ops=TopLevelOperators(sort=Sort(...), limit=Limit(...)),
            column_rewrite_map={"orders.id": "v_orders.id"}
        )
        → Limit(Sort(Projection(Relation("v_orders"))))
    """
    from alma_algebrakit.models.algebra import SortSpec

    result = core

    # Re-wrap Aggregation (with column rewrites)
    if ops.aggregation:
        result = Aggregation(
            group_by=_rewrite_expressions(ops.aggregation.group_by, column_rewrite_map),
            aggregates=ops.aggregation.aggregates,
            input=result,
            having=_rewrite_predicate(ops.aggregation.having, column_rewrite_map),
        )

    # Re-wrap Sort (with column rewrites for ORDER BY expressions)
    if ops.sort:
        result = Sort(
            order_by=[
                SortSpec(
                    expression=_rewrite_expression(spec.expression, column_rewrite_map),
                    direction=spec.direction,
                    nulls=spec.nulls,
                )
                for spec in ops.sort.order_by
            ],
            input=result,
        )

    # Re-wrap Limit (no column rewrites needed - just values)
    if ops.limit:
        result = Limit(
            limit=ops.limit.limit,
            offset=ops.limit.offset,
            input=result,
        )

    return result


def _rewrite_expression(
    expr: Expression,
    column_rewrite_map: dict[str, str] | None,
) -> Expression:
    """Rewrite column references in an expression using the rewrite map.

    Args:
        expr: The expression to rewrite
        column_rewrite_map: Mapping of old column refs to new ones

    Returns:
        The expression with column references rewritten
    """
    from alma_algebrakit.models.algebra import ColumnRef

    if column_rewrite_map is None:
        return expr

    # For ColumnRef, check if it needs rewriting
    if isinstance(expr, ColumnRef):
        fingerprint = expr.fingerprint()
        if fingerprint in column_rewrite_map:
            new_ref = column_rewrite_map[fingerprint]
            # Parse the new reference (format: "table.column" or "column")
            parts = new_ref.split(".")
            if len(parts) == 2:
                return ColumnRef(table=parts[0], column=parts[1])
            else:
                return ColumnRef(column=parts[0])
        # Also try without table prefix
        if expr.column in column_rewrite_map:
            new_ref = column_rewrite_map[expr.column]
            parts = new_ref.split(".")
            if len(parts) == 2:
                return ColumnRef(table=parts[0], column=parts[1])
            else:
                return ColumnRef(column=parts[0])

    # For other expression types, we'd need to recursively rewrite
    # For now, return as-is (most ORDER BY/GROUP BY use simple column refs)
    return expr


def _rewrite_expressions(
    exprs: list[Expression],
    column_rewrite_map: dict[str, str] | None,
) -> list[Expression]:
    """Rewrite a list of expressions."""
    return [_rewrite_expression(e, column_rewrite_map) for e in exprs]


def _rewrite_predicate(
    pred: Predicate | None,
    column_rewrite_map: dict[str, str] | None,
) -> Predicate | None:
    """Rewrite column references in a predicate (e.g., HAVING clause).

    Recursively traverses the predicate tree and rewrites ColumnRef
    expressions using the column_rewrite_map.

    Args:
        pred: The predicate to rewrite (may be None)
        column_rewrite_map: Mapping of old column refs to new ones

    Returns:
        The rewritten predicate, or None/original if no rewriting needed
    """
    from alma_algebrakit.models.algebra import AtomicPredicate, CompoundPredicate

    if pred is None or column_rewrite_map is None:
        return pred

    if isinstance(pred, AtomicPredicate):
        new_left = _rewrite_expression(pred.left, column_rewrite_map)
        new_right = (
            _rewrite_expression(pred.right, column_rewrite_map) if pred.right is not None else None
        )
        if new_left is pred.left and new_right is pred.right:
            return pred
        return AtomicPredicate(left=new_left, op=pred.op, right=new_right)

    if isinstance(pred, CompoundPredicate):
        new_operands = [_rewrite_predicate(op, column_rewrite_map) for op in pred.operands]
        # Check if anything changed
        if all(new is old for new, old in zip(new_operands, pred.operands, strict=False)):
            return pred
        return CompoundPredicate(op=pred.op, operands=new_operands)

    return pred
