"""RAFolder - Core RA-level query folding implementation.

This module provides the main RAFolder class that performs SQL-agnostic
query folding (factoring out views from relational algebra expressions).
"""

from __future__ import annotations

from alma_algebrakit.folding.boundary import analyze_boundary, normalize_table_name
from alma_algebrakit.folding.config import FoldingConfig
from alma_algebrakit.folding.types import (
    FoldCoverage,
    FoldResult,
    ViewSpecification,
)
from alma_algebrakit.folding.validation import (
    check_correlated_subqueries_safe,
    check_view_is_spj,
    validate_partial_fold,
)
from alma_algebrakit.models.algebra import (
    Join,
    Predicate,
    Projection,
    RAExpression,
    Relation,
    Selection,
)
from alma_algebrakit.normalizer import (
    RANormalizer,
    extract_top_level_operators,
    wrap_with_operators,
)
from alma_algebrakit.rewriting.predicates import extract_selection_predicates
from alma_algebrakit.transforms.decorrelate import decorrelate_query


def _is_cte_query(ra: RAExpression) -> bool:
    """Check if an RA expression is a CTE query (WITH clause).

    Handles both algebrakit's WithExpression and sqlkit's WithClause,
    which share the same interface (ctes + main_query attributes).
    """
    return hasattr(ra, "ctes") and hasattr(ra, "main_query")


def _get_main_query_tables(query_ra: RAExpression) -> set[str]:
    """Get base tables for coverage computation.

    For CTE queries:
    1. First try: main query body tables minus CTE names
    2. If main query only references CTEs (no base tables), fall back to
       ALL base tables across the entire query (including CTE bodies)

    This handles two patterns:
    - Partial coverage: main query has base tables + CTEs → use main query tables
    - Full CTE: main query only uses CTEs (e.g., FROM start_value s, current_value c)
      → check if CTE body tables are covered
    """
    if _is_cte_query(query_ra):
        cte_names = {cte.name.lower() for cte in query_ra.ctes}
        main_tables = {normalize_table_name(t) for t in query_ra.main_query.referenced_tables()}
        base_tables = main_tables - cte_names

        if base_tables:
            return base_tables

        # Main query only references CTEs — fall back to all base tables
        all_tables = {normalize_table_name(t) for t in query_ra.referenced_tables()}
        return all_tables

    return {normalize_table_name(t) for t in query_ra.referenced_tables()}


class RAFolder:
    """Folds (factors out) views from RA expressions.

    This class performs SQL-agnostic query folding. It determines whether
    a view can be used to rewrite a query, and if so, produces either:
    - A rewritten RA expression (for full coverage)
    - A BoundaryAnalysis (for partial coverage) that can be used by
      higher-level layers to generate SQL

    Example usage:
        folder = RAFolder()
        result = folder.fold(query_ra, view_spec)
        if result.success:
            if result.coverage == FoldCoverage.FULL:
                # Use result.rewritten_ra
                pass
            else:
                # Use result.boundary_analysis for SQL-level rewriting
                pass
    """

    def __init__(self, config: FoldingConfig | None = None) -> None:
        """Initialize the folder.

        Args:
            config: Optional folding configuration
        """
        self.config = config or FoldingConfig()
        self.normalizer = RANormalizer()

    def fold(
        self,
        query_ra: RAExpression,
        view: ViewSpecification,
        view_alias: str = "cv",
    ) -> FoldResult:
        """Attempt to factor out the view from the query.

        This is the main entry point for folding. It:
        1. (Optional) Decorrelates subqueries to enable more folding
        2. Computes coverage (full/partial/none)
        3. For full coverage: returns rewritten RA
        4. For partial coverage: returns BoundaryAnalysis

        Args:
            query_ra: The query as an RA expression
            view: The view specification
            view_alias: Alias to use for the view (default: "cv")

        Returns:
            FoldResult with success status, coverage, and rewrite details
        """
        # Step 0: Decorrelate subqueries if enabled
        # This transforms EXISTS/IN subqueries to SEMI/ANTI joins,
        # enabling folding that would otherwise be blocked by correlation
        if self.config.auto_decorrelate:
            decorr_result = decorrelate_query(query_ra)
            if decorr_result.success and decorr_result.decorrelated_ra is not None:
                query_ra = decorr_result.decorrelated_ra
            # If decorrelation fails, continue with original query
            # (will be rejected later if correlation blocks folding)

        # Step 1: Compute coverage
        coverage, coverage_fraction = self.compute_coverage(query_ra, view)

        if coverage == FoldCoverage.NONE:
            return FoldResult(
                success=False,
                coverage=FoldCoverage.NONE,
                rejection_reasons=["No table overlap between query and view"],
            )

        # Step 2: Check basic fold conditions
        can_fold, reasons = self.check_fold_condition(query_ra, view)
        if not can_fold and coverage == FoldCoverage.FULL:
            # For full coverage, some conditions are less strict
            # (e.g., SPJ not required)
            pass  # Continue to full coverage path

        # Step 3: Handle based on coverage type
        if coverage == FoldCoverage.FULL:
            return self._fold_full_coverage(query_ra, view, view_alias)
        else:
            return self._fold_partial_coverage(query_ra, view, view_alias, reasons)

    def check_fold_condition(
        self,
        query_ra: RAExpression,
        view: ViewSpecification,
    ) -> tuple[bool, list[str]]:
        """Check if basic folding conditions are satisfied.

        This checks:
        - Table overlap
        - Attribute coverage threshold
        - Predicate implication (query predicates must be implied by view predicates)
        - For partial coverage: SPJ requirement

        Args:
            query_ra: The query as an RA expression
            view: The view specification

        Returns:
            (can_fold, reasons) tuple
        """
        reasons: list[str] = []

        # Check table overlap
        query_tables = {normalize_table_name(t) for t in query_ra.referenced_tables()}
        view_tables = {normalize_table_name(t) for t in view.base_tables}
        overlap = query_tables & view_tables

        if not overlap:
            reasons.append("No table overlap between query and view")
            return False, reasons

        # Check attribute coverage
        attr_coverage = self._compute_attribute_coverage(
            query_ra.output_columns(), view.exposed_columns
        )
        if attr_coverage < self.config.min_attribute_coverage:
            reasons.append(
                f"Attribute coverage {attr_coverage:.2f} below threshold "
                f"{self.config.min_attribute_coverage}"
            )
            return False, reasons

        # Check predicate implication: query predicates on covered tables
        # must be implied by view predicates
        if self.config.check_predicate_implication and view.ra_expression:
            predicate_issues = self._check_predicate_implication(query_ra, view, view_tables)
            if predicate_issues:
                if self.config.strict_predicate_check:
                    reasons.extend(predicate_issues)
                    return False, reasons
                else:
                    # Add as warnings but don't reject
                    reasons.extend(f"Warning: {issue}" for issue in predicate_issues)

        # For partial coverage, check SPJ
        full_coverage = query_tables <= view_tables
        if not full_coverage:
            spj_result = check_view_is_spj(view.ra_expression, self.config)
            if not spj_result.is_spj:
                reasons.append(f"Partial coverage requires SPJ view: {spj_result.reason}")
                return False, reasons

        return True, reasons

    def _check_predicate_implication(
        self,
        query_ra: RAExpression,
        view: ViewSpecification,
        covered_tables: set[str],
    ) -> list[str]:
        """Check if query predicates on covered tables are implied by view predicates.

        For correctness, any query predicate that references only covered tables
        must be implied by the view predicates (otherwise the rewrite changes semantics).

        Args:
            query_ra: The query as an RA expression
            view: The view specification
            covered_tables: Set of normalized table names covered by the view

        Returns:
            List of issue descriptions (empty if all predicates are properly implied)
        """
        issues: list[str] = []

        # Extract predicates
        query_preds = extract_selection_predicates(query_ra)
        view_preds = extract_selection_predicates(view.ra_expression) if view.ra_expression else []

        if not query_preds:
            return issues  # No predicates to check

        # Get view predicate fingerprints for quick matching
        view_pred_fps = {p.fingerprint() for p in view_preds}

        # Check each query predicate
        for pred in query_preds:
            # Get tables referenced by this predicate
            pred_tables: set[str] = set()
            for col in pred.referenced_columns():
                if "." in col:
                    table = normalize_table_name(col.split(".")[0])
                    pred_tables.add(table)

            # Only check predicates that reference ONLY covered tables
            if not pred_tables or not pred_tables.issubset(covered_tables):
                continue  # Predicate references remaining tables, will be kept

            # Check if predicate is implied by view predicates
            pred_fp = pred.fingerprint()

            # Quick check: exact fingerprint match
            if pred_fp in view_pred_fps:
                continue  # Predicate matches exactly, safe to drop

            # Algebraic check: use normalizer's predicate_implies for
            # conjunction/disjunction logic (e.g., (A AND B) implies A)
            implied = False
            for view_pred in view_preds:
                if self.normalizer.predicate_implies(view_pred, pred):
                    implied = True
                    break
            if implied:
                continue

            # Not proven implied - this could cause incorrect results
            issues.append(
                f"Query predicate '{pred_fp}' on covered tables "
                f"is not implied by any view predicate"
            )

        return issues

    def compute_coverage(
        self,
        query_ra: RAExpression,
        view: ViewSpecification,
    ) -> tuple[FoldCoverage, float]:
        """Compute the coverage type and fraction.

        For CTE queries (WithExpression), coverage is computed against the
        **main query body** only, excluding tables referenced inside CTE
        definitions and CTE names. CTEs are opaque subqueries that are kept
        as-is in the rewritten SQL.

        Args:
            query_ra: The query as an RA expression
            view: The view specification

        Returns:
            (coverage_type, coverage_fraction) tuple
        """
        query_tables = _get_main_query_tables(query_ra)
        view_tables = {normalize_table_name(t) for t in view.base_tables}

        if not query_tables:
            return FoldCoverage.NONE, 0.0

        overlap = query_tables & view_tables
        if not overlap:
            return FoldCoverage.NONE, 0.0

        coverage_fraction = len(overlap) / len(query_tables)

        if query_tables <= view_tables:
            return FoldCoverage.FULL, coverage_fraction
        else:
            return FoldCoverage.PARTIAL, coverage_fraction

    def _compute_attribute_coverage(
        self,
        query_attrs: set[str],
        view_attrs: frozenset[str],
    ) -> float:
        """Compute what fraction of query attributes are in the view."""
        if not query_attrs:
            return 0.0

        # Normalize attribute names (remove table prefixes for comparison)
        query_cols = {a.split(".")[-1] for a in query_attrs if a != "*"}

        if not query_cols:
            return 1.0  # SELECT * case

        covered = query_cols & view_attrs
        return len(covered) / len(query_cols)

    def _fold_full_coverage(
        self,
        query_ra: RAExpression,
        view: ViewSpecification,
        view_alias: str,
    ) -> FoldResult:
        """Handle full coverage folding.

        For full coverage, we can produce a complete rewritten RA expression.
        """
        # Step 1: Peel top-level operators (Sort, Limit, Aggregation)
        ops = extract_top_level_operators(query_ra)

        # Step 2: Rewrite the core query to use the view
        rewritten_core = self._rewrite_core_for_view(ops.core, view, view_alias)

        # Step 3: Build column rewrite map (for full coverage, typically identity)
        # For full coverage, columns map through the view's lineage
        column_rewrite_map = self._build_full_coverage_column_map(query_ra, view, view_alias)

        # Step 4: Re-wrap with top-level operators
        # Note: This may need column rewrites for ORDER BY, GROUP BY, etc.
        rewritten_ra = wrap_with_operators(rewritten_core, ops, column_rewrite_map)

        # Step 5: Extract predicates for reporting
        kept_predicates: list[Predicate] = []
        dropped_predicates: list[Predicate] = []

        # For full coverage, predicates that match view predicates are dropped
        query_preds = extract_selection_predicates(query_ra)
        if view.ra_expression:
            view_preds = extract_selection_predicates(view.ra_expression)
            view_pred_fps = {p.fingerprint() for p in view_preds}

            for pred in query_preds:
                if pred.fingerprint() in view_pred_fps:
                    dropped_predicates.append(pred)
                else:
                    kept_predicates.append(pred)
        else:
            kept_predicates = query_preds

        return FoldResult(
            success=True,
            coverage=FoldCoverage.FULL,
            rewritten_ra=rewritten_ra,
            column_rewrite_map=column_rewrite_map,
            kept_predicates=kept_predicates,
            dropped_predicates=dropped_predicates,
        )

    def _fold_partial_coverage(
        self,
        query_ra: RAExpression,
        view: ViewSpecification,
        view_alias: str,
        existing_reasons: list[str],
    ) -> FoldResult:
        """Handle partial coverage folding.

        For partial coverage, we cannot produce a complete rewritten RA
        because we need SQL-level JOIN reconstruction. Instead, we return
        a BoundaryAnalysis that higher-level layers can use.
        """
        # Check SPJ requirement
        spj_result = check_view_is_spj(view.ra_expression, self.config)
        if not spj_result.is_spj:
            return FoldResult(
                success=False,
                coverage=FoldCoverage.PARTIAL,
                rejection_reasons=[f"Partial coverage requires SPJ view: {spj_result.reason}"],
            )

        # Perform boundary analysis
        analysis = analyze_boundary(query_ra, view, self.config, view_alias)

        if not analysis.is_valid:
            return FoldResult(
                success=False,
                coverage=FoldCoverage.PARTIAL,
                boundary_analysis=analysis,
                rejection_reasons=analysis.rejection_reasons,
            )

        # Check correlated subqueries
        is_safe, reason = check_correlated_subqueries_safe(query_ra, analysis)
        if not is_safe:
            analysis.is_valid = False
            analysis.rejection_reasons.append(reason)
            return FoldResult(
                success=False,
                coverage=FoldCoverage.PARTIAL,
                boundary_analysis=analysis,
                rejection_reasons=analysis.rejection_reasons,
            )

        # Final validation
        is_valid, reasons = validate_partial_fold(query_ra, analysis, self.config)
        if not is_valid:
            analysis.is_valid = False
            analysis.rejection_reasons.extend(reasons)
            return FoldResult(
                success=False,
                coverage=FoldCoverage.PARTIAL,
                boundary_analysis=analysis,
                rejection_reasons=analysis.rejection_reasons,
            )

        # Collect predicates
        kept_predicates = analysis.predicates.keep if analysis.predicates else []
        dropped_predicates = analysis.predicates.drop if analysis.predicates else []

        return FoldResult(
            success=True,
            coverage=FoldCoverage.PARTIAL,
            boundary_analysis=analysis,
            column_rewrite_map=analysis.column_rewrite_map,
            kept_predicates=kept_predicates,
            dropped_predicates=dropped_predicates,
        )

    def _rewrite_core_for_view(
        self,
        core: RAExpression,
        view: ViewSpecification,
        view_alias: str,
    ) -> RAExpression:
        """Rewrite the core query to use the view instead of base tables.

        This replaces base relations/joins with a single view reference.
        """
        # Create a view relation
        view_relation = Relation(
            name=view.name,
            alias=view_alias,
            columns=list(view.exposed_columns),
        )

        # For full coverage, the core should be a Relation, Join, or wrapped in
        # Selection/Projection. We need to find and replace the base tables.
        return self._replace_base_tables(core, view_relation)

    def _replace_base_tables(
        self,
        expr: RAExpression,
        view_relation: Relation,
    ) -> RAExpression:
        """Recursively replace base tables with the view relation."""
        if isinstance(expr, Relation):
            # Replace with view
            return view_relation

        elif isinstance(expr, Join):
            # For full coverage, the entire join becomes the view
            return view_relation

        elif isinstance(expr, Selection):
            # Keep the selection but replace the input
            rewritten_input = self._replace_base_tables(expr.input, view_relation)
            return Selection(
                predicate=expr.predicate,
                input=rewritten_input,
            )

        elif isinstance(expr, Projection):
            # Keep the projection but replace the input
            rewritten_input = self._replace_base_tables(expr.input, view_relation)
            return Projection(
                columns=expr.columns,
                input=rewritten_input,
                distinct=expr.distinct,
                distinct_on=expr.distinct_on,
            )

        # For other types, return as-is (shouldn't happen for core)
        return expr

    def _build_full_coverage_column_map(
        self,
        query_ra: RAExpression,
        view: ViewSpecification,
        view_alias: str,
    ) -> dict[str, str]:
        """Build column rewrite map for full coverage.

        For full coverage, columns from query tables map to view columns
        via the lineage.
        """
        column_map: dict[str, str] = {}

        if view.is_select_star:
            # Passthrough: columns keep their names
            all_rels = query_ra.relation_instances()
            for rel in all_rels:
                alias = rel.effective_name()
                # For SELECT *, assume all columns pass through with same name
                # The actual columns would need to be resolved from the catalog
                # For now, we create a pattern-based mapping
                column_map[f"{alias}."] = f"{view_alias}."
        else:
            # Use lineage
            for (table, col), view_col in view.column_lineage.items():
                column_map[f"{table}.{col}"] = f"{view_alias}.{view_col}"

        return column_map
