"""Boundary analysis for partial coverage query folding.

This module provides functions for analyzing the boundary between covered
and remaining relations when a view only partially covers a query's tables.
"""

from __future__ import annotations

from alma_algebrakit.folding.config import FoldingConfig
from alma_algebrakit.folding.outer_join_inference import (
    analyze_boundary_joins_with_constraints,
)
from alma_algebrakit.folding.self_join import (
    analyze_self_joins,
)
from alma_algebrakit.folding.types import (
    BoundaryAnalysis,
    JoinStep,
    ViewSpecification,
)
from alma_algebrakit.models.algebra import (
    Predicate,
    RAExpression,
    RelationRef,
)
from alma_algebrakit.normalizer import JoinEdge, extract_detailed_join_graph
from alma_algebrakit.rewriting.columns import (
    build_column_rewrite_map,
    check_column_availability,
)
from alma_algebrakit.rewriting.joins import check_outer_join_safety
from alma_algebrakit.rewriting.predicates import (
    classify_predicates,
    extract_selection_predicates,
    rewrite_on_predicate,
)


def normalize_table_name(table: str) -> str:
    """Normalize table name by stripping the schema prefix and lowercasing.

    This strips schema qualification and lowercases. Use this when you need to
    compare table names that may or may not be schema-qualified.

    For name normalization that preserves schema qualification (lowercase +
    strip quotes only), use ``algebrakit.naming.normalize_name()`` instead.

    Args:
        table: Table name, possibly schema-qualified (e.g., "acme.orders")

    Returns:
        Unqualified lowercase table name (e.g., "orders")

    Examples:
        "acme.position_snapshot" -> "position_snapshot"
        "POSITION_SNAPSHOT" -> "position_snapshot"
        "position_snapshot" -> "position_snapshot"
    """
    return table.split(".")[-1].lower()


def analyze_boundary(
    query_ra: RAExpression,
    view: ViewSpecification,
    config: FoldingConfig | None = None,
    view_alias: str = "cv",
) -> BoundaryAnalysis:
    """Analyze the boundary between covered and remaining relations.

    This is the core analysis for partial coverage rewriting. It:
    1. Partitions relations into covered vs remaining
    2. Builds a complete join plan including remaining↔remaining joins
    3. Classifies predicates as keep vs drop
    4. Maps required columns through the view's lineage
    5. Validates that the rewrite is possible

    Args:
        query_ra: The query as an RA expression
        view: The view specification for coverage
        config: Optional folding configuration
        view_alias: Alias to use for the view (default: "cv")

    Returns:
        BoundaryAnalysis with all information needed for rewriting
    """
    config = config or FoldingConfig()
    analysis = BoundaryAnalysis()

    # Step 1: Partition relations (with normalization for table comparison)
    # For CTE queries, only consider main query relations (CTE internals are opaque)
    if hasattr(query_ra, "ctes") and hasattr(query_ra, "main_query"):
        all_rels = query_ra.main_query.relation_instances()
        # Exclude CTE name references from relation instances
        cte_names_lower = {cte.name.lower() for cte in query_ra.ctes}
        all_rels = {r for r in all_rels if r.physical_table.lower() not in cte_names_lower}
    else:
        all_rels = query_ra.relation_instances()
    view_tables_normalized = {normalize_table_name(t) for t in view.base_tables}

    for rel in all_rels:
        if normalize_table_name(rel.physical_table) in view_tables_normalized:
            analysis.covered_rels.add(rel)
        else:
            analysis.remaining_rels.add(rel)

    # Gate: Check for self-joins on covered tables (with extended support)
    covered_tables = [r.physical_table for r in analysis.covered_rels]
    if len(covered_tables) != len(set(covered_tables)):
        if config.allow_self_join_rewriting and view.schema_constraints:
            # Try extended self-join analysis with key preservation check
            self_join_result = analyze_self_joins(
                analysis.covered_rels,
                view.exposed_columns,
                view.schema_constraints,
                view_alias,
            )

            if self_join_result.is_rewritable:
                # Self-join can be rewritten - store the instance info
                analysis.self_join_instances = self_join_result.instance_groups
            else:
                # Self-join cannot be rewritten
                analysis.is_valid = False
                analysis.rejection_reasons.append(
                    self_join_result.rejection_reason or "Self-join on covered tables not supported"
                )
                return analysis
        else:
            # No schema constraints or feature disabled - reject
            duplicates = [t for t in set(covered_tables) if covered_tables.count(t) > 1]
            analysis.is_valid = False
            if not config.allow_self_join_rewriting:
                analysis.rejection_reasons.append(
                    f"Self-join on covered tables not supported (allow_self_join_rewriting=False): {duplicates}"
                )
            else:
                analysis.rejection_reasons.append(
                    f"Self-join on covered tables requires schema_constraints for key analysis: {duplicates}"
                )
            return analysis

    # Gate: Check for self-joins on remaining tables that would cause ambiguity
    remaining_tables = [r.physical_table for r in analysis.remaining_rels]
    if len(remaining_tables) != len(set(remaining_tables)):
        # Self-joins on remaining tables are OK as long as they have distinct aliases
        remaining_aliases = [r.alias for r in analysis.remaining_rels]
        if len(remaining_aliases) != len(set(remaining_aliases)):
            duplicates = [a for a in set(remaining_aliases) if remaining_aliases.count(a) > 1]
            analysis.is_valid = False
            analysis.rejection_reasons.append(
                f"Duplicate alias in remaining tables not supported: {duplicates}"
            )
            return analysis

    # Step 2: Extract join graph
    join_edges = extract_detailed_join_graph(query_ra)

    # Step 3: Build join plan
    _build_join_plan(analysis, join_edges, view, view_alias, original_ra=query_ra)

    # Step 4: Check outer join safety (with extended constraint-based inference)
    if config.use_constraint_inference and view.schema_constraints:
        # Use constraint-based inference for outer join safety
        is_safe, reasons, effective_types = analyze_boundary_joins_with_constraints(
            join_edges,
            analysis.covered_rels,
            analysis.remaining_rels,
            view.schema_constraints,
        )
        if not is_safe:
            analysis.is_valid = False
            analysis.rejection_reasons.extend(reasons)
            return analysis

        # Store effective join types for rewriting
        analysis.effective_join_types = effective_types
    else:
        # Fall back to conservative outer join safety check
        is_safe, reason = check_outer_join_safety(
            query_ra, analysis.covered_rels, analysis.remaining_rels
        )
        if not is_safe:
            analysis.is_valid = False
            analysis.rejection_reasons.append(reason)
            return analysis

    # Step 5: Classify predicates
    _classify_predicates_for_analysis(analysis, query_ra, view, config)

    # Step 6: Build column rewrite map
    covered_aliases = {r.effective_name() for r in analysis.covered_rels}
    analysis.column_rewrite_map = build_column_rewrite_map(
        query_ra,
        covered_aliases,
        view.column_lineage,
        view.is_select_star,
        view_alias,
    )

    # Step 7: Check column availability
    all_available, missing = check_column_availability(
        analysis.required_cv_columns | analysis.predicates.required_covered_cols
        if analysis.predicates
        else analysis.required_cv_columns,
        covered_aliases,
        analysis.covered_rels,
        view.column_lineage,
        view.is_select_star,
    )
    if not all_available:
        analysis.missing_cv_columns = missing
        analysis.is_valid = False
        analysis.rejection_reasons.append(f"Missing columns in view: {sorted(missing)}")

    return analysis


def _build_join_plan(
    analysis: BoundaryAnalysis,
    join_edges: list[JoinEdge],
    view: ViewSpecification,
    view_alias: str,
    original_ra: RAExpression | None = None,
) -> None:
    """Build the complete join plan from covered→remaining and remaining↔remaining joins."""
    covered = analysis.covered_rels
    remaining = analysis.remaining_rels

    # Track which remaining relations have been joined
    joined_remaining: set[RelationRef] = set()

    for edge in join_edges:
        # Skip covered↔covered joins (absorbed into view)
        if edge.is_covered_to_covered(covered, remaining):
            continue

        # Handle boundary joins (view ↔ remaining)
        if edge.crosses_partition(covered, remaining):
            remaining_rels = edge.get_remaining_side_rels(remaining)

            # For each remaining relation in this edge, create a join step
            for rel in remaining_rels:
                if rel in joined_remaining:
                    continue  # Already joined

                # Rewrite the ON predicate to use view alias for covered columns
                on_sql = rewrite_on_predicate(
                    edge.on_predicate,
                    covered,
                    view.column_lineage,
                    view_alias,
                )

                step = JoinStep(
                    join_type=edge.join_type,
                    right_rel=rel,
                    on_predicate=edge.on_predicate,
                    on_sql=on_sql,
                    is_boundary=True,
                )
                analysis.join_plan.append(step)
                joined_remaining.add(rel)

                # Track required columns from covered side
                for col_ref in edge.columns_from_covered_side(covered):
                    analysis.required_cv_columns.add(col_ref)

        # Handle remaining↔remaining joins (must preserve exactly)
        elif edge.is_remaining_to_remaining(covered, remaining):
            # These joins connect remaining tables to each other
            # We need to preserve them in order
            right_rels = edge.right_rels & remaining

            for rel in right_rels:
                if rel in joined_remaining:
                    continue

                # ON predicate stays the same for remaining↔remaining
                on_sql = edge.on_predicate.fingerprint() if edge.on_predicate else ""

                step = JoinStep(
                    join_type=edge.join_type,
                    right_rel=rel,
                    on_predicate=edge.on_predicate,
                    on_sql=on_sql,
                    is_boundary=False,
                )
                analysis.join_plan.append(step)
                joined_remaining.add(rel)

    # Check that all remaining relations are reachable
    unreachable = remaining - joined_remaining

    # Exclude CTE-derived relations from the unreachable check.
    # CTEs (e.g., WITH latest_snapshots AS (...)) appear as relation instances
    # but are not base tables — they're kept as-is in the rewritten query.
    cte_names = _extract_cte_names(original_ra) if original_ra else set()
    truly_unreachable = {
        r for r in unreachable
        if r.physical_table.lower() not in cte_names
    }

    if truly_unreachable and len(remaining) > 0:
        if len(remaining) == 1 and not joined_remaining:
            pass
        elif truly_unreachable:
            analysis.is_valid = False
            analysis.rejection_reasons.append(
                f"Remaining relations have no join path: {[str(r) for r in truly_unreachable]}"
            )


def _classify_predicates_for_analysis(
    analysis: BoundaryAnalysis,
    query_ra: RAExpression,
    view: ViewSpecification,
    config: FoldingConfig,
) -> None:
    """Classify query predicates as keep vs drop for boundary analysis.

    Uses the basic classify_predicates from alma_algebrakit.rewriting.predicates.
    For full SMT-based predicate implication checking, use the higher-level
    query-analyzer layer.
    """
    # Extract selection predicates only (not JOIN ON conditions)
    query_predicates = extract_selection_predicates(query_ra)

    # Extract view predicates if we have the RA expression
    view_predicates: list[Predicate] | None = None
    if view.ra_expression:
        view_predicates = extract_selection_predicates(view.ra_expression)

    # Get alias sets
    covered_aliases = {r.effective_name() for r in analysis.covered_rels}
    remaining_aliases = {r.effective_name() for r in analysis.remaining_rels}

    # Classify predicates
    analysis.predicates = classify_predicates(
        query_predicates,
        view_predicates,
        covered_aliases,
        remaining_aliases,
    )


def _extract_cte_names(expr: RAExpression) -> set[str]:
    """Extract all CTE names defined in the expression tree.

    Traverses the RA tree to find WithExpression/WithClause nodes and
    collects the names of all CTEs. These are used to exclude CTE-derived
    relations from the unreachable check in boundary analysis.

    Handles both algebrakit's WithExpression and sqlkit's WithClause
    (both have ``ctes`` and ``main_query`` attributes).
    """
    from alma_algebrakit.models.algebra import (
        Aggregation,
        Difference,
        Intersect,
        Join,
        Limit,
        Projection,
        Selection,
        Sort,
        Union,
    )

    names: set[str] = set()

    # Check for CTE container (WithExpression or WithClause)
    if hasattr(expr, "ctes") and hasattr(expr, "main_query"):
        for cte in expr.ctes:
            names.add(cte.name.lower())
        names.update(_extract_cte_names(expr.main_query))
    elif isinstance(expr, (Selection, Projection, Aggregation, Sort, Limit)):
        names.update(_extract_cte_names(expr.input))
    elif isinstance(expr, Join):
        names.update(_extract_cte_names(expr.left))
        names.update(_extract_cte_names(expr.right))
    elif isinstance(expr, (Union, Difference, Intersect)):
        names.update(_extract_cte_names(expr.left))
        names.update(_extract_cte_names(expr.right))

    return names
