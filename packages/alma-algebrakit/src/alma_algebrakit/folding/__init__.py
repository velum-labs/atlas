"""Query folding (factoring out views from RA expressions).

This module provides SQL-agnostic query folding capabilities for relational
algebra expressions. It enables rewriting queries to use pre-defined views
by "factoring out" common subexpressions.

Key concepts:
- ViewSpecification: A view definition (RA expression + metadata)
- FoldResult: Result of folding (rewritten RA or boundary analysis)
- RAFolder: Main folding engine

Example usage:
    from alma_algebrakit.folding import RAFolder, ViewSpecification

    # Create a view specification
    view = ViewSpecification(
        name="customer_orders",
        ra_expression=view_ra,  # Optional
        base_tables=frozenset({"customers", "orders"}),
        column_lineage={("c", "id"): "customer_id", ("o", "amount"): "amount"},
        exposed_columns=frozenset({"customer_id", "amount"}),
    )

    # Fold a query
    folder = RAFolder()
    result = folder.fold(query_ra, view)

    if result.success:
        if result.coverage == FoldCoverage.FULL:
            # Use result.rewritten_ra
            pass
        else:
            # Use result.boundary_analysis for SQL-level rewriting
            pass
"""

from alma_algebrakit.folding.boundary import (
    analyze_boundary,
    normalize_table_name,
)
from alma_algebrakit.folding.config import (
    AGGREGATE_FUNCTION_NAMES,
    FoldingConfig,
)
from alma_algebrakit.folding.folder import RAFolder
from alma_algebrakit.folding.outer_join_inference import (
    JoinInferenceResult,
    OuterJoinInference,
    analyze_boundary_joins_with_constraints,
    infer_join_type,
    infer_outer_join_safety,
)
from alma_algebrakit.folding.self_join import (
    SelfJoinAnalysis,
    analyze_self_joins,
    check_self_join_rewritable,
    create_multi_instance_column_mapping,
    detect_self_joins,
    group_by_physical_table,
)
from alma_algebrakit.folding.types import (
    BoundaryAnalysis,
    FoldCoverage,
    FoldResult,
    JoinStep,
    ViewSpecification,
)
from alma_algebrakit.folding.validation import (
    SPJValidationResult,
    check_correlated_subqueries_safe,
    check_view_is_spj,
    validate_partial_fold,
)

__all__ = [
    # Main folder class
    "RAFolder",
    # Types
    "ViewSpecification",
    "FoldResult",
    "FoldCoverage",
    "BoundaryAnalysis",
    "JoinStep",
    "SPJValidationResult",
    # Config
    "FoldingConfig",
    "AGGREGATE_FUNCTION_NAMES",
    # Functions
    "analyze_boundary",
    "normalize_table_name",
    "check_view_is_spj",
    "validate_partial_fold",
    "check_correlated_subqueries_safe",
    # Self-join support
    "SelfJoinAnalysis",
    "analyze_self_joins",
    "check_self_join_rewritable",
    "create_multi_instance_column_mapping",
    "detect_self_joins",
    "group_by_physical_table",
    # Outer join inference
    "JoinInferenceResult",
    "OuterJoinInference",
    "analyze_boundary_joins_with_constraints",
    "infer_join_type",
    "infer_outer_join_safety",
]
