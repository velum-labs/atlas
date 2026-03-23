"""Rewriting utilities for relational algebra expressions."""

from alma_algebrakit.rewriting.columns import (
    build_column_rewrite_map,
    check_column_availability,
    collect_column_references,
)
from alma_algebrakit.rewriting.equivalence import (
    EquivalenceChecker,
    EquivalenceLevel,
    EquivalenceResult,
)
from alma_algebrakit.rewriting.joins import check_outer_join_safety
from alma_algebrakit.rewriting.predicates import (
    PredicateClassification,
    classify_predicates,
    extract_selection_predicates,
    rewrite_on_predicate,
    rewrite_predicate_columns,
)

__all__ = [
    # Equivalence checking
    "EquivalenceChecker",
    "EquivalenceLevel",
    "EquivalenceResult",
    # Predicate utilities
    "PredicateClassification",
    "classify_predicates",
    "extract_selection_predicates",
    "rewrite_predicate_columns",
    "rewrite_on_predicate",
    # Column utilities
    "collect_column_references",
    "build_column_rewrite_map",
    "check_column_availability",
    # Join utilities
    "check_outer_join_safety",
]
