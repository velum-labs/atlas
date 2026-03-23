"""Predicate implication and query containment checking."""

from alma_algebrakit.proof.containment import (
    ContainmentChecker,
    ContainmentMethod,
    ContainmentResult,
    CQAtom,
    CQRepresentation,
    GeneralizationResult,
    QueryGeneralizer,
    cq_from_bound_query,
)
from alma_algebrakit.proof.empirical import (
    DuckDBExecutor,
    EmpiricalValidator,
    QueryExecutor,
    ValidationConfig,
    ValidationResult,
    ValidationTier,
)
from alma_algebrakit.proof.implication import (
    ImplicationMethod,
    ImplicationResult,
    PredicateImplicationChecker,
)
from alma_algebrakit.proof.linear_arithmetic import (
    LinearArithmeticResult,
    LinearCheckResult,
    LinearInequality,
    check_linear_implication,
    is_linear_predicate,
)

__all__ = [
    # Implication
    "PredicateImplicationChecker",
    "ImplicationResult",
    "ImplicationMethod",
    # Linear Arithmetic
    "check_linear_implication",
    "is_linear_predicate",
    "LinearArithmeticResult",
    "LinearCheckResult",
    "LinearInequality",
    # Containment
    "ContainmentChecker",
    "ContainmentResult",
    "ContainmentMethod",
    # CQ Representation
    "CQRepresentation",
    "CQAtom",
    "cq_from_bound_query",
    # Generalization
    "QueryGeneralizer",
    "GeneralizationResult",
    # Empirical
    "EmpiricalValidator",
    "ValidationTier",
    "ValidationConfig",
    "ValidationResult",
    "QueryExecutor",
    "DuckDBExecutor",
]
