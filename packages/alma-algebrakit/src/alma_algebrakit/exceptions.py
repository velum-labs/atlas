"""Exception hierarchy for algebrakit.

This module defines a comprehensive exception hierarchy that enables
proper error handling, debugging, and user-facing error messages.

Exception Hierarchy:
    AlgebraKitError (base)
    ├── ScopeError
    │   ├── AmbiguousColumnError
    │   └── UnresolvedColumnError
    ├── ProofError
    │   ├── SMTError
    │   ├── SMTTimeoutError
    │   └── SMTEncodingError
    ├── FoldingError
    │   ├── FoldRejectionError
    │   ├── BoundaryAnalysisError
    │   └── SPJValidationError
    └── ValidationError
        ├── TypeMismatchError
        └── InvalidPredicateError
"""

from __future__ import annotations


class AlgebraKitError(Exception):
    """Base exception for all algebrakit errors.

    All exceptions raised by algebrakit should inherit from this class.
    This allows consumers to catch all algebrakit errors with a single
    except clause.
    """

    pass


# =============================================================================
# Scope Errors
# =============================================================================


class ScopeError(AlgebraKitError):
    """Base class for scope-related errors.

    Raised when there are issues with variable/column scoping during
    query analysis.
    """

    pass


class AmbiguousColumnError(ScopeError):
    """Raised when a column reference is ambiguous.

    This occurs when an unqualified column name matches multiple
    relations in scope.

    Example:
        SELECT id FROM users, orders  -- 'id' is ambiguous
    """

    def __init__(
        self,
        column_name: str,
        matching_relations: list[str],
        message: str | None = None,
    ):
        self.column_name = column_name
        self.matching_relations = matching_relations
        if message is None:
            message = (
                f"Column '{column_name}' is ambiguous; "
                f"found in relations: {', '.join(matching_relations)}"
            )
        super().__init__(message)


class UnresolvedColumnError(ScopeError):
    """Raised when a column reference cannot be resolved.

    This occurs when a column name doesn't match any relation in scope.

    Example:
        SELECT nonexistent_column FROM users
    """

    def __init__(
        self,
        column_name: str,
        available_relations: list[str] | None = None,
        message: str | None = None,
    ):
        self.column_name = column_name
        self.available_relations = available_relations
        if message is None:
            if available_relations:
                message = (
                    f"Column '{column_name}' not found in any relation in scope. "
                    f"Available relations: {', '.join(available_relations)}"
                )
            else:
                message = f"Column '{column_name}' not found in any relation in scope"
        super().__init__(message)


# =============================================================================
# Proof Errors
# =============================================================================


class ProofError(AlgebraKitError):
    """Base class for predicate implication/containment proof errors.

    Raised when there are issues during predicate implication checking
    or query containment verification.
    """

    pass


class SMTError(ProofError):
    """Base class for SMT solver-related errors."""

    pass


class SMTTimeoutError(SMTError):
    """Raised when the SMT solver times out.

    This indicates that the predicate implication check was too complex
    to resolve within the configured timeout.
    """

    def __init__(
        self,
        timeout_ms: int,
        predicate_description: str | None = None,
        message: str | None = None,
    ):
        self.timeout_ms = timeout_ms
        self.predicate_description = predicate_description
        if message is None:
            desc = f" for {predicate_description}" if predicate_description else ""
            message = f"SMT solver timed out after {timeout_ms}ms{desc}"
        super().__init__(message)


class SMTEncodingError(SMTError):
    """Raised when a predicate cannot be encoded for SMT solving.

    This can happen when:
    - The predicate uses unsupported operators
    - Type information is missing or incompatible
    - The predicate structure is too complex
    """

    def __init__(
        self,
        reason: str,
        predicate_fingerprint: str | None = None,
        message: str | None = None,
    ):
        self.reason = reason
        self.predicate_fingerprint = predicate_fingerprint
        if message is None:
            fp_str = f" ({predicate_fingerprint})" if predicate_fingerprint else ""
            message = f"Cannot encode predicate for SMT{fp_str}: {reason}"
        super().__init__(message)


# =============================================================================
# Folding Errors
# =============================================================================


class FoldingError(AlgebraKitError):
    """Base class for query folding errors.

    Raised when there are issues during query folding/rewriting
    with contract views.
    """

    pass


class FoldRejectionError(FoldingError):
    """Raised when a fold is rejected due to constraint violations.

    This is raised when the folding conditions are not met, such as:
    - Predicate implication failure
    - Attribute coverage below threshold
    - Unsafe outer join patterns
    """

    def __init__(
        self,
        reasons: list[str],
        view_name: str | None = None,
        message: str | None = None,
    ):
        self.reasons = reasons
        self.view_name = view_name
        if message is None:
            view_str = f" with view '{view_name}'" if view_name else ""
            message = f"Fold rejected{view_str}: {'; '.join(reasons)}"
        super().__init__(message)


class BoundaryAnalysisError(FoldingError):
    """Raised when boundary analysis fails for partial coverage folding.

    Boundary analysis determines how to split a query between covered
    (from view) and remaining (from base tables) parts.
    """

    def __init__(
        self,
        reason: str,
        covered_tables: set[str] | None = None,
        remaining_tables: set[str] | None = None,
        message: str | None = None,
    ):
        self.reason = reason
        self.covered_tables = covered_tables
        self.remaining_tables = remaining_tables
        if message is None:
            message = f"Boundary analysis failed: {reason}"
            if covered_tables or remaining_tables:
                message += f" (covered: {covered_tables}, remaining: {remaining_tables})"
        super().__init__(message)


class SPJValidationError(FoldingError):
    """Raised when a view fails SPJ (Select-Project-Join) validation.

    Partial coverage folding requires views to be SPJ queries (no
    aggregation, no subqueries, etc.).
    """

    def __init__(
        self,
        reason: str,
        view_name: str | None = None,
        message: str | None = None,
    ):
        self.reason = reason
        self.view_name = view_name
        if message is None:
            view_str = f" '{view_name}'" if view_name else ""
            message = f"View{view_str} is not a valid SPJ query: {reason}"
        super().__init__(message)


# =============================================================================
# Validation Errors
# =============================================================================


class ValidationError(AlgebraKitError):
    """Base class for validation errors.

    Raised when data or expressions fail validation checks.
    """

    pass


class TypeMismatchError(ValidationError):
    """Raised when there's a type mismatch in expressions.

    Example:
        - Comparing VARCHAR to INTEGER without explicit cast
        - Aggregating non-numeric types with SUM
    """

    def __init__(
        self,
        expected_type: str,
        actual_type: str,
        context: str | None = None,
        message: str | None = None,
    ):
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.context = context
        if message is None:
            ctx_str = f" in {context}" if context else ""
            message = f"Type mismatch{ctx_str}: expected {expected_type}, got {actual_type}"
        super().__init__(message)


class InvalidPredicateError(ValidationError):
    """Raised when a predicate is structurally invalid.

    Example:
        - NOT predicate with multiple operands
        - Comparison with NULL operand (should use IS NULL)
    """

    def __init__(
        self,
        reason: str,
        predicate_fingerprint: str | None = None,
        message: str | None = None,
    ):
        self.reason = reason
        self.predicate_fingerprint = predicate_fingerprint
        if message is None:
            fp_str = f" ({predicate_fingerprint})" if predicate_fingerprint else ""
            message = f"Invalid predicate{fp_str}: {reason}"
        super().__init__(message)
