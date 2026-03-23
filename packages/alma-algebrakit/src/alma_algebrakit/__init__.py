"""AlgebraKit - Pure relational algebra engine (no SQL dependencies).

This package provides a complete relational algebra engine including:
- RA types: Core relational algebra expressions
- Schema: Generic schema representation
- Scope: Algebraic scoping for any nested query language
- Bound types: Resolved expressions with stable identifiers
- Normalization: Canonicalize RA expressions for comparison
- Proof: Predicate implication and query containment checking
- Equivalence: Structural equivalence checking

All abstractions are SQL-agnostic and work for any query language.
"""

from alma_algebrakit.bound.query import (
    BoundOrderItem,
    BoundQuery,
    BoundSelectItem,
    RelationBinding,
)

# Bound types (resolved expressions)
from alma_algebrakit.bound.types import (
    AttributeRef,
    BoundBetween,
    BoundBinaryOp,
    BoundComparison,
    BoundExpression,
    BoundExprLike,
    BoundFunctionCall,
    BoundIn,
    BoundIsNull,
    BoundLike,
    BoundLiteral,
    BoundLogical,
    BoundPredicate,
)

# Config
from alma_algebrakit.config import (
    NormalizationConfig,
    ProofConfig,
)

# Folding (factoring out views from RA expressions)
from alma_algebrakit.folding import (
    AGGREGATE_FUNCTION_NAMES,
    BoundaryAnalysis,
    FoldCoverage,
    FoldingConfig,
    FoldResult,
    JoinStep,
    RAFolder,
    SPJValidationResult,
    ViewSpecification,
    analyze_boundary,
    check_correlated_subqueries_safe,
    check_view_is_spj,
    normalize_table_name,
    validate_partial_fold,
)
from alma_algebrakit.models.algebra import (
    AggregateFunction,
    AggregateSpec,
    Aggregation,
    AtomicPredicate,
    BinaryExpression,
    CaseExpression,
    ColumnRef,
    ComparisonOp,
    CompoundPredicate,
    CTEDefinition,
    Difference,
    ExistsExpression,
    Expression,
    FunctionCall,
    InSubqueryExpression,
    Intersect,
    Join,
    JoinType,
    Limit,
    Literal,
    LogicalOp,
    NullsPosition,
    Predicate,
    Projection,
    RAExpression,
    Relation,
    RelationRef,
    Selection,
    Sort,
    SortDirection,
    SortSpec,
    SubqueryExpression,
    Union,
    WindowExpression,
    WindowFrameBound,
    WindowFrameSpec,
    WindowFrameType,
    WindowSpec,
    WithExpression,
    build_alias_map,
    check_for_self_joins,
    resolve_column_to_relation,
)
from alma_algebrakit.models.capabilities import (
    REWRITE_REQUIREMENTS,
    DeterminismLevel,
    NullMode,
    QueryCapabilities,
    SemanticsMode,
    SQLFeature,
    check_rewrite_compatibility,
)

# Naming utilities (alias generation, qualified names)
from alma_algebrakit.naming import (
    CTE_ID_PREFIX,
    DEFAULT_AGGREGATE_ALIAS,
    DEFAULT_CONTRACT_VIEW_ALIAS,
    DEFAULT_SUBQUERY_ALIAS,
    SUBQUERY_ID_PREFIX,
    QualifiedName,
    effective_table_name,
    generate_column_alias,
    generate_cte_id,
    generate_subquery_alias,
    generate_subquery_id,
    normalize_cte_name,
    normalize_name,
    parse_parts,
)

# Normalization
from alma_algebrakit.normalizer import (
    JoinEdge,
    RANormalizer,
    # Peel/wrap utilities for query rewriting
    TopLevelOperators,
    extract_detailed_join_graph,
    extract_top_level_operators,
    wrap_with_operators,
)

# Rewriting utilities
from alma_algebrakit.rewriting import (
    # Equivalence checking
    EquivalenceChecker,
    EquivalenceLevel,
    EquivalenceResult,
    # Predicate utilities
    PredicateClassification,
    build_column_rewrite_map,
    check_column_availability,
    # Join utilities
    check_outer_join_safety,
    classify_predicates,
    # Column utilities
    collect_column_references,
    extract_selection_predicates,
    rewrite_on_predicate,
    rewrite_predicate_columns,
)
from alma_algebrakit.schema.catalog import (
    Catalog,
    ColumnSchema,
    TableSchema,
)

# Schema (generic, not SQL-specific)
from alma_algebrakit.schema.types import (
    Attribute,
    DataType,
    SQLDataType,
)

# Scope (algebraic scoping - works for any query language)
from alma_algebrakit.scope.instance import RelationInstance
from alma_algebrakit.scope.resolution import (
    AmbiguousColumnError,
    UnresolvedColumnError,
    resolve,
    resolve_star,
)
from alma_algebrakit.scope.scope import Scope

__all__ = [
    # RA types
    "RAExpression",
    "Relation",
    "Selection",
    "Projection",
    "Join",
    "Aggregation",
    "Union",
    "Difference",
    "Intersect",
    "Sort",
    "SortSpec",
    "Limit",
    "WithExpression",
    "CTEDefinition",
    # Predicates
    "Predicate",
    "AtomicPredicate",
    "CompoundPredicate",
    # Expressions
    "Expression",
    "ColumnRef",
    "Literal",
    "BinaryExpression",
    "FunctionCall",
    "WindowFrameSpec",
    "WindowSpec",
    "WindowExpression",
    "CaseExpression",
    "SubqueryExpression",
    "ExistsExpression",
    "InSubqueryExpression",
    # Specs
    "AggregateSpec",
    # Enums
    "JoinType",
    "ComparisonOp",
    "LogicalOp",
    "AggregateFunction",
    "SortDirection",
    "NullsPosition",
    "WindowFrameType",
    "WindowFrameBound",
    # Relation Identity
    "RelationRef",
    "build_alias_map",
    "resolve_column_to_relation",
    "check_for_self_joins",
    # Capabilities
    "QueryCapabilities",
    "SQLFeature",
    "SemanticsMode",
    "NullMode",
    "DeterminismLevel",
    "REWRITE_REQUIREMENTS",
    "check_rewrite_compatibility",
    # Schema
    "DataType",
    "SQLDataType",
    "Attribute",
    "Catalog",
    "TableSchema",
    "ColumnSchema",
    # Scope
    "RelationInstance",
    "Scope",
    "resolve",
    "resolve_star",
    "AmbiguousColumnError",
    "UnresolvedColumnError",
    # Bound types
    "AttributeRef",
    "BoundExpression",
    "BoundPredicate",
    "BoundComparison",
    "BoundLogical",
    "BoundLiteral",
    "BoundFunctionCall",
    "BoundBinaryOp",
    "BoundIsNull",
    "BoundIn",
    "BoundBetween",
    "BoundLike",
    "BoundExprLike",
    "BoundQuery",
    "BoundSelectItem",
    "BoundOrderItem",
    "RelationBinding",
    # Normalization
    "RANormalizer",
    "JoinEdge",
    "extract_detailed_join_graph",
    # Peel/wrap utilities
    "TopLevelOperators",
    "extract_top_level_operators",
    "wrap_with_operators",
    # Rewriting utilities
    "EquivalenceChecker",
    "EquivalenceLevel",
    "EquivalenceResult",
    "PredicateClassification",
    "classify_predicates",
    "extract_selection_predicates",
    "rewrite_predicate_columns",
    "rewrite_on_predicate",
    "collect_column_references",
    "build_column_rewrite_map",
    "check_column_availability",
    "check_outer_join_safety",
    # Config
    "NormalizationConfig",
    "ProofConfig",
    # Folding
    "RAFolder",
    "ViewSpecification",
    "FoldResult",
    "FoldCoverage",
    "BoundaryAnalysis",
    "JoinStep",
    "SPJValidationResult",
    "FoldingConfig",
    "AGGREGATE_FUNCTION_NAMES",
    "analyze_boundary",
    "normalize_table_name",
    "check_view_is_spj",
    "validate_partial_fold",
    "check_correlated_subqueries_safe",
    # Naming utilities
    "DEFAULT_SUBQUERY_ALIAS",
    "DEFAULT_AGGREGATE_ALIAS",
    "DEFAULT_CONTRACT_VIEW_ALIAS",
    "CTE_ID_PREFIX",
    "SUBQUERY_ID_PREFIX",
    "effective_table_name",
    "generate_subquery_alias",
    "generate_column_alias",
    "normalize_cte_name",
    "generate_subquery_id",
    "generate_cte_id",
    "QualifiedName",
    "parse_parts",
    "normalize_name",
]
