"""Configuration for RA-level query folding."""

from __future__ import annotations

from pydantic import BaseModel, Field

# Default aggregate function names for SPJ validation
AGGREGATE_FUNCTION_NAMES: frozenset[str] = frozenset(
    {
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "array_agg",
        "string_agg",
        "bool_and",
        "bool_or",
        "stddev",
        "variance",
        "covar_pop",
        "covar_samp",
        "corr",
        "percentile_cont",
        "percentile_disc",
        "mode",
        "regr_avgx",
        "regr_avgy",
        "regr_count",
        "regr_intercept",
        "regr_r2",
        "regr_slope",
        "regr_sxx",
        "regr_sxy",
        "regr_syy",
    }
)


class FoldingConfig(BaseModel):
    """Configuration for RA-level folding operations."""

    check_predicate_implication: bool = Field(
        default=True,
        description="Whether to use SMT to check predicate implication for dropping redundant predicates",
    )
    strict_predicate_check: bool = Field(
        default=False,
        description="Whether to reject folds when query predicates are not implied by view predicates",
    )
    min_attribute_coverage: float = Field(
        default=0.5,
        description="Minimum fraction of query attributes that must be covered for a view to be considered",
    )
    smt_timeout_ms: int = Field(
        default=1000,
        description="Timeout for SMT predicate implication checks",
    )
    smt_timeout_for_classification_ms: int = Field(
        default=500,
        description="Shorter timeout for predicate classification during boundary analysis",
    )
    allow_partial_coverage: bool = Field(
        default=True,
        description="Whether to allow partial coverage rewrites",
    )
    aggregate_function_names: frozenset[str] = Field(
        default=AGGREGATE_FUNCTION_NAMES,
        description="Set of function names considered as aggregates for SPJ validation",
    )
    use_smt: bool = Field(
        default=True,
        description="Whether to use SMT solver for predicate implication checks",
    )

    # Extended folding features (decidable extensions)
    allow_self_join_rewriting: bool = Field(
        default=True,
        description=(
            "Enable self-join rewriting with key constraint verification. "
            "When True and schema_constraints are provided, self-joins on covered "
            "tables can be rewritten if the view is key-preserving."
        ),
    )
    use_constraint_inference: bool = Field(
        default=True,
        description=(
            "Use FK/NOT NULL constraints to infer outer join safety. "
            "When True, LEFT/RIGHT JOINs with FK + NOT NULL can be treated as INNER JOINs."
        ),
    )
    auto_decorrelate: bool = Field(
        default=True,
        description=(
            "Automatically decorrelate subqueries before folding. "
            "Transforms EXISTS/IN subqueries to SEMI/ANTI joins when possible."
        ),
    )
    use_linear_arithmetic: bool = Field(
        default=True,
        description=(
            "Use Fourier-Motzkin elimination for complete linear predicate implication. "
            "This provides definitive results for linear arithmetic predicates."
        ),
    )

    model_config = {"frozen": True}
