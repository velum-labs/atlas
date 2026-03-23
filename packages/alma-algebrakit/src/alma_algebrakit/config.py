"""Configuration types for algebrakit."""

from pydantic import BaseModel, Field


class NormalizationConfig(BaseModel):
    """Configuration for RA expression normalization."""

    merge_selections: bool = Field(
        default=True,
        description="Merge cascading selections into one",
    )
    flatten_joins: bool = Field(
        default=True,
        description="Flatten nested joins (for inner joins only)",
    )
    canonicalize_join_order: bool = Field(
        default=True,
        description="Sort joins by table name for canonical form",
    )


class ProofConfig(BaseModel):
    """Configuration for proof checking."""

    use_smt: bool = Field(
        default=True,
        description="Use SMT solver (z3) for proof checking",
    )
    smt_timeout_ms: int = Field(
        default=5000,
        ge=100,
        le=60000,
        description="Timeout for SMT solver in milliseconds",
    )
    cache_size: int = Field(
        default=10000,
        ge=100,
        le=1000000,
        description="Maximum number of implication results to cache (LRU eviction)",
    )
    use_empirical: bool = Field(
        default=False,
        description="Use empirical validation with test data",
    )
    empirical_sample_size: int = Field(
        default=100,
        ge=10,
        le=10000,
        description="Number of rows to sample for empirical validation",
    )
