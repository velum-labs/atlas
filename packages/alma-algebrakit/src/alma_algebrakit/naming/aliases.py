"""Unified alias generation utilities.

Centralizes all alias generation patterns that were previously scattered
across algebrakit, sqlkit, and query-analyzer.
"""

from __future__ import annotations

# =============================================================================
# Default Alias Constants
# =============================================================================

DEFAULT_SUBQUERY_ALIAS = "_subquery"
"""Default alias for unnamed subqueries."""

DEFAULT_AGGREGATE_ALIAS = "agg"
"""Default alias for unnamed aggregate expressions."""

DEFAULT_CONTRACT_VIEW_ALIAS = "cv"
"""Default alias for contract views in rewritten queries."""

# =============================================================================
# ID Prefix Constants
# =============================================================================

SUBQUERY_ID_PREFIX = "_subq_"
"""Prefix for subquery table IDs."""

CTE_ID_PREFIX = "_cte_"
"""Prefix for CTE table IDs."""


# =============================================================================
# Alias Generation Functions
# =============================================================================


def effective_table_name(alias: str | None, table_name: str) -> str:
    """Get the effective table name (alias if present, else table name).

    Args:
        alias: Optional table alias
        table_name: Physical table name

    Returns:
        The alias if present, otherwise the table name

    Example:
        >>> effective_table_name("o", "orders")
        'o'
        >>> effective_table_name(None, "orders")
        'orders'
    """
    return alias or table_name


def generate_subquery_alias(existing_alias: str | None = None) -> str:
    """Generate an alias for a subquery.

    Args:
        existing_alias: Existing alias if any

    Returns:
        The existing alias if present, otherwise the default subquery alias

    Example:
        >>> generate_subquery_alias("sq1")
        'sq1'
        >>> generate_subquery_alias(None)
        '_subquery'
    """
    return existing_alias or DEFAULT_SUBQUERY_ALIAS


def generate_column_alias(index: int, existing_alias: str | None = None) -> str:
    """Generate an alias for a column based on its position.

    Args:
        index: Zero-based column index
        existing_alias: Existing alias if any

    Returns:
        The existing alias if present, otherwise a generated alias like '_col0'

    Example:
        >>> generate_column_alias(0, "user_id")
        'user_id'
        >>> generate_column_alias(2, None)
        '_col2'
    """
    return existing_alias or f"_col{index}"


def normalize_cte_name(name: str) -> str:
    """Normalize a CTE name for use in SQL generation.

    Replaces dots and hyphens with underscores.

    Args:
        name: Original CTE name

    Returns:
        Normalized name safe for SQL identifiers

    Example:
        >>> normalize_cte_name("my-cte.name")
        'my_cte_name'
    """
    return name.replace(".", "_").replace("-", "_")


def generate_subquery_id(alias: str) -> str:
    """Generate a stable ID for a subquery.

    Args:
        alias: Subquery alias

    Returns:
        ID with subquery prefix

    Example:
        >>> generate_subquery_id("sq1")
        '_subq_sq1'
    """
    return f"{SUBQUERY_ID_PREFIX}{alias}"


def generate_cte_id(cte_name: str) -> str:
    """Generate a stable ID for a CTE.

    Args:
        cte_name: CTE name

    Returns:
        ID with CTE prefix

    Example:
        >>> generate_cte_id("my_cte")
        '_cte_my_cte'
    """
    return f"{CTE_ID_PREFIX}{cte_name}"
