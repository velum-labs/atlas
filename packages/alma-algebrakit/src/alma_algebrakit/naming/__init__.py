"""Naming utilities for consistent alias and qualified name handling.

This module provides centralized utilities for:
- Alias generation (table, column, subquery, CTE)
- Qualified name parsing and normalization
"""

from alma_algebrakit.naming.aliases import (
    CTE_ID_PREFIX,
    DEFAULT_AGGREGATE_ALIAS,
    DEFAULT_CONTRACT_VIEW_ALIAS,
    DEFAULT_SUBQUERY_ALIAS,
    SUBQUERY_ID_PREFIX,
    effective_table_name,
    generate_column_alias,
    generate_cte_id,
    generate_subquery_alias,
    generate_subquery_id,
    normalize_cte_name,
)
from alma_algebrakit.naming.qualified import (
    QualifiedName,
    normalize_name,
    parse_parts,
)

__all__ = [
    # Alias constants
    "DEFAULT_SUBQUERY_ALIAS",
    "DEFAULT_AGGREGATE_ALIAS",
    "DEFAULT_CONTRACT_VIEW_ALIAS",
    "CTE_ID_PREFIX",
    "SUBQUERY_ID_PREFIX",
    # Alias functions
    "effective_table_name",
    "generate_subquery_alias",
    "generate_column_alias",
    "normalize_cte_name",
    "generate_subquery_id",
    "generate_cte_id",
    # Qualified name utilities
    "QualifiedName",
    "parse_parts",
    "normalize_name",
]
