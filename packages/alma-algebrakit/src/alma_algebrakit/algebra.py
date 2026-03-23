"""SQL algebraic reduction for alma-algebrakit.

Reduces a parsed SQL expression to its relational algebra representation.
This is the intermediate form used before hashing into a fingerprint.

The algebraic form captures:
- The set of source relations (tables/views) referenced
- The join graph (which relations are joined and on what conditions)
- The filter predicates (WHERE/HAVING clauses, structurally)
- The projection (which columns are selected, by position not name)

Literal values and column aliases are stripped so that semantically
equivalent queries produce identical algebra strings.
"""

from __future__ import annotations

from alma_sqlkit.dialect import Dialect
from alma_sqlkit.normalize import normalize_sql


def to_algebra(sql: str, dialect: Dialect | str = Dialect.ANSI) -> str:
    """Reduce a SQL string to its algebraic normal form.

    This is a best-effort reduction: complex window functions, CTEs, and
    subqueries are included but may not be fully normalized. The output is
    a deterministic string representation suitable for fingerprinting.

    Args:
        sql: Raw SQL string.
        dialect: SQL dialect for parsing.

    Returns:
        Algebraic normal form as a string.
    """
    # Phase 1: normalize literals and formatting
    normalized = normalize_sql(sql, dialect=dialect)
    # Phase 2: further structural normalization will be implemented here
    # (join reordering, predicate canonicalization, etc.)
    return normalized
