"""SQL fingerprinting for alma-algebrakit.

Produces a stable, short hexadecimal fingerprint for a SQL query by:
1. Reducing it to algebraic normal form (via ``to_algebra``)
2. Hashing the result with SHA-256 (truncated to 16 hex chars)

The fingerprint is stable across: formatting changes, alias renames,
literal value changes, and whitespace differences. It changes when the
structural query logic changes (different tables, joins, or filters).
"""

from __future__ import annotations

import hashlib

from alma_algebrakit.algebra import to_algebra
from alma_sqlkit.dialect import Dialect


def fingerprint_sql(sql: str, dialect: Dialect | str = Dialect.ANSI) -> str:
    """Compute a stable fingerprint for a SQL string.

    Args:
        sql: Raw SQL string.
        dialect: SQL dialect for parsing.

    Returns:
        16-character hex fingerprint string (64-bit truncated SHA-256).
    """
    algebra = to_algebra(sql, dialect=dialect)
    digest = hashlib.sha256(algebra.encode("utf-8")).hexdigest()
    return digest[:16]
