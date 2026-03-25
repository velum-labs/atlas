"""SQL identifier quoting utilities for database adapters.

These functions replace regex-based "validation" with proper, database-native
identifier quoting so that user-supplied names cannot break out of a SQL
statement regardless of their content.
"""

from __future__ import annotations


def quote_bq_identifier(value: str) -> str:
    """Return *value* quoted as a BigQuery identifier (backtick-style).

    BigQuery uses backticks to delimit identifiers.  Any literal backtick
    inside *value* is escaped with a preceding backslash.

    Args:
        value: Raw identifier string (e.g. a project ID or region name).

    Returns:
        The identifier wrapped in backticks, e.g. `` `my-project` ``.

    Raises:
        ValueError: If *value* is empty.
    """
    if not value:
        raise ValueError("SQL identifier must not be empty")
    escaped = value.replace("`", "\\`")
    return f"`{escaped}`"


def quote_sf_identifier(value: str) -> str:
    """Return *value* quoted as a Snowflake identifier (double-quote style).

    Snowflake follows the SQL standard: identifiers are delimited with double
    quotes, and a literal ``"`` inside the identifier is represented as ``""``.

    Args:
        value: Raw identifier string (e.g. a database or schema name).

    Returns:
        The identifier wrapped in double quotes, e.g. ``"my_db"``.

    Raises:
        ValueError: If *value* is empty.
    """
    if not value:
        raise ValueError("SQL identifier must not be empty")
    escaped = value.replace('"', '""')
    return f'"{escaped}"'
