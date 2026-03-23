"""Unified qualified name parsing and handling.

Provides consistent parsing of qualified names like:
- "column" (unqualified)
- "table.column" (2-part)
- "schema.table.column" (3-part)
- "catalog.schema.table.column" (4-part)
"""

from __future__ import annotations

from dataclasses import dataclass


def parse_parts(name: str) -> tuple[str | None, str | None, str | None, str]:
    """Parse a dotted name into (catalog, schema, table, name) parts.

    Handles 1 to 4 part qualified names.

    Args:
        name: Dotted name string

    Returns:
        Tuple of (catalog, schema, table, name) where missing parts are None

    Examples:
        >>> parse_parts("id")
        (None, None, None, 'id')
        >>> parse_parts("users.id")
        (None, None, 'users', 'id')
        >>> parse_parts("public.users.id")
        (None, 'public', 'users', 'id')
        >>> parse_parts("mydb.public.users.id")
        ('mydb', 'public', 'users', 'id')
    """
    parts = name.split(".")

    if len(parts) == 4:
        return (parts[0], parts[1], parts[2], parts[3])
    if len(parts) == 3:
        return (None, parts[0], parts[1], parts[2])
    if len(parts) == 2:
        return (None, None, parts[0], parts[1])
    return (None, None, None, parts[0])


def normalize_name(name: str) -> str:
    """Normalize a name for case-insensitive comparison.

    Lowercases and strips surrounding quotes.

    Args:
        name: Name to normalize

    Returns:
        Normalized name

    Examples:
        >>> normalize_name("Users")
        'users'
        >>> normalize_name('"MyTable"')
        'mytable'
    """
    name = name.strip().lower()
    if name.startswith('"') and name.endswith('"') and len(name) > 1:
        name = name[1:-1]
    return name


@dataclass(frozen=True)
class QualifiedName:
    """A fully-qualified database object name.

    Supports catalog.schema.table.column format with proper comparison.
    """

    catalog: str | None = None
    schema: str | None = None
    table: str | None = None
    name: str = ""

    @classmethod
    def parse(cls, dotted_name: str) -> QualifiedName:
        """Parse a dotted name string into a QualifiedName.

        Args:
            dotted_name: Name like "schema.table.column"

        Returns:
            QualifiedName instance
        """
        catalog, schema, table, name = parse_parts(dotted_name)
        return cls(catalog=catalog, schema=schema, table=table, name=name)

    def to_string(self) -> str:
        """Convert back to dotted string notation."""
        parts = []
        if self.catalog:
            parts.append(self.catalog)
        if self.schema:
            parts.append(self.schema)
        if self.table:
            parts.append(self.table)
        parts.append(self.name)
        return ".".join(parts)

    def with_table(self, table: str | None) -> QualifiedName:
        """Return a copy with a different table."""
        return QualifiedName(
            catalog=self.catalog,
            schema=self.schema,
            table=table,
            name=self.name,
        )

    def with_schema(self, schema: str | None) -> QualifiedName:
        """Return a copy with a different schema."""
        return QualifiedName(
            catalog=self.catalog,
            schema=schema,
            table=self.table,
            name=self.name,
        )

    def matches(self, other: QualifiedName, ignore_case: bool = True) -> bool:
        """Check if this name matches another, considering partial qualification.

        A name matches if all specified parts match. Unspecified parts (None)
        are treated as wildcards.

        Args:
            other: Name to compare against
            ignore_case: Whether to ignore case differences

        Returns:
            True if names match
        """

        def _compare(a: str | None, b: str | None) -> bool:
            if a is None or b is None:
                return True
            if ignore_case:
                return normalize_name(a) == normalize_name(b)
            return a == b

        return (
            _compare(self.catalog, other.catalog)
            and _compare(self.schema, other.schema)
            and _compare(self.table, other.table)
            and _compare(self.name, other.name)
        )

    @property
    def qualified_table(self) -> str | None:
        """Get the fully qualified table name (schema.table)."""
        if self.schema and self.table:
            return f"{self.schema}.{self.table}"
        return self.table

    @property
    def qualified_column(self) -> str:
        """Get the qualified column reference (table.column or just column)."""
        if self.table:
            return f"{self.table}.{self.name}"
        return self.name

    def __str__(self) -> str:
        return self.to_string()
