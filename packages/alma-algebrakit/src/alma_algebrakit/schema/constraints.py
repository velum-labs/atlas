"""Schema constraints for extended query rewriting.

This module provides representations of database constraints (PK, FK, NOT NULL, UNIQUE)
that enable more sophisticated reasoning about query equivalence and safe rewrites.

Theoretical Foundation:
    - Self-join rewriting requires key preservation (PK/UNIQUE)
    - Outer join → Inner join inference requires FK + NOT NULL
    - Predicate implication can leverage constraint knowledge
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PrimaryKey:
    """Primary key constraint.

    A primary key uniquely identifies rows and implies NOT NULL on all columns.
    """

    table: str
    columns: frozenset[str]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("Primary key must have at least one column")

    def covers(self, columns: frozenset[str]) -> bool:
        """Check if this PK covers (is a subset of) the given columns."""
        return self.columns <= columns

    def matches(self, columns: frozenset[str]) -> bool:
        """Check if this PK exactly matches the given columns."""
        return self.columns == columns


@dataclass(frozen=True)
class ForeignKey:
    """Foreign key constraint.

    Represents a referential integrity constraint: from_table.from_columns -> to_table.to_columns.

    The FK guarantees that every non-NULL value in from_columns exists in to_columns.
    """

    from_table: str
    from_columns: tuple[str, ...]
    to_table: str
    to_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.from_columns) != len(self.to_columns):
            raise ValueError("FK column counts must match")
        if not self.from_columns:
            raise ValueError("FK must have at least one column")

    def references(self, from_table: str, from_col: str, to_table: str) -> bool:
        """Check if this FK references from from_table.from_col to to_table."""
        if self.from_table.lower() != from_table.lower():
            return False
        if self.to_table.lower() != to_table.lower():
            return False
        return from_col.lower() in (c.lower() for c in self.from_columns)

    def get_referenced_column(self, from_col: str) -> str | None:
        """Get the to_column that corresponds to from_col."""
        for i, fc in enumerate(self.from_columns):
            if fc.lower() == from_col.lower():
                return self.to_columns[i]
        return None


@dataclass(frozen=True)
class UniqueConstraint:
    """Unique constraint.

    Guarantees that the combination of columns is unique across all rows.
    Unlike PK, allows NULL values (though NULL != NULL in SQL).
    """

    table: str
    columns: frozenset[str]
    name: str | None = None

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("Unique constraint must have at least one column")

    def covers(self, columns: frozenset[str]) -> bool:
        """Check if this unique constraint covers the given columns."""
        return self.columns <= columns


@dataclass
class SchemaConstraints:
    """Collection of schema constraints for a database.

    This class aggregates all constraint information needed for extended
    query rewriting, including:

    - Primary keys: For key-preserving view analysis
    - Foreign keys: For outer join → inner join inference
    - NOT NULL: For null-extension safety analysis
    - Unique: For additional key analysis

    Theorems supported:

    1. Self-Join Rewriting: Safe iff view is key-preserving.
       A view V over table T is key-preserving if V's projection includes T's PK.

    2. Outer Join Inference: L LEFT JOIN R ≡ L INNER JOIN R if:
       - R.join_col IS NOT NULL
       - FK: L.join_col → R.join_col exists

    3. Functional Dependencies: PK → all other columns (for aggregation reasoning)
    """

    primary_keys: dict[str, PrimaryKey] = field(default_factory=dict)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    not_null: set[tuple[str, str]] = field(default_factory=set)
    unique_constraints: list[UniqueConstraint] = field(default_factory=list)

    def get_pk(self, table: str) -> PrimaryKey | None:
        """Get the primary key for a table."""
        return self.primary_keys.get(table.lower())

    def add_pk(self, pk: PrimaryKey) -> None:
        """Add a primary key constraint."""
        self.primary_keys[pk.table.lower()] = pk
        # PK columns are implicitly NOT NULL
        for col in pk.columns:
            self.not_null.add((pk.table.lower(), col.lower()))

    def add_fk(self, fk: ForeignKey) -> None:
        """Add a foreign key constraint."""
        self.foreign_keys.append(fk)

    def add_not_null(self, table: str, column: str) -> None:
        """Add a NOT NULL constraint."""
        self.not_null.add((table.lower(), column.lower()))

    def add_unique(self, unique: UniqueConstraint) -> None:
        """Add a unique constraint."""
        self.unique_constraints.append(unique)

    def has_fk(self, from_table: str, from_col: str, to_table: str) -> bool:
        """Check if a foreign key exists from from_table.from_col to to_table."""
        return any(fk.references(from_table, from_col, to_table) for fk in self.foreign_keys)

    def get_fk(self, from_table: str, from_col: str, to_table: str) -> ForeignKey | None:
        """Get the FK constraint if it exists."""
        for fk in self.foreign_keys:
            if fk.references(from_table, from_col, to_table):
                return fk
        return None

    def is_not_null(self, table: str, column: str) -> bool:
        """Check if a column has NOT NULL constraint."""
        return (table.lower(), column.lower()) in self.not_null

    def get_keys(self, table: str) -> list[frozenset[str]]:
        """Get all keys (PK + UNIQUE) for a table.

        Returns list of column sets that uniquely identify rows.
        """
        keys: list[frozenset[str]] = []

        pk = self.get_pk(table)
        if pk:
            keys.append(pk.columns)

        for unique in self.unique_constraints:
            if unique.table.lower() == table.lower():
                keys.append(unique.columns)

        return keys

    def is_key_preserving(
        self,
        base_table: str,
        projected_columns: set[str],
    ) -> bool:
        """Check if a projection preserves any key of the base table.

        A view/projection is key-preserving if it includes all columns
        of at least one key (PK or UNIQUE) of the base table.

        This is the key theorem for self-join rewriting safety:
        - If V is key-preserving for T, then T t1 JOIN T t2 can be
          rewritten as V v1 JOIN V v2 without changing semantics.

        Args:
            base_table: The table to check keys for
            projected_columns: Columns included in the view/projection

        Returns:
            True if at least one key is fully contained in projected_columns
        """
        projected_lower = {c.lower() for c in projected_columns}

        for key in self.get_keys(base_table):
            key_lower = {c.lower() for c in key}
            if key_lower <= projected_lower:
                return True

        return False

    def can_infer_inner_join(
        self,
        left_table: str,
        left_col: str,
        right_table: str,
        right_col: str,
    ) -> tuple[bool, str]:
        """Check if LEFT JOIN can be inferred as INNER JOIN.

        Theorem: L LEFT JOIN R ON L.a = R.b ≡ L INNER JOIN R ON L.a = R.b if:
        1. R.b IS NOT NULL (no nulls to match against)
        2. FK: L.a → R.b exists (every L.a value has a match in R.b)

        Args:
            left_table: Left side of the join
            left_col: Join column from left side
            right_table: Right side of the join (null-extended side)
            right_col: Join column from right side

        Returns:
            (can_infer, reason) tuple
        """
        # Check condition 1: R.b IS NOT NULL
        if not self.is_not_null(right_table, right_col):
            return False, f"{right_table}.{right_col} is not NOT NULL"

        # Check condition 2: FK L.a → R.b
        if not self.has_fk(left_table, left_col, right_table):
            return False, f"No FK from {left_table}.{left_col} to {right_table}"

        return True, "FK + NOT NULL allows INNER JOIN inference"

    def can_infer_inner_from_right_join(
        self,
        left_table: str,
        left_col: str,
        right_table: str,
        right_col: str,
    ) -> tuple[bool, str]:
        """Check if RIGHT JOIN can be inferred as INNER JOIN.

        For RIGHT JOIN, the left side is null-extended, so we need:
        1. L.a IS NOT NULL
        2. FK: R.b → L.a exists (every R.b value has a match in L.a)
        """
        # Check L.a IS NOT NULL
        if not self.is_not_null(left_table, left_col):
            return False, f"{left_table}.{left_col} is not NOT NULL"

        # Check FK R.b → L.a
        if not self.has_fk(right_table, right_col, left_table):
            return False, f"No FK from {right_table}.{right_col} to {left_table}"

        return True, "FK + NOT NULL allows INNER JOIN inference"

    @classmethod
    def from_catalog_metadata(
        cls,
        primary_keys: dict[str, list[str]] | None = None,
        foreign_keys: list[dict[str, str | list[str]]] | None = None,
        not_null_columns: list[tuple[str, str]] | None = None,
    ) -> SchemaConstraints:
        """Create SchemaConstraints from catalog metadata.

        Convenience factory for creating constraints from common metadata formats.

        Args:
            primary_keys: {table: [col1, col2, ...]}
            foreign_keys: [{from_table, from_columns, to_table, to_columns}, ...]
            not_null_columns: [(table, column), ...]

        Returns:
            SchemaConstraints instance
        """
        constraints = cls()

        if primary_keys:
            for table, columns in primary_keys.items():
                pk = PrimaryKey(table=table, columns=frozenset(columns))
                constraints.add_pk(pk)

        if foreign_keys:
            for fk_dict in foreign_keys:
                from_cols = fk_dict["from_columns"]
                to_cols = fk_dict["to_columns"]
                if isinstance(from_cols, str):
                    from_cols = [from_cols]
                if isinstance(to_cols, str):
                    to_cols = [to_cols]
                fk = ForeignKey(
                    from_table=str(fk_dict["from_table"]),
                    from_columns=tuple(from_cols),
                    to_table=str(fk_dict["to_table"]),
                    to_columns=tuple(to_cols),
                )
                constraints.add_fk(fk)

        if not_null_columns:
            for table, column in not_null_columns:
                constraints.add_not_null(table, column)

        return constraints
