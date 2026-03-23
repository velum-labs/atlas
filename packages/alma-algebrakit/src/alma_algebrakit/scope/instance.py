"""Relation instance for algebraic scoping.

A RelationInstance represents a specific occurrence of a relation in a query.
This handles cases like self-joins where the same table appears multiple times
with different aliases.
"""

from __future__ import annotations

from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from alma_algebrakit.schema.catalog import TableSchema


class RelationInstance(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    """A specific occurrence of a relation in a query scope.

    Unlike a table name or alias (which are strings), a RelationInstance
    provides a unique identity for each occurrence of a relation. This is
    essential for handling:

    - Self-joins: `orders o1 JOIN orders o2` creates two RelationInstances
    - Subqueries: Each subquery creates its own set of instances
    - CTEs: CTE references are distinct from the underlying table

    This is an algebraic concept that exists in any query language:
    - SQL: `FROM orders o`
    - Datalog: `orders(X, Y)` binds variables to a specific relation occurrence
    - DataFrames: `df.alias("o")` creates a named reference

    Attributes:
        id: Unique identifier for this instance
        table_id: The underlying table/relation identifier
        alias: The alias used to reference this instance (may equal table name)
        schema: Optional schema information for the relation
        is_subquery: True if this is a derived table (subquery)
        is_cte: True if this is a CTE reference
        is_lateral: True if this is a LATERAL subquery (can see outer columns)
    """

    id: UUID = Field(default_factory=uuid4, description="Unique instance identifier")
    table_id: str = Field(description="Underlying table identifier")
    alias: str = Field(description="Alias used in query")
    schema: TableSchema | None = Field(default=None, description="Table schema if known")
    is_subquery: bool = Field(default=False, description="Is this a derived table")
    is_cte: bool = Field(default=False, description="Is this a CTE reference")
    is_lateral: bool = Field(default=False, description="Is this a LATERAL subquery")

    def effective_name(self) -> str:
        """Return the name used to qualify columns."""
        return self.alias

    def column_names(self) -> list[str]:
        """Get column names if schema is available."""
        if self.schema:
            return self.schema.column_names()
        return []

    def has_column(self, name: str) -> bool:
        """Check if this relation has a column with the given name."""
        if not self.schema:
            # Without schema info, we can't verify
            return True
        return self.schema.get_column(name) is not None

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RelationInstance):
            return False
        return self.id == other.id

    @classmethod
    def from_table_schema(cls, schema: TableSchema, alias: str | None = None) -> RelationInstance:
        """Create a relation instance from a table schema."""
        return cls(
            table_id=schema.table_id,
            alias=alias or schema.name,
            schema=schema,
        )
