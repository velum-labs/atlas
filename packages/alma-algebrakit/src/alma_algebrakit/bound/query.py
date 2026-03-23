"""Bound query types.

A BoundQuery represents a fully resolved query with all column references
bound to stable identifiers.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from alma_algebrakit.bound.types import (
    AttributeRef,
    BoundExpression,
    BoundLiteral,
    BoundPredicate,
)
from alma_algebrakit.schema.catalog import TableSchema
from alma_algebrakit.schema.types import DataType, SQLDataType
from alma_algebrakit.scope.instance import RelationInstance
from alma_algebrakit.scope.scope import Scope


class RelationBinding(BaseModel):
    """Binding information for a relation in scope.

    This is the bridge between RelationInstance (algebraic identity)
    and the column references that can be made from it.
    """

    alias: str = Field(description="Alias used in the query")
    table_id: str = Field(description="Resolved table/subquery identifier")
    columns: dict[str, AttributeRef] = Field(
        default_factory=dict,
        description="Available columns from this relation",
    )
    is_subquery: bool = Field(default=False)
    is_cte: bool = Field(default=False)
    is_lateral: bool = Field(default=False, description="LATERAL subquery can see outer columns")

    def get_column(self, name: str) -> AttributeRef | None:
        """Get a column by name (case-insensitive)."""
        name_lower = name.lower()
        for col_name, col_ref in self.columns.items():
            if col_name.lower() == name_lower:
                return col_ref
        return None

    @classmethod
    def from_instance(cls, instance: RelationInstance) -> RelationBinding:
        """Create a binding from a RelationInstance."""
        columns: dict[str, AttributeRef] = {}

        if instance.schema:
            for col in instance.schema.columns:
                columns[col.name] = AttributeRef(
                    table_id=instance.table_id,
                    column_id=col.column_id,
                    sql_name=col.name,
                    table_alias=instance.alias,
                    data_type=col.data_type,
                )

        return cls(
            alias=instance.alias,
            table_id=instance.table_id,
            columns=columns,
            is_subquery=instance.is_subquery,
            is_cte=instance.is_cte,
            is_lateral=instance.is_lateral,
        )

    @classmethod
    def from_table_schema(cls, schema: TableSchema, alias: str | None = None) -> RelationBinding:
        """Create a binding from a TableSchema.

        Args:
            schema: The table schema to create a binding from
            alias: Optional alias (defaults to table name)

        Returns:
            RelationBinding with columns populated from the schema
        """
        alias = alias or schema.name
        columns: dict[str, AttributeRef] = {}

        for col in schema.columns:
            columns[col.name] = AttributeRef(
                table_id=schema.table_id,
                column_id=col.column_id,
                sql_name=col.name,
                table_alias=alias,
                data_type=col.data_type,
            )

        return cls(
            alias=alias,
            table_id=schema.table_id,
            columns=columns,
            is_subquery=False,
            is_cte=False,
        )


class BoundSelectItem(BaseModel):
    """A bound SELECT list item."""

    expression: BoundExpression | AttributeRef | BoundLiteral | None = None
    alias: str | None = None
    is_star: bool = False
    star_table: str | None = None
    expanded_columns: list[AttributeRef] = Field(default_factory=list)


class BoundOrderItem(BaseModel):
    """A bound ORDER BY item."""

    expression: BoundExpression | AttributeRef = Field(...)
    descending: bool = False
    nulls_first: bool | None = None


class BoundQuery(BaseModel):
    """A fully bound query with all references resolved.

    This is the output of the binding process - a query where every
    column reference has been resolved to a stable AttributeRef.
    """

    columns: list[BoundSelectItem] = Field(default_factory=list)
    from_relations: list[RelationBinding] = Field(default_factory=list)
    where: BoundPredicate | None = None
    group_by: list[BoundExpression | AttributeRef] = Field(default_factory=list)
    having: BoundPredicate | None = None
    order_by: list[BoundOrderItem] = Field(default_factory=list)
    distinct: bool = False
    limit: int | None = None
    scope: Scope | None = None

    model_config = {"arbitrary_types_allowed": True}

    def all_upstream_columns(self) -> set[AttributeRef]:
        """Get all column references in the query."""
        cols: set[AttributeRef] = set()

        for item in self.columns:
            if item.expression and hasattr(item.expression, "upstream_columns"):
                cols.update(item.expression.upstream_columns)
            elif isinstance(item.expression, AttributeRef):
                cols.add(item.expression)
            cols.update(item.expanded_columns)

        if self.where:
            cols.update(self.where.upstream_columns)

        if self.having:
            cols.update(self.having.upstream_columns)

        for expr in self.group_by:
            if hasattr(expr, "upstream_columns"):
                cols.update(expr.upstream_columns)
            elif isinstance(expr, AttributeRef):
                cols.add(expr)

        return cols

    def referenced_tables(self) -> set[str]:
        """Get all table IDs referenced in the query."""
        return {rel.table_id for rel in self.from_relations}

    def output_schema(self) -> list[tuple[str, DataType]]:
        """Get the output schema of the query (column name, type)."""
        result: list[tuple[str, DataType]] = []

        for item in self.columns:
            if item.is_star:
                for col in item.expanded_columns:
                    result.append((col.sql_name, col.data_type))
            elif item.alias:
                expr_type = DataType(base_type=SQLDataType.UNKNOWN)
                if item.expression:
                    if hasattr(item.expression, "result_type"):
                        expr_type = item.expression.result_type
                    elif isinstance(item.expression, AttributeRef):
                        expr_type = item.expression.data_type
                result.append((item.alias, expr_type))
            elif isinstance(item.expression, AttributeRef):
                result.append((item.expression.sql_name, item.expression.data_type))
            else:
                result.append(("?", DataType(base_type=SQLDataType.UNKNOWN)))

        return result
