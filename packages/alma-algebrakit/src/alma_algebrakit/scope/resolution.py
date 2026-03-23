"""Attribute resolution in algebraic scopes.

This module provides the core resolution algorithm for finding which
attribute (column) a reference refers to, given a scope.

The resolution algorithm is language-agnostic - it works for:
- SQL: `SELECT o.id FROM orders o`
- Datalog: `query(X) :- orders(X, Y)`
- DataFrames: `df.select("id")`
"""

from __future__ import annotations

from alma_algebrakit.bound.types import AttributeRef
from alma_algebrakit.exceptions import AmbiguousColumnError, UnresolvedColumnError
from alma_algebrakit.schema.types import DataType, SQLDataType
from alma_algebrakit.scope.instance import RelationInstance
from alma_algebrakit.scope.scope import Scope


def resolve(
    scope: Scope,
    column_name: str,
    table_alias: str | None = None,
) -> AttributeRef:
    """Resolve a column reference to an AttributeRef.

    This is the core resolution function that maps a (possibly unqualified)
    column name to a fully resolved AttributeRef with stable identifiers.

    Args:
        scope: The current binding scope
        column_name: The column name to resolve
        table_alias: Optional table qualifier (e.g., "o" in "o.id")

    Returns:
        AttributeRef with stable identifiers

    Raises:
        AmbiguousColumnError: If unqualified reference matches multiple relations
        UnresolvedColumnError: If column cannot be found

    Example:
        >>> scope = Scope()
        >>> scope.add_relation(orders_instance)
        >>> ref = resolve(scope, "id", "o")
        >>> print(ref.qualified_id())
        "public.orders.id"
    """
    if table_alias:
        # Qualified reference: look up specific relation
        return _resolve_qualified(scope, table_alias, column_name)
    # Unqualified reference: search all relations
    return _resolve_unqualified(scope, column_name)


def _resolve_qualified(
    scope: Scope,
    table_alias: str,
    column_name: str,
) -> AttributeRef:
    """Resolve a qualified column reference (e.g., o.id)."""
    instance = scope.get_relation(table_alias)
    if not instance:
        available = list(scope.all_relations().keys())
        raise UnresolvedColumnError(
            f"{table_alias}.{column_name}",
            available_relations=available,
            message=f"Relation '{table_alias}' not found in scope",
        )

    # Get column schema if available
    data_type = DataType(base_type=SQLDataType.UNKNOWN)
    column_id = column_name

    if instance.schema:
        col_schema = instance.schema.get_column(column_name)
        if not col_schema:
            available_cols = instance.schema.column_names() if instance.schema else []
            raise UnresolvedColumnError(
                column_name,
                available_relations=[table_alias],
                message=f"Column '{column_name}' not found in '{table_alias}'. Available columns: {', '.join(available_cols)}",
            )
        data_type = col_schema.data_type
        column_id = col_schema.column_id

    return AttributeRef(
        table_id=instance.table_id,
        column_id=column_id,
        sql_name=column_name,
        table_alias=table_alias,
        data_type=data_type,
    )


def _resolve_unqualified(
    scope: Scope,
    column_name: str,
) -> AttributeRef:
    """Resolve an unqualified column reference by searching all relations."""
    matches = scope.find_attribute_in_relations(column_name)

    if len(matches) == 0:
        available = list(scope.all_relations().keys())
        raise UnresolvedColumnError(column_name, available_relations=available)

    if len(matches) > 1:
        aliases = [alias for alias, _ in matches]
        raise AmbiguousColumnError(column_name, aliases)

    alias, instance = matches[0]

    # Get column schema if available
    data_type = DataType(base_type=SQLDataType.UNKNOWN)
    column_id = column_name

    if instance.schema:
        col_schema = instance.schema.get_column(column_name)
        if col_schema:
            data_type = col_schema.data_type
            column_id = col_schema.column_id

    return AttributeRef(
        table_id=instance.table_id,
        column_id=column_id,
        sql_name=column_name,
        table_alias=alias if not alias.startswith("_outer.") else alias[7:],
        data_type=data_type,
    )


def resolve_star(scope: Scope, table_alias: str | None = None) -> list[AttributeRef]:
    """Resolve a star expression (SELECT * or SELECT t.*).

    Args:
        scope: The current binding scope
        table_alias: Optional table qualifier for t.*

    Returns:
        List of AttributeRef for all columns

    Raises:
        UnresolvedColumnError: If table_alias is specified but not found
    """
    if table_alias:
        instance = scope.get_relation(table_alias)
        if not instance:
            raise UnresolvedColumnError("*", table_alias)

        return _get_instance_columns(instance, table_alias)

    # SELECT * - get all columns from all relations
    refs: list[AttributeRef] = []
    for alias, instance in scope.all_relations().items():
        refs.extend(_get_instance_columns(instance, alias))

    return refs


def _get_instance_columns(instance: RelationInstance, alias: str) -> list[AttributeRef]:
    """Get all column references for a relation instance."""
    refs: list[AttributeRef] = []

    if instance.schema:
        for col in instance.schema.columns:
            refs.append(
                AttributeRef(
                    table_id=instance.table_id,
                    column_id=col.column_id,
                    sql_name=col.name,
                    table_alias=alias,
                    data_type=col.data_type,
                )
            )

    return refs
