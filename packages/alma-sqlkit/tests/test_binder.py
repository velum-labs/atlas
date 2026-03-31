from __future__ import annotations

from alma_algebrakit import AttributeRef, BoundComparison, Catalog

from alma_sqlkit import SQLBinder


def _catalog() -> Catalog:
    return Catalog.from_dict(
        {
            "orders": [
                {"name": "id", "type": "integer"},
                {"name": "customer_id", "type": "integer"},
            ],
        }
    )


def test_bind_subquery_exposes_derived_columns_to_outer_scope() -> None:
    binder = SQLBinder(_catalog())

    bound = binder.bind(
        """
        SELECT sq.id
        FROM (SELECT id FROM orders) AS sq
        """
    )

    assert bound.from_relations[0].alias == "sq"
    assert bound.from_relations[0].get_column("id") is not None
    assert isinstance(bound.columns[0].expression, AttributeRef)
    assert bound.columns[0].expression.table_alias == "sq"


def test_bind_cte_exposes_cte_columns_to_main_query() -> None:
    binder = SQLBinder(_catalog())

    bound = binder.bind(
        """
        WITH recent_orders AS (
            SELECT id FROM orders
        )
        SELECT recent_orders.id
        FROM recent_orders
        """
    )

    assert bound.from_relations[0].alias == "recent_orders"
    assert bound.from_relations[0].get_column("id") is not None
    assert isinstance(bound.columns[0].expression, AttributeRef)
    assert bound.columns[0].expression.table_alias == "recent_orders"


def test_bind_uses_outer_where_instead_of_nested_subquery_where() -> None:
    binder = SQLBinder(_catalog())

    bound = binder.bind(
        """
        SELECT sq.id
        FROM (
            SELECT id, customer_id
            FROM orders
            WHERE customer_id > 10
            ORDER BY id
            LIMIT 5
        ) AS sq
        WHERE sq.id = 1
        """
    )

    assert bound.where is not None
    assert isinstance(bound.where, BoundComparison)
    assert isinstance(bound.where.left, AttributeRef)
    assert bound.where.left.table_alias == "sq"
