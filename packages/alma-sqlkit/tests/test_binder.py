from __future__ import annotations

from alma_algebrakit import AttributeRef, Catalog
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
