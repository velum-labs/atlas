"""Pytest configuration and fixtures for algebrakit tests."""

import pytest

from algebrakit import (
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    Join,
    JoinType,
    Relation,
)
from alma_algebrakit.schema import (
    Catalog,
    ColumnSchema,
    DataType,
    SQLDataType,
    TableSchema,
)


@pytest.fixture
def sample_catalog() -> Catalog:
    """Create a sample catalog for testing."""
    catalog = Catalog()

    # Users table
    catalog.add_table(
        TableSchema(
            name="users",
            table_id="public.users",
            schema_name="public",
            columns=[
                ColumnSchema(
                    name="id",
                    column_id="id",
                    data_type=DataType(base_type=SQLDataType.INTEGER, nullable=False),
                    is_primary_key=True,
                ),
                ColumnSchema(
                    name="name",
                    column_id="name",
                    data_type=DataType(base_type=SQLDataType.TEXT),
                ),
                ColumnSchema(
                    name="email",
                    column_id="email",
                    data_type=DataType(base_type=SQLDataType.TEXT),
                ),
            ],
        )
    )

    # Orders table
    catalog.add_table(
        TableSchema(
            name="orders",
            table_id="public.orders",
            schema_name="public",
            columns=[
                ColumnSchema(
                    name="id",
                    column_id="id",
                    data_type=DataType(base_type=SQLDataType.INTEGER, nullable=False),
                    is_primary_key=True,
                ),
                ColumnSchema(
                    name="user_id",
                    column_id="user_id",
                    data_type=DataType(base_type=SQLDataType.INTEGER),
                    is_foreign_key=True,
                    foreign_key_target="public.users.id",
                ),
                ColumnSchema(
                    name="total",
                    column_id="total",
                    data_type=DataType(base_type=SQLDataType.DECIMAL),
                ),
                ColumnSchema(
                    name="created_at",
                    column_id="created_at",
                    data_type=DataType(base_type=SQLDataType.TIMESTAMP),
                ),
            ],
        )
    )

    return catalog


@pytest.fixture
def simple_relation() -> Relation:
    """Create a simple relation for testing."""
    return Relation(name="users", alias="u")


@pytest.fixture
def simple_join() -> Join:
    """Create a simple join for testing."""
    users = Relation(name="users", alias="u")
    orders = Relation(name="orders", alias="o")

    return Join(
        left=users,
        right=orders,
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        ),
    )
