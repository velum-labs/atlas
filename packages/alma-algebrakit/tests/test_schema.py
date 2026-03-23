"""Tests for algebrakit schema types."""

from algebrakit import (
    Attribute,
    Catalog,
    ColumnSchema,
    DataType,
    SQLDataType,
    TableSchema,
)


class TestDataType:
    """Tests for DataType."""

    def test_create_integer_type(self):
        dt = DataType(base_type=SQLDataType.INTEGER, nullable=False)
        assert dt.base_type == SQLDataType.INTEGER
        assert not dt.nullable
        assert dt.is_comparable_to(DataType(base_type=SQLDataType.INTEGER))

    def test_nullable_type(self):
        dt = DataType(base_type=SQLDataType.TEXT, nullable=True)
        assert dt.nullable

    def test_numeric_types_comparable(self):
        int_type = DataType(base_type=SQLDataType.INTEGER)
        decimal_type = DataType(base_type=SQLDataType.DECIMAL)
        assert int_type.is_comparable_to(decimal_type)

    def test_unknown_comparable_to_all(self):
        unknown = DataType(base_type=SQLDataType.UNKNOWN)
        int_type = DataType(base_type=SQLDataType.INTEGER)
        text_type = DataType(base_type=SQLDataType.TEXT)
        assert unknown.is_comparable_to(int_type)
        assert unknown.is_comparable_to(text_type)


class TestColumnSchema:
    """Tests for ColumnSchema."""

    def test_create_column_schema(self):
        cs = ColumnSchema(
            name="id",
            column_id="public.users.id",
            data_type=DataType(base_type=SQLDataType.INTEGER),
        )
        assert cs.name == "id"
        assert cs.column_id == "public.users.id"


class TestTableSchema:
    """Tests for TableSchema."""

    def test_create_table_schema(self):
        ts = TableSchema(
            name="users",
            table_id="public.users",
            columns=[
                ColumnSchema(
                    name="id",
                    column_id="public.users.id",
                    data_type=DataType(base_type=SQLDataType.INTEGER),
                ),
                ColumnSchema(
                    name="name",
                    column_id="public.users.name",
                    data_type=DataType(base_type=SQLDataType.TEXT),
                ),
            ],
        )
        assert ts.name == "users"
        assert len(ts.columns) == 2

    def test_get_column(self):
        ts = TableSchema(
            name="users",
            table_id="public.users",
            columns=[
                ColumnSchema(
                    name="id",
                    column_id="public.users.id",
                    data_type=DataType(base_type=SQLDataType.INTEGER),
                ),
            ],
        )
        col = ts.get_column("id")
        assert col is not None
        assert col.name == "id"

    def test_get_column_not_found(self):
        ts = TableSchema(
            name="users",
            table_id="public.users",
            columns=[],
        )
        col = ts.get_column("nonexistent")
        assert col is None


class TestCatalog:
    """Tests for Catalog."""

    def test_create_catalog(self):
        """Catalog uses a dict[str, TableSchema] for tables."""
        ts = TableSchema(
            name="users",
            table_id="public.users",
            columns=[
                ColumnSchema(
                    name="id",
                    column_id="public.users.id",
                    data_type=DataType(base_type=SQLDataType.INTEGER),
                ),
            ],
        )
        cat = Catalog(tables={"public.users": ts})
        assert len(cat.tables) == 1

    def test_get_table(self):
        ts = TableSchema(
            name="users",
            table_id="public.users",
            columns=[],
        )
        cat = Catalog(tables={"public.users": ts}, aliases={"users": "public.users"})
        table = cat.get_table("users")
        assert table is not None
        assert table.name == "users"

    def test_from_dict(self):
        cat = Catalog.from_dict(
            {
                "users": [
                    {"name": "id", "type": "integer"},
                    {"name": "name", "type": "text"},
                ],
                "orders": [
                    {"name": "id", "type": "integer"},
                    {"name": "user_id", "type": "integer"},
                ],
            }
        )
        assert cat.get_table("users") is not None
        assert cat.get_table("orders") is not None
        users = cat.get_table("users")
        assert len(users.columns) == 2

    def test_add_table(self):
        cat = Catalog()
        ts = TableSchema(
            name="users",
            table_id="public.users",
            columns=[],
        )
        cat.add_table(ts)
        assert cat.get_table("users") is not None


class TestAttribute:
    """Tests for Attribute."""

    def test_create_attribute(self):
        attr = Attribute(
            name="id",
            data_type=DataType(base_type=SQLDataType.INTEGER),
        )
        assert attr.name == "id"
