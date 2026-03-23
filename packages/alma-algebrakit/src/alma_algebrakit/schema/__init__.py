"""Generic schema representation (not SQL-specific)."""

from alma_algebrakit.schema.catalog import (
    Catalog,
    ColumnSchema,
    TableSchema,
)
from alma_algebrakit.schema.constraints import (
    ForeignKey,
    PrimaryKey,
    SchemaConstraints,
    UniqueConstraint,
)
from alma_algebrakit.schema.types import (
    Attribute,
    DataType,
    SQLDataType,
)

__all__ = [
    "DataType",
    "SQLDataType",
    "Attribute",
    "Catalog",
    "TableSchema",
    "ColumnSchema",
    "SchemaConstraints",
    "PrimaryKey",
    "ForeignKey",
    "UniqueConstraint",
]
