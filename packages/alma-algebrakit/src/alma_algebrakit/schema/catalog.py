"""Catalog and schema types for algebrakit.

Provides a generic schema representation that can be populated from
any source (SQL databases, data catalogs, dataframe schemas, etc.).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from alma_algebrakit.schema.types import DataType, SQLDataType


class ColumnSchema(BaseModel):
    """Schema information for a single column."""

    name: str = Field(description="Column name")
    column_id: str = Field(description="Stable column identifier")
    data_type: DataType = Field(description="Column type")
    is_primary_key: bool = Field(default=False)
    is_foreign_key: bool = Field(default=False)
    foreign_key_target: str | None = Field(
        default=None, description="Target column for FK (table_id.column_id)"
    )
    comment: str | None = Field(default=None)
    tags: list[str] = Field(default_factory=list)


class TableSchema(BaseModel):
    """Schema information for a table/relation."""

    name: str = Field(description="Table name")
    table_id: str = Field(description="Stable fully-qualified identifier")
    schema_name: str | None = Field(default=None, description="Schema/namespace")
    columns: list[ColumnSchema] = Field(default_factory=list)
    primary_key: list[str] | None = Field(default=None, description="Primary key column names")
    comment: str | None = Field(default=None)
    tags: list[str] = Field(default_factory=list)
    row_count_estimate: int | None = Field(default=None)

    def get_column(self, name: str) -> ColumnSchema | None:
        """Get column by name (case-insensitive)."""
        name_lower = name.lower()
        for col in self.columns:
            if col.name.lower() == name_lower:
                return col
        return None

    def column_names(self) -> list[str]:
        """Get all column names."""
        return [col.name for col in self.columns]

    def fully_qualified_name(self) -> str:
        """Get fully qualified table name."""
        if self.schema_name:
            return f"{self.schema_name}.{self.name}"
        return self.name


class Catalog(BaseModel):
    """Schema catalog for resolving table and column references.

    This is a generic representation that can be populated from:
    - SQL information_schema queries
    - dbt manifest files
    - DataFrame schema introspection
    - Manual definition
    """

    tables: dict[str, TableSchema] = Field(
        default_factory=dict,
        description="Mapping of table_id -> TableSchema",
    )
    aliases: dict[str, str] = Field(
        default_factory=dict,
        description="Table name aliases -> table_id",
    )
    default_schema: str | None = Field(
        default=None, description="Default schema for unqualified names"
    )

    def get_table(self, name: str) -> TableSchema | None:
        """Get table by name, checking aliases and qualified names."""
        # Direct lookup
        if name in self.tables:
            return self.tables[name]

        # Check aliases
        if name in self.aliases:
            return self.tables.get(self.aliases[name])

        # Try with default schema
        if self.default_schema and "." not in name:
            qualified = f"{self.default_schema}.{name}"
            if qualified in self.tables:
                return self.tables[qualified]

        # Case-insensitive search
        name_lower = name.lower()
        for _table_id, schema in self.tables.items():
            if schema.name.lower() == name_lower:
                return schema
            if schema.fully_qualified_name().lower() == name_lower:
                return schema

        return None

    def add_table(self, schema: TableSchema) -> None:
        """Add a table to the catalog."""
        self.tables[schema.table_id] = schema
        # Also index by name
        self.aliases[schema.name] = schema.table_id
        if schema.schema_name:
            self.aliases[schema.fully_qualified_name()] = schema.table_id

    def resolve_column(
        self, table_name: str, column_name: str
    ) -> tuple[TableSchema, ColumnSchema] | None:
        """Resolve a qualified column reference."""
        table = self.get_table(table_name)
        if not table:
            return None

        col = table.get_column(column_name)
        if not col:
            return None

        return (table, col)

    @classmethod
    def from_dict(cls, data: dict[str, list[dict[str, str]]]) -> Catalog:
        """Create catalog from simple dict format.

        Format: {"table_name": [{"name": "col1", "type": "integer"}, ...]}
        """
        catalog = cls()

        for table_name, columns in data.items():
            table_id = table_name
            col_schemas = []
            for col in columns:
                col_type = col.get("type", "unknown")
                try:
                    base_type = SQLDataType(col_type.lower())
                except ValueError:
                    base_type = SQLDataType.UNKNOWN

                col_schemas.append(
                    ColumnSchema(
                        name=col["name"],
                        column_id=col["name"],
                        data_type=DataType(
                            base_type=base_type,
                            nullable=col.get("nullable", True),
                        ),
                        is_primary_key=col.get("primary_key", False),
                    )
                )

            catalog.add_table(
                TableSchema(
                    name=table_name,
                    table_id=table_id,
                    columns=col_schemas,
                )
            )

        return catalog


# Backward compatibility alias
CatalogSnapshot = Catalog
