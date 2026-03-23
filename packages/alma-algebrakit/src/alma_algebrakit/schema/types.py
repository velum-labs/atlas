"""Data type definitions for the schema module.

Provides strongly-typed representations of data types that are
generic enough to work with any query language (SQL, Datalog, etc.).
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class SQLDataType(StrEnum):
    """SQL data types for type checking.

    Note: While named "SQL" for familiarity, these types are
    generic enough to represent data types in any query system.
    """

    # Numeric
    INTEGER = "integer"
    BIGINT = "bigint"
    SMALLINT = "smallint"
    DECIMAL = "decimal"
    NUMERIC = "numeric"
    REAL = "real"
    DOUBLE = "double"

    # String
    TEXT = "text"
    VARCHAR = "varchar"
    CHAR = "char"

    # Binary
    BYTEA = "bytea"

    # Boolean
    BOOLEAN = "boolean"

    # Date/Time
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    INTERVAL = "interval"

    # UUID
    UUID = "uuid"

    # JSON
    JSON = "json"
    JSONB = "jsonb"

    # Array
    ARRAY = "array"

    # Unknown/Any
    UNKNOWN = "unknown"


class DataType(BaseModel):
    """Rich data type with nullability and precision info."""

    base_type: SQLDataType = Field(description="Base data type")
    nullable: bool = Field(default=True, description="Whether NULL is allowed")
    precision: int | None = Field(default=None, description="Numeric precision")
    scale: int | None = Field(default=None, description="Numeric scale")
    length: int | None = Field(default=None, description="String length")
    element_type: DataType | None = Field(default=None, description="Array element type")

    def is_numeric(self) -> bool:
        """Check if type is numeric."""
        return self.base_type in (
            SQLDataType.INTEGER,
            SQLDataType.BIGINT,
            SQLDataType.SMALLINT,
            SQLDataType.DECIMAL,
            SQLDataType.NUMERIC,
            SQLDataType.REAL,
            SQLDataType.DOUBLE,
        )

    def is_string(self) -> bool:
        """Check if type is string-like."""
        return self.base_type in (
            SQLDataType.TEXT,
            SQLDataType.VARCHAR,
            SQLDataType.CHAR,
        )

    def is_temporal(self) -> bool:
        """Check if type is date/time."""
        return self.base_type in (
            SQLDataType.DATE,
            SQLDataType.TIME,
            SQLDataType.TIMESTAMP,
            SQLDataType.TIMESTAMPTZ,
            SQLDataType.INTERVAL,
        )

    def is_comparable_to(self, other: DataType) -> bool:
        """Check if two types can be compared."""
        # Same type always comparable
        if self.base_type == other.base_type:
            return True

        # Numeric types are inter-comparable
        if self.is_numeric() and other.is_numeric():
            return True

        # String types are inter-comparable
        if self.is_string() and other.is_string():
            return True

        # Temporal types are inter-comparable
        if self.is_temporal() and other.is_temporal():
            return True

        # Unknown is comparable to anything
        if self.base_type == SQLDataType.UNKNOWN or other.base_type == SQLDataType.UNKNOWN:
            return True

        return False


class Attribute(BaseModel):
    """An attribute (column) in a relation schema.

    This is the algebraic representation of a column, independent
    of any specific query language.
    """

    name: str = Field(description="Attribute name")
    data_type: DataType = Field(description="Attribute data type")
    nullable: bool = Field(default=True, description="Whether NULL is allowed")
    comment: str | None = Field(default=None, description="Documentation comment")

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Attribute):
            return False
        return self.name == other.name
