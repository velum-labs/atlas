"""CTE (Common Table Expression) extensions for SQL.

CTEs (WITH clause) are SQL-specific constructs that allow defining
named subqueries that can be referenced in the main query.
"""

from __future__ import annotations

from typing import Literal as TypeLiteral

from alma_algebrakit import RAExpression, RelationRef
from pydantic import BaseModel, Field


class CTEDefinition(BaseModel):
    """A single CTE definition (WITH name AS (query))."""

    name: str = Field(description="CTE name")
    columns: list[str] | None = Field(default=None, description="Column aliases for CTE")
    query: RAExpression = Field(description="CTE query expression")
    is_recursive: bool = Field(default=False, description="Whether this CTE is recursive")

    def fingerprint(self) -> str:
        recursive_str = "RECURSIVE " if self.is_recursive else ""
        cols_str = f"({', '.join(self.columns)})" if self.columns else ""
        return f"{recursive_str}{self.name}{cols_str} AS ({self.query.fingerprint()})"


class WithClause(RAExpression):
    """WITH clause containing one or more CTEs."""

    type: TypeLiteral["with_clause"] = "with_clause"
    ctes: list[CTEDefinition] = Field(description="CTE definitions")
    main_query: RAExpression = Field(description="Main query that uses the CTEs")
    is_recursive: bool = Field(default=False, description="Whether any CTE is recursive")

    def referenced_tables(self) -> set[str]:
        """Return referenced tables, excluding CTE names.

        Collects all tables from the main query and CTE queries, then removes
        all CTE names. This handles the case where one CTE references another CTE.
        """
        tables = self.main_query.referenced_tables()
        for cte in self.ctes:
            tables |= cte.query.referenced_tables()
        # Remove all CTE names at the end (handles cross-references between CTEs)
        for cte in self.ctes:
            tables.discard(cte.name)
        return tables

    def relation_instances(self) -> set[RelationRef]:
        instances = self.main_query.relation_instances()
        for cte in self.ctes:
            instances |= cte.query.relation_instances()
        return instances

    def output_columns(self) -> set[str]:
        return self.main_query.output_columns()

    def fingerprint(self) -> str:
        cte_strs = [cte.fingerprint() for cte in self.ctes]
        recursive_str = "RECURSIVE " if self.is_recursive else ""
        return f"WITH {recursive_str}{', '.join(cte_strs)} ({self.main_query.fingerprint()})"

    def cte_names(self) -> set[str]:
        """Return the set of CTE names defined in this WITH clause."""
        return {cte.name for cte in self.ctes}
