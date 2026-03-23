"""Scope management for algebraic binding.

A Scope represents the set of relations visible at a point in a query.
This is an algebraic concept that exists in any nested query language.

Scopes are nested for:
- Subqueries: Inner scope can shadow outer names
- CTEs: CTEs are added to scope before the main query
- LATERAL joins: Can see relations from earlier in the FROM clause
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from alma_algebrakit.scope.instance import RelationInstance


class Scope(BaseModel):
    """Represents a binding scope for attribute resolution.

    A scope contains:
    - Relations: Mapping from alias to RelationInstance
    - CTEs: Mapping from CTE name to RelationInstance
    - Parent: Optional parent scope for nested queries

    Resolution order:
    1. Current scope CTEs
    2. Current scope relations
    3. Parent scope CTEs (always visible to children)
    4. Parent scope relations (only for LATERAL subqueries)

    This is a generic algebraic concept:
    - SQL: FROM clause defines relations in scope
    - Datalog: Rule head defines variables in scope
    - DataFrames: Chain of operations defines available columns
    """

    relations: dict[str, RelationInstance] = Field(
        default_factory=dict,
        description="Alias -> RelationInstance in this scope",
    )
    parent: Scope | None = Field(default=None, description="Parent scope for nested queries")
    ctes: dict[str, RelationInstance] = Field(
        default_factory=dict,
        description="CTE name -> RelationInstance",
    )
    is_lateral: bool = Field(default=False, description="Can see parent scope columns")

    model_config = {"arbitrary_types_allowed": True}

    def add_relation(self, instance: RelationInstance) -> None:
        """Add a relation instance to this scope.

        Raises:
            ValueError: If the alias already exists with a different ID,
                       or if the alias conflicts with a CTE name.
        """
        alias = instance.alias
        # Check for alias collision with existing relation (different ID = different table)
        if alias in self.relations:
            existing = self.relations[alias]
            if existing.id != instance.id:
                raise ValueError(
                    f"Alias '{alias}' already exists in scope "
                    f"(existing: {existing.id}, new: {instance.id}). "
                    f"Use a different alias for self-joins."
                )
        # Check for collision with CTE name
        if alias in self.ctes:
            raise ValueError(f"Alias '{alias}' conflicts with CTE name in scope")
        self.relations[alias] = instance

    def add_cte(self, name: str, instance: RelationInstance) -> None:
        """Add a CTE to this scope."""
        # Mark as CTE
        instance_copy = instance.model_copy()
        instance_copy.is_cte = True
        self.ctes[name] = instance_copy

    def get_relation(self, alias: str) -> RelationInstance | None:
        """Get a relation by alias, checking CTEs first.

        Resolution order:
        1. Current scope CTEs
        2. Current scope relations
        3. Parent scope CTEs (always visible)

        Note: Parent relations are NOT accessible via get_relation(), even for
        LATERAL scopes. Use find_attribute_in_relations() for LATERAL visibility.
        """
        # CTEs take precedence
        if alias in self.ctes:
            return self.ctes[alias]

        if alias in self.relations:
            return self.relations[alias]

        # Check parent scope CTEs (always visible to child scopes)
        if self.parent:
            if alias in self.parent.ctes:
                return self.parent.ctes[alias]

        return None

    def all_relations(self) -> dict[str, RelationInstance]:
        """Get all relations visible in this scope."""
        result: dict[str, RelationInstance] = {}

        # Add parent relations first (if LATERAL)
        if self.is_lateral and self.parent:
            result.update(self.parent.relations)
            result.update(self.parent.ctes)

        # Parent CTEs are always visible
        if self.parent:
            result.update(self.parent.ctes)

        # Current scope
        result.update(self.relations)
        result.update(self.ctes)

        return result

    def find_attribute_in_relations(self, column_name: str) -> list[tuple[str, RelationInstance]]:
        """Find which relations contain a given attribute.

        Returns list of (alias, instance) pairs that have this column.
        Used for resolving unqualified column references.

        Resolution order (consistent with get_relation()):
        1. Current scope CTEs
        2. Current scope relations (CTEs shadow relations with same alias)
        3. Parent scope CTEs (always visible to children)
        4. Parent scope relations (only for LATERAL subqueries)
        """
        matches: list[tuple[str, RelationInstance]] = []
        shadowed_aliases: set[str] = set()

        # Check CTEs first (consistent with get_relation)
        for cte_name, instance in self.ctes.items():
            if instance.has_column(column_name):
                matches.append((cte_name, instance))
                # CTE shadows any relation with the same alias
                shadowed_aliases.add(cte_name)

        # Check current scope relations (skip shadowed aliases)
        for alias, instance in self.relations.items():
            if alias not in shadowed_aliases and instance.has_column(column_name):
                matches.append((alias, instance))

        # Parent CTEs are ALWAYS visible to children (not just for LATERAL)
        if self.parent:
            for cte_name, instance in self.parent.ctes.items():
                if instance.has_column(column_name):
                    matches.append((f"_outer.{cte_name}", instance))

        # Parent relations only visible for LATERAL subqueries
        if self.is_lateral and self.parent:
            for alias, instance in self.parent.relations.items():
                if instance.has_column(column_name):
                    matches.append((f"_outer.{alias}", instance))

        return matches

    def create_child_scope(self, lateral: bool = False) -> Scope:
        """Create a child scope for a subquery."""
        return Scope(parent=self, is_lateral=lateral)

    def __repr__(self) -> str:
        rel_names = list(self.relations.keys())
        cte_names = list(self.ctes.keys())
        parent_str = " (has parent)" if self.parent else ""
        return f"Scope(relations={rel_names}, ctes={cte_names}{parent_str})"
