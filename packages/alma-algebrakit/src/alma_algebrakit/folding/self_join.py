"""Self-join rewriting support for extended folding.

This module provides algorithms for handling self-joins (same table appearing
multiple times) when folding queries using views.

Theoretical Foundation:
    Theorem: A self-join T t1 JOIN T t2 can be rewritten as V v1 JOIN V v2
    (where V is a view over T) if and only if V is key-preserving for T.

    Proof sketch:
    - Key preservation means V's projection includes T's primary key
    - This ensures each row in T maps to exactly one row in V
    - Therefore, the join cardinality is preserved
    - Without key preservation, V may collapse rows, changing join semantics

Algorithm:
    1. Detect self-joins: Group RelationRefs by physical_table
    2. For each group with multiple instances:
       a. Check if view is key-preserving for that table
       b. If yes, create multi-instance mapping (t1→v1, t2→v2)
       c. If no, reject with explanation
    3. Generate column rewrite map for each instance

Example:
    Query: SELECT * FROM users u1 JOIN users u2 ON u1.manager_id = u2.id
    View: SELECT id, name, manager_id FROM users WHERE active = true

    If view preserves PK (id), rewrite to:
    SELECT * FROM view v1 JOIN view v2 ON v1.manager_id = v2.id
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from alma_algebrakit.models.algebra import RelationRef
from alma_algebrakit.schema.constraints import SchemaConstraints


@dataclass
class SelfJoinAnalysis:
    """Result of analyzing self-joins in covered relations.

    Attributes:
        has_self_joins: Whether any self-joins exist
        is_rewritable: Whether all self-joins can be safely rewritten
        instance_groups: Maps physical_table -> list of instances
        instance_aliases: Maps physical_table -> {orig_alias: view_instance_alias}
        rejection_reason: If not rewritable, explains why
    """

    has_self_joins: bool
    is_rewritable: bool
    instance_groups: dict[str, list[RelationRef]]
    instance_aliases: dict[str, dict[str, str]]
    rejection_reason: str | None = None


def group_by_physical_table(
    relations: set[RelationRef],
) -> dict[str, list[RelationRef]]:
    """Group RelationRefs by their physical table name.

    Args:
        relations: Set of relation references

    Returns:
        Dictionary mapping physical_table (lowercased) to list of RelationRefs
    """
    groups: dict[str, list[RelationRef]] = defaultdict(list)
    for rel in relations:
        key = rel.physical_table.lower()
        groups[key].append(rel)
    return dict(groups)


def detect_self_joins(
    relations: set[RelationRef],
) -> tuple[bool, dict[str, list[RelationRef]]]:
    """Detect if any physical table appears multiple times.

    Args:
        relations: Set of relation references to analyze

    Returns:
        (has_self_joins, groups) tuple where groups maps table to instances
    """
    groups = group_by_physical_table(relations)
    has_self_joins = any(len(instances) > 1 for instances in groups.values())
    return has_self_joins, groups


def check_self_join_rewritable(
    self_join_instances: list[RelationRef],
    view_exposed_columns: frozenset[str],
    constraints: SchemaConstraints,
) -> tuple[bool, str]:
    """Check if a self-join can be rewritten using multiple view instances.

    A self-join T t1 JOIN T t2 is rewritable to V v1 JOIN V v2 iff:
    1. V is key-preserving for T (V includes T's primary key)

    This ensures that the join semantics are preserved:
    - Each row in T maps to exactly one row in V
    - The join cardinality remains unchanged

    Args:
        self_join_instances: List of RelationRef instances of the same table
        view_exposed_columns: Columns exposed by the view
        constraints: Schema constraints containing key information

    Returns:
        (is_safe, reason) tuple where reason explains the decision
    """
    if len(self_join_instances) < 2:
        return True, "No self-join present"

    # Get the physical table name (should be same for all instances)
    physical_table = self_join_instances[0].physical_table

    # Check key preservation
    if not constraints.is_key_preserving(physical_table, set(view_exposed_columns)):
        keys = constraints.get_keys(physical_table)
        if not keys:
            return False, (
                f"Self-join on '{physical_table}' requires key constraint. "
                f"No primary key or unique constraint found for table."
            )
        else:
            key_strs = [", ".join(sorted(k)) for k in keys]
            return False, (
                f"Self-join on '{physical_table}' requires key-preserving view. "
                f"View must include all columns from at least one key: {key_strs}. "
                f"View exposes: {sorted(view_exposed_columns)}"
            )

    return True, f"View is key-preserving for '{physical_table}'"


def analyze_self_joins(
    covered_rels: set[RelationRef],
    view_exposed_columns: frozenset[str],
    constraints: SchemaConstraints | None,
    view_alias_prefix: str = "cv",
) -> SelfJoinAnalysis:
    """Analyze self-joins in covered relations and determine rewritability.

    This is the main entry point for self-join analysis. It:
    1. Detects self-joins by grouping relations by physical table
    2. Checks if each self-join is rewritable (key-preserving view)
    3. Generates instance aliases for multi-instance rewriting

    Args:
        covered_rels: Relations covered by the view
        view_exposed_columns: Columns exposed by the view
        constraints: Schema constraints (required for self-join support)
        view_alias_prefix: Prefix for generated view instance aliases

    Returns:
        SelfJoinAnalysis with all information needed for rewriting
    """
    has_self_joins, groups = detect_self_joins(covered_rels)

    if not has_self_joins:
        return SelfJoinAnalysis(
            has_self_joins=False,
            is_rewritable=True,
            instance_groups=groups,
            instance_aliases={},
        )

    # Self-joins exist - check if constraints are available
    if constraints is None:
        return SelfJoinAnalysis(
            has_self_joins=True,
            is_rewritable=False,
            instance_groups=groups,
            instance_aliases={},
            rejection_reason=(
                "Self-join detected but no schema_constraints provided. "
                "Cannot verify key preservation for safe rewriting."
            ),
        )

    # Check each self-join group
    instance_aliases: dict[str, dict[str, str]] = {}

    for table, instances in groups.items():
        if len(instances) > 1:
            # Check if this self-join is rewritable
            is_safe, reason = check_self_join_rewritable(
                instances, view_exposed_columns, constraints
            )

            if not is_safe:
                return SelfJoinAnalysis(
                    has_self_joins=True,
                    is_rewritable=False,
                    instance_groups=groups,
                    instance_aliases={},
                    rejection_reason=reason,
                )

            # Generate instance aliases: u1 -> cv_1, u2 -> cv_2
            instance_aliases[table] = {}
            for i, rel in enumerate(instances, start=1):
                instance_aliases[table][rel.alias] = f"{view_alias_prefix}_{i}"
        else:
            # Single instance - use standard alias
            rel = instances[0]
            instance_aliases[table] = {rel.alias: view_alias_prefix}

    return SelfJoinAnalysis(
        has_self_joins=True,
        is_rewritable=True,
        instance_groups=groups,
        instance_aliases=instance_aliases,
    )


def create_multi_instance_column_mapping(
    analysis: SelfJoinAnalysis,
    column_lineage: dict[tuple[str, str], str],
    is_select_star: bool,
) -> dict[str, str]:
    """Create column rewrite map for multi-instance self-join rewriting.

    For each original column reference (e.g., "u1.id"), maps it to the
    corresponding view instance column (e.g., "cv_1.user_id").

    Args:
        analysis: Self-join analysis result
        column_lineage: View's column lineage mapping
        is_select_star: Whether view is SELECT * (passthrough)

    Returns:
        Dictionary mapping "orig_alias.col" -> "view_instance_alias.mapped_col"

    Example:
        Input: Self-join users u1, u2 with lineage {("u", "id"): "user_id"}
        Output: {"u1.id": "cv_1.user_id", "u2.id": "cv_2.user_id", ...}
    """
    rewrite_map: dict[str, str] = {}

    for table, alias_mapping in analysis.instance_aliases.items():
        for orig_alias, view_alias in alias_mapping.items():
            # For each column, create mapping
            if is_select_star:
                # SELECT * - columns pass through with same name
                # We don't know all columns, but map what we can
                for (src_table, src_col), view_col in column_lineage.items():
                    if src_table.lower() == orig_alias.lower() or src_table.lower() == table:
                        rewrite_map[f"{orig_alias}.{src_col}"] = f"{view_alias}.{view_col}"
            else:
                # Explicit lineage - use it
                for (src_table, src_col), view_col in column_lineage.items():
                    # Match by alias or physical table name
                    if src_table.lower() == orig_alias.lower() or src_table.lower() == table:
                        rewrite_map[f"{orig_alias}.{src_col}"] = f"{view_alias}.{view_col}"

    return rewrite_map


def get_view_instances_needed(analysis: SelfJoinAnalysis) -> int:
    """Get the total number of view instances needed for rewriting.

    Args:
        analysis: Self-join analysis result

    Returns:
        Number of distinct view instances needed
    """
    if not analysis.has_self_joins:
        return 1

    return sum(len(instances) for instances in analysis.instance_groups.values())
