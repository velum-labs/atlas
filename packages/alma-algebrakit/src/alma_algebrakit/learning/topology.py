"""Topology-focused view learning.

This module provides topology-only view learning that focuses on relations
and joins, ignoring filter predicates. This produces maximally reusable views
that serve any query on a given join topology.

Key insight: Views should capture join structure, not query-specific filters.
Filters can be pushed down at query time.

Topology = Relations ∩ Joins (no predicates)

## Normalization Pipeline

All topologies go through a normalization pipeline controlled by `TopologyNormalization`:

1. **Schema normalization**: Unqualified tables → `schema.table` (via `default_schema`)
2. **Alias insensitivity**: Different aliases for same table → same physical name
3. **Self-join canonicalization**: Occurrence numbers assigned by edge columns

This means:
- `orders o JOIN customers c` and `orders ord JOIN customers cust` produce
  equivalent topologies (both reference the same physical tables)
- `orders` and `public.orders` produce the same topology (with default_schema="public")
- Self-joins with swapped left/right produce the same canonical topology

## Usage

```python
from alma_algebrakit.learning.topology import (
    extract_topology,
    topology_gcs,
    TopologyNormalization,
)

# Extract topology (uses default normalization)
topo = extract_topology(expr)
print(topo.physical_tables())  # {'public.orders', 'public.customers'}
print(topo.join_edges)  # {JoinEdge(...)}

# Custom normalization
norm = TopologyNormalization(
    default_schema="analytics",
    canonicalize_self_joins=True,
)
topo = extract_topology(expr, normalization=norm)

# Compute common topology across expressions
result = topology_gcs([expr1, expr2, expr3])
if result.success:
    view_expr = result.expression
```
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, Field, computed_field

if TYPE_CHECKING:
    pass

from alma_algebrakit.models.algebra import (
    Aggregation,
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    Difference,
    Intersect,
    Join,
    JoinType,
    Predicate,
    Projection,
    RAExpression,
    Relation,
    Selection,
    Union,
)
from alma_algebrakit.visitor import RAExpressionVisitor

# =============================================================================
# Normalization Configuration
# =============================================================================


@dataclass(frozen=True)
class TopologyNormalization:
    """Configuration for all topology normalizations.

    Groups normalization settings to avoid parameter proliferation
    and make it easy to add future normalizations.

    Normalizations applied:
    1. Schema normalization: Unqualified tables → schema.table (via default_schema)
    2. Alias insensitivity: Different aliases for same table → same physical name
    3. Self-join canonicalization: Occurrence numbers assigned by edge columns
    """

    default_schema: str = "public"
    """Schema to use for unqualified table references."""

    canonicalize_self_joins: bool = True
    """Whether to canonicalize self-join occurrence numbers based on edge columns.

    When True, self-join occurrences are renumbered so that semantically
    equivalent self-joins produce the same topology regardless of AST order.
    """


# Default normalization instance
DEFAULT_NORMALIZATION = TopologyNormalization()


# =============================================================================
# Topology Data Structures
# =============================================================================


class TableRef(BaseModel):
    """Reference to a table in a topology.

    Stores schema_name, table, and alias separately for:
    - Correct comparison (via physical property)
    - Correct reconstruction (via to_relation())

    Equality and hashing use only the computed physical name (schema.table#occurrence),
    making topologies alias-insensitive. Self-joins are distinguished by occurrence
    number rather than string suffixes.
    """

    model_config = {"frozen": True}

    schema_name: str = Field(description="Schema name (always present, defaulted if needed)")
    table: str = Field(description="Table name (without schema)")
    alias: str = Field(description="Alias used in query")
    occurrence: int = Field(default=1, description="Occurrence number for self-joins")

    @classmethod
    def from_relation(
        cls,
        rel: Relation,
        occurrence: int = 1,
        default_schema: str = "public",
    ) -> TableRef:
        """Create a TableRef from a Relation.

        Args:
            rel: The Relation to create a ref from
            occurrence: Occurrence number for self-joins (1 = first, 2 = second, etc.)
            default_schema: Schema to use if rel.schema_name is None
        """
        return cls(
            schema_name=rel.schema_name or default_schema,
            table=rel.name,
            alias=rel.alias or rel.name,
            occurrence=occurrence,
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def physical(self) -> str:
        """Fully qualified name for comparison (with occurrence suffix for self-joins)."""
        base = f"{self.schema_name}.{self.table}"
        return f"{base}#{self.occurrence}" if self.occurrence > 1 else base

    def base_table(self) -> str:
        """Fully qualified name without occurrence suffix."""
        return f"{self.schema_name}.{self.table}"

    def to_relation(self) -> Relation:
        """Reconstruct a Relation with correct schema_name and name."""
        return Relation(
            name=self.table,
            schema_name=self.schema_name,
            alias=self.alias,
        )

    def __hash__(self) -> int:
        return hash(self.physical)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TableRef):
            return False
        return self.physical == other.physical

    def __str__(self) -> str:
        base = self.base_table()
        if self.alias != self.table:
            return f"{base} AS {self.alias}"
        return base


class JoinEdge(BaseModel):
    """A single join edge in the topology.

    Represents an equi-join between two tables on specific columns.
    Uses TableRef for table references, making edges alias-insensitive.

    The canonical form ensures consistent comparison regardless of
    which side was "left" or "right" in the original query.
    """

    model_config = {"frozen": True}

    left: TableRef = Field(description="Left table reference")
    right: TableRef = Field(description="Right table reference")
    left_column: str = Field(description="Left join column")
    right_column: str = Field(description="Right join column")

    def canonical(self) -> tuple[str, str, str, str]:
        """Return canonical form for comparison.

        Orders by (physical table, column) to ensure a.x = b.y and b.y = a.x
        are treated as the same edge.
        """
        left = (self.left.physical, self.left_column)
        right = (self.right.physical, self.right_column)
        if left > right:
            return (self.right.physical, self.right_column, self.left.physical, self.left_column)
        return (self.left.physical, self.left_column, self.right.physical, self.right_column)

    def fingerprint(self) -> str:
        """Return string fingerprint for display."""
        lt, lc, rt, rc = self.canonical()
        return f"{lt}.{lc}={rt}.{rc}"

    def __hash__(self) -> int:
        return hash(self.canonical())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JoinEdge):
            return False
        return self.canonical() == other.canonical()


class Topology(BaseModel):
    """Join topology: relations + join edges (no predicates).

    A topology captures the structural "shape" of queries without
    any filter predicates. Two queries have the same topology if
    they join the same physical tables on the same columns.

    Uses TableRef for alias-insensitive comparison while preserving
    schema, table, and alias information for correct RA reconstruction.
    """

    model_config = {"arbitrary_types_allowed": True}

    relations: frozenset[TableRef] = Field(
        default_factory=frozenset, description="Set of table references"
    )
    join_edges: frozenset[JoinEdge] = Field(
        default_factory=frozenset, description="Set of join edges"
    )

    def is_empty(self) -> bool:
        """Check if topology has no relations."""
        return len(self.relations) == 0

    def relation_count(self) -> int:
        """Number of relations in the topology."""
        return len(self.relations)

    def edge_count(self) -> int:
        """Number of join edges in the topology."""
        return len(self.join_edges)

    def physical_tables(self) -> frozenset[str]:
        """Get unique fully qualified table names (without occurrence suffixes)."""
        return frozenset(r.base_table() for r in self.relations)

    def is_connected(self) -> bool:
        """Check if all relations are connected via joins.

        A topology is connected if you can reach any relation from
        any other relation by following join edges.
        """
        if len(self.relations) <= 1:
            return True

        # Build adjacency list using physical names
        physical_names = {r.physical for r in self.relations}
        adjacency: dict[str, set[str]] = {p: set() for p in physical_names}
        for edge in self.join_edges:
            if edge.left.physical in adjacency and edge.right.physical in adjacency:
                adjacency[edge.left.physical].add(edge.right.physical)
                adjacency[edge.right.physical].add(edge.left.physical)

        # BFS from first relation
        start = next(iter(physical_names))
        visited: set[str] = {start}
        queue = [start]

        while queue:
            current = queue.pop(0)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)

        return len(visited) == len(physical_names)

    def fingerprint(self) -> str:
        """Return a canonical string fingerprint for the topology."""
        rels = sorted(r.physical for r in self.relations)
        edges = sorted(e.fingerprint() for e in self.join_edges)
        return f"({','.join(rels)})[{';'.join(edges)}]"

    def __hash__(self) -> int:
        return hash((self.relations, self.join_edges))


class TopologyResult(BaseModel):
    """Result of topology computation."""

    model_config = {"arbitrary_types_allowed": True}

    success: bool = Field(description="Whether topology computation succeeded")
    topology: Topology | None = Field(default=None, description="The computed topology")
    expression: RAExpression | None = Field(
        default=None, description="The RA expression for the topology"
    )
    explanation: str = Field(default="")
    common_relations: list[str] = Field(
        default_factory=list, description="Physical table names in the topology"
    )
    common_joins: list[str] = Field(default_factory=list, description="Join edge fingerprints")
    input_count: int = Field(default=0, description="Number of input expressions")


# =============================================================================
# Self-Join Canonicalization
# =============================================================================


class SelfJoinCanonicalizer(Protocol):
    """Strategy for canonicalizing self-join occurrences.

    Self-joins produce multiple TableRefs for the same physical table.
    The occurrence numbers assigned during extraction depend on AST traversal
    order, which can vary for semantically equivalent queries.

    A canonicalizer renumbers occurrences to ensure consistent topologies
    regardless of traversal order.
    """

    def canonicalize(self, topology: Topology) -> Topology:
        """Return topology with canonical self-join ordering.

        Args:
            topology: The topology to canonicalize

        Returns:
            A new Topology with canonical occurrence numbers
        """
        ...


class EdgeBasedCanonicalizer:
    """Canonicalizes self-join occurrences by their edge column names.

    For each self-join table (a table with multiple occurrences), this
    canonicalizer assigns occurrence numbers based on the columns each
    occurrence contributes to join edges.

    Example:
        Given: orders#?.parent_id = orders#?.id
        - Occurrence contributing 'id' → orders (occurrence 1, "id" < "parent_id")
        - Occurrence contributing 'parent_id' → orders#2 (occurrence 2)

    This ensures that regardless of which side was "left" or "right" in
    the original query, the same canonical topology is produced.
    """

    def canonicalize(self, topology: Topology) -> Topology:
        """Canonicalize self-join occurrences by edge columns.

        Args:
            topology: The topology to canonicalize

        Returns:
            A new Topology with canonical occurrence numbers
        """
        # Group relations by base table
        by_base: dict[str, list[TableRef]] = defaultdict(list)
        for ref in topology.relations:
            by_base[ref.base_table()].append(ref)

        # Find tables with self-joins (multiple occurrences)
        self_join_tables = {base for base, refs in by_base.items() if len(refs) > 1}

        if not self_join_tables:
            # No self-joins, return unchanged
            return topology

        # Build mapping: old physical → sort key (min column contributed)
        sort_keys: dict[str, str] = {}
        for edge in topology.join_edges:
            left_base = edge.left.base_table()
            right_base = edge.right.base_table()

            # For self-join edges (both sides same base table)
            if left_base == right_base and left_base in self_join_tables:
                # Each occurrence contributes a column
                left_physical = edge.left.physical
                right_physical = edge.right.physical

                # Update sort key with minimum column seen
                if left_physical not in sort_keys or edge.left_column < sort_keys[left_physical]:
                    sort_keys[left_physical] = edge.left_column
                if right_physical not in sort_keys or edge.right_column < sort_keys[right_physical]:
                    sort_keys[right_physical] = edge.right_column

        # For occurrences not in any self-join edge, use alias as fallback
        for base in self_join_tables:
            for ref in by_base[base]:
                if ref.physical not in sort_keys:
                    sort_keys[ref.physical] = ref.alias

        # Build canonical mapping: old physical → new TableRef
        mapping: dict[str, TableRef] = {}

        for base in self_join_tables:
            refs = by_base[base]
            # Sort by (sort_key, alias) for deterministic ordering
            sorted_refs = sorted(refs, key=lambda r: (sort_keys.get(r.physical, r.alias), r.alias))

            # Assign canonical occurrence numbers
            for i, old_ref in enumerate(sorted_refs, 1):
                new_ref = TableRef(
                    schema_name=old_ref.schema_name,
                    table=old_ref.table,
                    alias=old_ref.alias,
                    occurrence=i,
                )
                mapping[old_ref.physical] = new_ref

        # Apply mapping to relations
        new_relations: set[TableRef] = set()
        for ref in topology.relations:
            if ref.physical in mapping:
                new_relations.add(mapping[ref.physical])
            else:
                new_relations.add(ref)

        # Apply mapping to edges
        new_edges: set[JoinEdge] = set()
        for edge in topology.join_edges:
            new_left = mapping.get(edge.left.physical, edge.left)
            new_right = mapping.get(edge.right.physical, edge.right)
            new_edges.add(
                JoinEdge(
                    left=new_left,
                    right=new_right,
                    left_column=edge.left_column,
                    right_column=edge.right_column,
                )
            )

        return Topology(
            relations=frozenset(new_relations),
            join_edges=frozenset(new_edges),
        )


# Default canonicalizer instance
_default_canonicalizer = EdgeBasedCanonicalizer()


# =============================================================================
# Topology Extraction
# =============================================================================


class TopologyExtractor(RAExpressionVisitor[None]):
    """Extracts topology (relations + joins) from RA, ignoring predicates.

    This visitor walks an RA expression and collects:
    - All relation references (tables) as TableRef
    - All equi-join edges (from join conditions)

    Selection predicates are completely ignored - we only care about
    the structural join topology.

    All table names are normalized to fully qualified form (schema.table)
    using the configured default_schema for unqualified references.

    Tracks table occurrences for self-join handling - the same physical
    table appearing multiple times gets distinct occurrence numbers.
    """

    def __init__(self, default_schema: str = "public") -> None:
        self.relations: set[TableRef] = set()
        self.join_edges: list[JoinEdge] = []
        self._table_counts: dict[str, int] = {}  # base_table (schema.table) -> count
        self._alias_to_ref: dict[str, TableRef] = {}  # alias -> TableRef
        self._default_schema = default_schema

    def visit_relation(self, expr: Relation) -> None:
        """Record a relation, handling self-joins."""
        # Always compute fully qualified base table
        schema = expr.schema_name or self._default_schema
        base_table = f"{schema}.{expr.name}"
        alias = expr.alias or expr.name

        # Track occurrence count for self-joins
        count = self._table_counts.get(base_table, 0) + 1
        self._table_counts[base_table] = count

        # Create TableRef with occurrence number
        ref = TableRef.from_relation(
            expr,
            occurrence=count,
            default_schema=self._default_schema,
        )
        self.relations.add(ref)
        self._alias_to_ref[alias] = ref

    def visit_selection(self, expr: Selection) -> None:
        """Ignore selection predicate, just recurse."""
        expr.input.accept(self)

    def visit_projection(self, expr: Projection) -> None:
        """Ignore projection, just recurse."""
        expr.input.accept(self)

    def visit_join(self, expr: Join) -> None:
        """Extract join edge from condition.

        Note: Must visit children first to populate alias_to_ref mapping.
        """
        # Visit children first to collect relations and their aliases
        expr.left.accept(self)
        expr.right.accept(self)

        # Now extract join edge if present
        if expr.condition:
            edge = self._extract_join_edge(expr.condition)
            if edge:
                self.join_edges.append(edge)

    def visit_aggregation(self, expr: Aggregation) -> None:
        """Aggregations still have underlying topology."""
        expr.input.accept(self)

    def visit_union(self, expr: Union) -> None:
        """For unions, extract topology from both sides."""
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_difference(self, expr: Difference) -> None:
        """For difference, extract topology from both sides."""
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_intersect(self, expr: Intersect) -> None:
        """For intersect, extract topology from both sides."""
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_sort(self, expr: Any) -> None:
        """Sort doesn't affect topology."""
        expr.input.accept(self)

    def visit_limit(self, expr: Any) -> None:
        """Limit doesn't affect topology."""
        expr.input.accept(self)

    def visit_with_expression(self, expr: Any) -> None:
        """For CTEs, extract topology from the main query."""
        expr.main_query.accept(self)

    def _extract_join_edge(self, predicate: Predicate) -> JoinEdge | None:
        """Extract a JoinEdge from an equi-join predicate.

        Only handles simple equi-joins: t1.col = t2.col
        """
        if isinstance(predicate, AtomicPredicate):
            if predicate.op == ComparisonOp.EQ:
                left_col = self._extract_column_ref(predicate.left)
                right_col = self._extract_column_ref(predicate.right)

                if left_col and right_col:
                    left_alias, left_column = left_col
                    right_alias, right_column = right_col

                    # Look up TableRefs for the aliases
                    left_ref = self._alias_to_ref.get(left_alias)
                    right_ref = self._alias_to_ref.get(right_alias)

                    # Only create edge if both refs found and tables are different
                    if left_ref and right_ref and left_ref.physical != right_ref.physical:
                        return JoinEdge(
                            left=left_ref,
                            right=right_ref,
                            left_column=left_column,
                            right_column=right_column,
                        )
        return None

    def _extract_column_ref(self, expr: Any) -> tuple[str, str] | None:
        """Extract (alias, column) from an expression if it's a column ref."""
        if isinstance(expr, ColumnRef):
            if expr.table:
                return (expr.table, expr.column)
        return None


def extract_topology(
    expr: RAExpression,
    normalization: TopologyNormalization | None = None,
    *,
    default_schema: str | None = None,
) -> Topology:
    """Extract topology from an RA expression.

    Collects all relations and equi-join edges, ignoring predicates.
    Uses fully qualified table names (schema.table) for comparison,
    making the result alias-insensitive.

    Normalization pipeline:
    1. Schema normalization: Unqualified tables → schema.table
    2. Alias insensitivity: Different aliases → same physical name
    3. Self-join canonicalization: Occurrence numbers assigned by edge columns

    Args:
        expr: The RA expression to analyze
        normalization: Normalization configuration (preferred)
        default_schema: Optional shorthand for normalization.default_schema

    Returns:
        Topology with relations and join edges
    """
    # Resolve normalization config with a default schema fallback.
    if normalization is None:
        normalization = TopologyNormalization(
            default_schema=default_schema or "public",
        )

    # Phase 1: Extract (schema normalization + alias handling)
    extractor = TopologyExtractor(default_schema=normalization.default_schema)
    expr.accept(extractor)

    topology = Topology(
        relations=frozenset(extractor.relations),
        join_edges=frozenset(extractor.join_edges),
    )

    # Phase 2: Post-extraction normalization (self-join canonicalization)
    if normalization.canonicalize_self_joins:
        topology = _default_canonicalizer.canonicalize(topology)

    return topology


# =============================================================================
# Topology GCS (Greatest Common Substructure)
# =============================================================================


def topology_gcs(
    expressions: list[RAExpression],
    normalization: TopologyNormalization | None = None,
    *,
    default_schema: str | None = None,
) -> TopologyResult:
    """Compute the common topology across multiple RA expressions.

    This is much simpler than full GCS because we only intersect:
    - Relations (set intersection by physical name)
    - Join edges (set intersection)

    No predicate relaxation or compatibility checking needed.
    Topologies are compared by fully qualified physical table names,
    making the comparison alias-insensitive.

    Args:
        expressions: List of RA expressions
        normalization: Normalization configuration (preferred)
        default_schema: Optional shorthand for normalization.default_schema

    Returns:
        TopologyResult with the common topology
    """
    # Resolve normalization config with a default schema fallback.
    if normalization is None:
        normalization = TopologyNormalization(
            default_schema=default_schema or "public",
        )

    if not expressions:
        return TopologyResult(
            success=False,
            explanation="No input expressions",
            input_count=0,
        )

    # Extract topologies (normalization applied to each)
    topologies = [extract_topology(e, normalization=normalization) for e in expressions]

    if len(topologies) == 1:
        topo = topologies[0]
        return TopologyResult(
            success=True,
            topology=topo,
            expression=topology_to_ra(topo) if not topo.is_empty() else None,
            explanation="Single expression",
            common_relations=sorted(r.physical for r in topo.relations),
            common_joins=[e.fingerprint() for e in topo.join_edges],
            input_count=1,
        )

    # Intersect relations by physical name
    # Create a mapping from physical -> TableRef (use first one found)
    physical_to_ref: dict[str, TableRef] = {}
    for topo in topologies:
        for ref in topo.relations:
            if ref.physical not in physical_to_ref:
                physical_to_ref[ref.physical] = ref

    # Find common physical names
    relation_physical_sets = [{r.physical for r in t.relations} for t in topologies]
    common_physical = set.intersection(*relation_physical_sets)

    if not common_physical:
        return TopologyResult(
            success=False,
            explanation="No common relations across expressions",
            input_count=len(expressions),
        )

    # Get TableRefs for common physical names
    common_relations = frozenset(physical_to_ref[p] for p in common_physical)

    # Intersect join edges
    edge_sets = [set(t.join_edges) for t in topologies]
    common_edges_raw = set.intersection(*edge_sets) if edge_sets else set()

    # Filter edges to only those connecting common relations
    common_edges = frozenset(
        e
        for e in common_edges_raw
        if e.left.physical in common_physical and e.right.physical in common_physical
    )

    result_topology = Topology(
        relations=common_relations,
        join_edges=common_edges,
    )

    # Convert to RA expression
    try:
        ra_expr = topology_to_ra(result_topology)
    except ValueError as e:
        return TopologyResult(
            success=False,
            topology=result_topology,
            explanation=f"Could not convert topology to RA: {e}",
            common_relations=sorted(common_physical),
            common_joins=[e.fingerprint() for e in common_edges],
            input_count=len(expressions),
        )

    return TopologyResult(
        success=True,
        topology=result_topology,
        expression=ra_expr,
        explanation=f"Common topology of {len(expressions)} expressions: {len(common_relations)} relations, {len(common_edges)} joins",
        common_relations=sorted(common_physical),
        common_joins=[e.fingerprint() for e in common_edges],
        input_count=len(expressions),
    )


def topology_to_ra(topology: Topology) -> RAExpression:
    """Convert a topology to an RA expression.

    Builds: R1 ⋈_c1 R2 ⋈_c2 R3 ...

    Uses the schema, table, and alias stored in TableRefs to reconstruct
    Relation nodes with correct schema_name, name, and alias fields.

    Uses a greedy approach to build a connected join tree:
    1. Start with an arbitrary relation
    2. Repeatedly add relations that can be joined via an edge
    3. For each join, use the appropriate equi-join condition

    Args:
        topology: The topology to convert

    Returns:
        An RA expression with the join structure

    Raises:
        ValueError: If topology is empty or disconnected
    """
    if topology.is_empty():
        raise ValueError("Cannot convert empty topology to RA")

    relations = list(topology.relations)

    # Single relation case - use to_relation() for correct reconstruction
    if len(relations) == 1:
        return relations[0].to_relation()

    # Build adjacency list with edges (using physical names as keys)
    adjacency: dict[str, list[tuple[str, JoinEdge, TableRef]]] = {r.physical: [] for r in relations}
    physical_to_ref: dict[str, TableRef] = {r.physical: r for r in relations}

    for edge in topology.join_edges:
        if edge.left.physical in adjacency and edge.right.physical in adjacency:
            adjacency[edge.left.physical].append(
                (edge.right.physical, edge, physical_to_ref[edge.right.physical])
            )
            adjacency[edge.right.physical].append(
                (edge.left.physical, edge, physical_to_ref[edge.left.physical])
            )

    # Greedy join tree construction
    # Start with first relation
    first_ref = relations[0]
    result: RAExpression = first_ref.to_relation()
    joined: set[str] = {first_ref.physical}
    remaining = {r.physical for r in relations[1:]}

    while remaining:
        # Find a relation we can join
        found = False
        for rel_physical in list(remaining):
            # Check if rel can be joined to any already-joined relation
            for joined_physical in joined:
                for neighbor_physical, edge, neighbor_ref in adjacency.get(joined_physical, []):
                    if neighbor_physical == rel_physical:
                        # Build join condition using aliases
                        condition = AtomicPredicate(
                            left=ColumnRef(table=edge.left.alias, column=edge.left_column),
                            op=ComparisonOp.EQ,
                            right=ColumnRef(table=edge.right.alias, column=edge.right_column),
                        )

                        result = Join(
                            left=result,
                            right=neighbor_ref.to_relation(),
                            join_type=JoinType.INNER,
                            condition=condition,
                        )
                        joined.add(rel_physical)
                        remaining.remove(rel_physical)
                        found = True
                        break
                if found:
                    break
            if found:
                break

        if not found and remaining:
            # No more relations can be joined via edges
            # Add remaining as cross joins (disconnected topology)
            for rel_physical in remaining:
                ref = physical_to_ref[rel_physical]
                result = Join(
                    left=result,
                    right=ref.to_relation(),
                    join_type=JoinType.CROSS,
                    condition=None,
                )
            break

    return result


# =============================================================================
# Utility Functions
# =============================================================================


def topology_similarity(t1: Topology, t2: Topology) -> float:
    """Compute Jaccard similarity between two topologies.

    Uses weighted combination of relation and edge similarity.
    Comparison is by physical table names, making it alias-insensitive.

    Args:
        t1: First topology
        t2: Second topology

    Returns:
        Similarity score between 0 and 1
    """
    # Get physical names for comparison
    t1_physical = {r.physical for r in t1.relations}
    t2_physical = {r.physical for r in t2.relations}

    # Relation similarity (Jaccard)
    rel_intersection = len(t1_physical & t2_physical)
    rel_union = len(t1_physical | t2_physical)
    rel_sim = rel_intersection / rel_union if rel_union > 0 else 1.0

    # Edge similarity (Jaccard) - edges already compare by physical names
    edge_intersection = len(t1.join_edges & t2.join_edges)
    edge_union = len(t1.join_edges | t2.join_edges)
    edge_sim = edge_intersection / edge_union if edge_union > 0 else 1.0

    # Weight edges more heavily (they're more specific)
    return 0.4 * rel_sim + 0.6 * edge_sim


def is_subtopology(small: Topology, large: Topology) -> bool:
    """Check if small is a subtopology of large.

    A topology A is a subtopology of B if:
    - A.relations ⊆ B.relations (by physical name)
    - A.join_edges ⊆ B.join_edges

    Args:
        small: Potential subtopology
        large: Potential supertopology

    Returns:
        True if small ⊆ large
    """
    small_physical = {r.physical for r in small.relations}
    large_physical = {r.physical for r in large.relations}
    return small_physical <= large_physical and small.join_edges <= large.join_edges
