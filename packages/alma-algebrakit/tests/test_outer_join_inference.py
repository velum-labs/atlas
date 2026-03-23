"""Comprehensive tests for outer join inference module.

Tests infer_join_type, infer_outer_join_safety, and analyze_boundary_joins_with_constraints
using schema constraints (FK, NOT NULL) for outer join safety analysis.
"""

from alma_algebrakit.folding.outer_join_inference import (
    JoinInferenceResult,
    OuterJoinInference,
    analyze_boundary_joins_with_constraints,
    infer_join_type,
    infer_outer_join_safety,
)
from alma_algebrakit.models.algebra import (
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    JoinType,
    RelationRef,
)
from alma_algebrakit.normalizer import JoinEdge
from alma_algebrakit.schema.constraints import (
    ForeignKey,
    PrimaryKey,
    SchemaConstraints,
)

# =============================================================================
# Test Fixtures
# =============================================================================


def _make_edge(
    join_type: JoinType,
    left_table: str,
    left_col: str,
    right_table: str,
    right_col: str,
    left_alias: str | None = None,
    right_alias: str | None = None,
) -> JoinEdge:
    """Create a JoinEdge with the given join type and predicate.

    Uses physical table names in ColumnRef for constraint matching.
    """
    left_ref = RelationRef(left_table, left_alias or left_table)
    right_ref = RelationRef(right_table, right_alias or right_table)
    return JoinEdge(
        join_type=join_type,
        left_rels=frozenset({left_ref}),
        right_rels=frozenset({right_ref}),
        on_predicate=AtomicPredicate(
            left=ColumnRef(table=left_table, column=left_col),
            op=ComparisonOp.EQ,
            right=ColumnRef(table=right_table, column=right_col),
        ),
    )


def _constraints_users_orders_fk() -> SchemaConstraints:
    """Schema: users.id PK, orders.user_id FK -> users.id."""
    constraints = SchemaConstraints()
    constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
    constraints.add_fk(
        ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
    )
    constraints.add_not_null("users", "id")
    constraints.add_not_null("orders", "user_id")
    return constraints


def _constraints_symmetric_fk() -> SchemaConstraints:
    """Schema with symmetric FK: users <-> profiles."""
    constraints = SchemaConstraints()
    constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
    constraints.add_pk(PrimaryKey(table="profiles", columns=frozenset({"user_id"})))
    constraints.add_fk(
        ForeignKey(
            from_table="profiles",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
    )
    constraints.add_fk(
        ForeignKey(
            from_table="users",
            from_columns=("id",),
            to_table="profiles",
            to_columns=("user_id",),
        )
    )
    constraints.add_not_null("users", "id")
    constraints.add_not_null("profiles", "user_id")
    return constraints


# =============================================================================
# infer_join_type Tests
# =============================================================================


class TestInferJoinType:
    """Tests for infer_join_type function."""

    def test_inner_join_returns_inner(self) -> None:
        """INNER join returns INNER (no inference needed)."""
        constraints = SchemaConstraints()
        result = infer_join_type(
            JoinType.INNER,
            "users",
            "id",
            "orders",
            "user_id",
            constraints,
        )
        assert isinstance(result, OuterJoinInference)
        assert result.original_type == JoinType.INNER
        assert result.effective_type == JoinType.INNER
        assert result.can_infer_inner
        assert "inner" in result.reason.lower() or "cross" in result.reason.lower()

    def test_cross_join_returns_cross(self) -> None:
        """CROSS join returns CROSS (no inference needed)."""
        constraints = SchemaConstraints()
        result = infer_join_type(
            JoinType.CROSS,
            "users",
            "id",
            "orders",
            "user_id",
            constraints,
        )
        assert result.original_type == JoinType.CROSS
        assert result.effective_type == JoinType.CROSS
        assert result.can_infer_inner

    def test_left_join_to_inner_with_fk_and_not_null(self) -> None:
        """LEFT JOIN -> INNER with FK + NOT NULL on right column."""
        constraints = _constraints_users_orders_fk()
        # orders LEFT JOIN users ON orders.user_id = users.id
        result = infer_join_type(
            JoinType.LEFT,
            "orders",
            "user_id",
            "users",
            "id",
            constraints,
        )
        assert result.can_infer_inner
        assert result.effective_type == JoinType.INNER
        assert result.original_type == JoinType.LEFT

    def test_left_join_unchanged_without_fk(self) -> None:
        """LEFT JOIN unchanged without FK."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        constraints.add_not_null("users", "id")
        # No FK from orders.user_id to users
        result = infer_join_type(
            JoinType.LEFT,
            "orders",
            "user_id",
            "users",
            "id",
            constraints,
        )
        assert not result.can_infer_inner
        assert result.effective_type == JoinType.LEFT
        assert "FK" in result.reason or "No FK" in result.reason

    def test_left_join_unchanged_without_not_null_on_right(self) -> None:
        """LEFT JOIN unchanged without NOT NULL on right column."""
        constraints = SchemaConstraints()
        constraints.add_fk(
            ForeignKey(
                from_table="orders",
                from_columns=("user_id",),
                to_table="users",
                to_columns=("id",),
            )
        )
        # users.id NOT NULL from PK - but we need right col (users.id) NOT NULL
        # Actually PK adds NOT NULL. Let's use a different column - orders.optional_ref
        constraints.add_fk(
            ForeignKey(
                from_table="orders",
                from_columns=("optional_ref",),
                to_table="refs",
                to_columns=("id",),
            )
        )
        # refs.id not marked NOT NULL
        result = infer_join_type(
            JoinType.LEFT,
            "orders",
            "optional_ref",
            "refs",
            "id",
            constraints,
        )
        assert not result.can_infer_inner
        assert result.effective_type == JoinType.LEFT
        assert "NOT NULL" in result.reason

    def test_right_join_to_inner_with_fk_and_not_null(self) -> None:
        """RIGHT JOIN -> INNER with FK + NOT NULL on left column."""
        constraints = _constraints_users_orders_fk()
        # users RIGHT JOIN orders ON users.id = orders.user_id
        # For RIGHT: left is null-extended, need FK R->L and L.col NOT NULL
        result = infer_join_type(
            JoinType.RIGHT,
            "users",
            "id",
            "orders",
            "user_id",
            constraints,
        )
        assert result.can_infer_inner
        assert result.effective_type == JoinType.INNER
        assert result.original_type == JoinType.RIGHT

    def test_right_join_unchanged_without_fk(self) -> None:
        """RIGHT JOIN unchanged without FK."""
        constraints = SchemaConstraints()
        constraints.add_not_null("users", "id")
        result = infer_join_type(
            JoinType.RIGHT,
            "users",
            "id",
            "orders",
            "user_id",
            constraints,
        )
        assert not result.can_infer_inner
        assert result.effective_type == JoinType.RIGHT

    def test_full_outer_to_inner_with_symmetric_fk(self) -> None:
        """FULL OUTER -> INNER with symmetric FK + NOT NULL both directions."""
        constraints = _constraints_symmetric_fk()
        result = infer_join_type(
            JoinType.FULL,
            "users",
            "id",
            "profiles",
            "user_id",
            constraints,
        )
        assert result.can_infer_inner
        assert result.effective_type == JoinType.INNER
        assert "Symmetric" in result.reason

    def test_join_inference_result_enum_values(self) -> None:
        """JoinInferenceResult enum has expected values for mapping."""
        assert JoinInferenceResult.INNER.value == "inner"
        assert JoinInferenceResult.UNCHANGED.value == "unchanged"
        assert JoinInferenceResult.UNKNOWN.value == "unknown"

    def test_full_outer_unchanged_with_one_direction_fk(self) -> None:
        """FULL OUTER unchanged with only one direction FK."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        constraints.add_pk(PrimaryKey(table="profiles", columns=frozenset({"user_id"})))
        constraints.add_fk(
            ForeignKey(
                from_table="profiles",
                from_columns=("user_id",),
                to_table="users",
                to_columns=("id",),
            )
        )
        constraints.add_not_null("users", "id")
        constraints.add_not_null("profiles", "user_id")
        # Only profiles->users FK, no users->profiles
        result = infer_join_type(
            JoinType.FULL,
            "users",
            "id",
            "profiles",
            "user_id",
            constraints,
        )
        assert not result.can_infer_inner
        assert result.effective_type == JoinType.FULL
        assert "symmetric" in result.reason.lower() or "R→L" in result.reason


# =============================================================================
# infer_outer_join_safety Tests
# =============================================================================


class TestInferOuterJoinSafety:
    """Tests for infer_outer_join_safety function."""

    def test_inner_join_at_boundary_always_safe(self) -> None:
        """Inner join at boundary is always safe."""
        edge = _make_edge(JoinType.INNER, "users", "id", "orders", "user_id")
        covered = {RelationRef("users", "users")}
        remaining = {RelationRef("orders", "orders")}
        constraints = SchemaConstraints()

        is_safe, reason, effective_type = infer_outer_join_safety(
            edge, covered, remaining, constraints
        )
        assert is_safe
        assert "inner" in reason.lower()
        assert effective_type == JoinType.INNER

    def test_left_join_covered_on_preserved_side_safe(self) -> None:
        """LEFT JOIN with covered on preserved (left) side is safe."""
        edge = _make_edge(JoinType.LEFT, "users", "id", "orders", "user_id")
        covered = {RelationRef("users", "users")}
        remaining = {RelationRef("orders", "orders")}

        is_safe, reason, effective_type = infer_outer_join_safety(
            edge, covered, remaining, SchemaConstraints()
        )
        assert is_safe
        assert "preserved" in reason.lower() and "left" in reason.lower()
        assert effective_type == JoinType.LEFT

    def test_left_join_covered_on_null_extended_unsafe_without_constraints(
        self,
    ) -> None:
        """LEFT JOIN with covered on null-extended (right) side - unsafe without constraints."""
        edge = _make_edge(JoinType.LEFT, "users", "id", "orders", "user_id")
        covered = {RelationRef("orders", "orders")}
        remaining = {RelationRef("users", "users")}

        is_safe, reason, effective_type = infer_outer_join_safety(
            edge, covered, remaining, SchemaConstraints()
        )
        assert not is_safe
        assert "null-extended" in reason.lower() or "right" in reason.lower()
        assert effective_type is None

    def test_left_join_covered_on_null_extended_safe_with_fk_not_null(
        self,
    ) -> None:
        """LEFT JOIN with covered on null-extended side - safe with FK+NOT NULL."""
        # orders LEFT JOIN users ON orders.user_id = users.id
        # Left=orders (preserved), right=users (null-extended)
        # Covered=users (right), remaining=orders (left) -> covered on null-extended
        # FK orders.user_id -> users.id + users.id NOT NULL allows INNER inference
        edge = _make_edge(JoinType.LEFT, "orders", "user_id", "users", "id")
        covered = {RelationRef("users", "users")}
        remaining = {RelationRef("orders", "orders")}
        constraints = _constraints_users_orders_fk()

        is_safe, reason, effective_type = infer_outer_join_safety(
            edge, covered, remaining, constraints
        )
        assert is_safe
        assert effective_type == JoinType.INNER

    def test_right_join_covered_on_preserved_side_safe(self) -> None:
        """RIGHT JOIN with covered on preserved (right) side is safe."""
        edge = _make_edge(JoinType.RIGHT, "users", "id", "orders", "user_id")
        covered = {RelationRef("orders", "orders")}
        remaining = {RelationRef("users", "users")}

        is_safe, reason, effective_type = infer_outer_join_safety(
            edge, covered, remaining, SchemaConstraints()
        )
        assert is_safe
        assert "preserved" in reason.lower() and "right" in reason.lower()
        assert effective_type == JoinType.RIGHT


# =============================================================================
# analyze_boundary_joins_with_constraints Tests
# =============================================================================


class TestAnalyzeBoundaryJoinsWithConstraints:
    """Tests for analyze_boundary_joins_with_constraints function."""

    def test_all_boundary_joins_safe_returns_effective_types(self) -> None:
        """All boundary joins safe -> returns (True, [], effective_types)."""
        edge = _make_edge(JoinType.LEFT, "users", "id", "orders", "user_id")
        covered = {RelationRef("users", "users")}
        remaining = {RelationRef("orders", "orders")}
        constraints = SchemaConstraints()

        all_safe, reasons, effective_types = analyze_boundary_joins_with_constraints(
            [edge], covered, remaining, constraints
        )
        assert all_safe
        assert reasons == []
        assert len(effective_types) >= 1

    def test_one_unsafe_boundary_join_returns_reasons(self) -> None:
        """One unsafe boundary join -> returns (False, [reason], {})."""
        edge = _make_edge(JoinType.LEFT, "users", "id", "orders", "user_id")
        covered = {RelationRef("orders", "orders")}
        remaining = {RelationRef("users", "users")}
        constraints = SchemaConstraints()

        all_safe, reasons, effective_types = analyze_boundary_joins_with_constraints(
            [edge], covered, remaining, constraints
        )
        assert not all_safe
        assert len(reasons) >= 1
        assert any("null-extended" in r.lower() or "right" in r.lower() for r in reasons)

    def test_without_constraints_falls_back_to_positional(self) -> None:
        """Without constraints -> falls back to positional analysis."""
        edge = _make_edge(JoinType.LEFT, "users", "id", "orders", "user_id")
        covered = {RelationRef("orders", "orders")}
        remaining = {RelationRef("users", "users")}

        all_safe, reasons, effective_types = analyze_boundary_joins_with_constraints(
            [edge], covered, remaining, None
        )
        assert not all_safe
        assert len(reasons) >= 1
        # Positional analysis produces specific message
        assert any("null-extended" in r.lower() or "right" in r.lower() for r in reasons)
