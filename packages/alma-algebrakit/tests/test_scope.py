"""Tests for algebrakit scope and resolution."""

import pytest

from algebrakit import (
    AmbiguousColumnError,
    ColumnSchema,
    DataType,
    RelationInstance,
    Scope,
    SQLDataType,
    TableSchema,
    UnresolvedColumnError,
    resolve,
    resolve_star,
)


@pytest.fixture
def users_schema():
    """Create a users table schema."""
    return TableSchema(
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


@pytest.fixture
def orders_schema():
    """Create an orders table schema."""
    return TableSchema(
        name="orders",
        table_id="public.orders",
        columns=[
            ColumnSchema(
                name="id",
                column_id="public.orders.id",
                data_type=DataType(base_type=SQLDataType.INTEGER),
            ),
            ColumnSchema(
                name="user_id",
                column_id="public.orders.user_id",
                data_type=DataType(base_type=SQLDataType.INTEGER),
            ),
        ],
    )


class TestRelationInstance:
    """Tests for RelationInstance."""

    def test_create_instance(self, users_schema):
        instance = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        assert instance.table_id == "public.users"
        assert instance.alias == "u"

    def test_has_column(self, users_schema):
        instance = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        assert instance.has_column("id")
        assert instance.has_column("name")
        assert not instance.has_column("nonexistent")


class TestScope:
    """Tests for Scope."""

    def test_add_relation(self, users_schema):
        scope = Scope()
        instance = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        scope.add_relation(instance)

        retrieved = scope.get_relation("u")
        assert retrieved is not None
        assert retrieved.table_id == "public.users"

    def test_get_relation_not_found(self):
        scope = Scope()
        assert scope.get_relation("nonexistent") is None

    def test_create_child_scope(self, users_schema):
        parent = Scope()
        instance = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        parent.add_relation(instance)

        child = parent.create_child_scope()
        # Child scopes typically DON'T inherit parent relations directly
        # unless they're LATERAL. The scope isolation is intentional.
        # For subqueries, we need explicit "outer" access.
        # So this test was incorrect - let's verify child is created
        assert child is not None
        assert child.parent is parent


class TestResolve:
    """Tests for resolve function."""

    def test_resolve_qualified_column(self, users_schema):
        scope = Scope()
        instance = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        scope.add_relation(instance)

        ref = resolve(scope, "id", "u")
        assert ref.column_id == "public.users.id"
        assert ref.table_alias == "u"

    def test_resolve_unqualified_column(self, users_schema):
        scope = Scope()
        instance = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        scope.add_relation(instance)

        ref = resolve(scope, "id")
        assert ref.column_id == "public.users.id"

    def test_resolve_ambiguous_column(self, users_schema, orders_schema):
        """Both tables have 'id' column - should raise ambiguous error."""
        scope = Scope()
        scope.add_relation(
            RelationInstance(
                table_id="public.users",
                alias="u",
                schema=users_schema,
            )
        )
        scope.add_relation(
            RelationInstance(
                table_id="public.orders",
                alias="o",
                schema=orders_schema,
            )
        )

        with pytest.raises(AmbiguousColumnError) as exc_info:
            resolve(scope, "id")
        assert "id" in str(exc_info.value)

    def test_resolve_unresolved_column(self, users_schema):
        scope = Scope()
        scope.add_relation(
            RelationInstance(
                table_id="public.users",
                alias="u",
                schema=users_schema,
            )
        )

        with pytest.raises(UnresolvedColumnError):
            resolve(scope, "nonexistent_column")

    def test_resolve_unresolved_table(self, users_schema):
        scope = Scope()
        scope.add_relation(
            RelationInstance(
                table_id="public.users",
                alias="u",
                schema=users_schema,
            )
        )

        with pytest.raises(UnresolvedColumnError):
            resolve(scope, "id", "nonexistent_table")


class TestResolveStar:
    """Tests for resolve_star function."""

    def test_resolve_star_all(self, users_schema, orders_schema):
        scope = Scope()
        scope.add_relation(
            RelationInstance(
                table_id="public.users",
                alias="u",
                schema=users_schema,
            )
        )
        scope.add_relation(
            RelationInstance(
                table_id="public.orders",
                alias="o",
                schema=orders_schema,
            )
        )

        refs = resolve_star(scope)
        # Should have 4 columns: users.id, users.name, orders.id, orders.user_id
        assert len(refs) == 4

    def test_resolve_star_specific_table(self, users_schema, orders_schema):
        scope = Scope()
        scope.add_relation(
            RelationInstance(
                table_id="public.users",
                alias="u",
                schema=users_schema,
            )
        )
        scope.add_relation(
            RelationInstance(
                table_id="public.orders",
                alias="o",
                schema=orders_schema,
            )
        )

        refs = resolve_star(scope, "u")
        # Should have 2 columns: users.id, users.name
        assert len(refs) == 2
        assert all(r.table_alias == "u" for r in refs)

    def test_resolve_star_unknown_table(self, users_schema):
        scope = Scope()
        scope.add_relation(
            RelationInstance(
                table_id="public.users",
                alias="u",
                schema=users_schema,
            )
        )

        with pytest.raises(UnresolvedColumnError):
            resolve_star(scope, "nonexistent")


class TestScopeResolutionConsistency:
    """Tests for consistent scope resolution order (CTEs shadow relations)."""

    def test_cte_shadows_relation_same_name(self, users_schema):
        """CTE with same name as relation should shadow it."""
        scope = Scope()

        # Add relation with alias "u"
        relation_instance = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        scope.add_relation(relation_instance)

        # Add CTE with same alias "u" - should shadow relation
        cte_instance = RelationInstance(
            table_id="public.cte_users",
            alias="u",
            schema=users_schema,
        )
        scope.add_cte("u", cte_instance)

        # get_relation should return CTE (CTEs checked first)
        retrieved = scope.get_relation("u")
        assert retrieved is not None
        assert retrieved.table_id == "public.cte_users"
        assert retrieved.is_cte is True

        # find_attribute_in_relations should also find CTE first
        matches = scope.find_attribute_in_relations("id")
        assert len(matches) == 1
        assert matches[0][0] == "u"  # CTE alias
        assert matches[0][1].table_id == "public.cte_users"

    def test_parent_cte_always_visible(self, users_schema):
        """Parent CTEs should always be visible to child scopes."""
        parent = Scope()
        parent_cte = RelationInstance(
            table_id="public.parent_cte",
            alias="parent_cte",
            schema=users_schema,
        )
        parent.add_cte("parent_cte", parent_cte)

        # Create non-LATERAL child scope
        child = parent.create_child_scope(lateral=False)

        # Parent CTE should be visible via get_relation
        retrieved = child.get_relation("parent_cte")
        assert retrieved is not None
        assert retrieved.table_id == "public.parent_cte"

        # Parent CTE should be visible via find_attribute_in_relations
        matches = child.find_attribute_in_relations("id")
        assert len(matches) == 1
        assert matches[0][0] == "_outer.parent_cte"
        assert matches[0][1].table_id == "public.parent_cte"

    def test_parent_relation_only_visible_lateral(self, users_schema):
        """Parent relations should only be visible for LATERAL subqueries.

        Note: Parent relations are visible via find_attribute_in_relations()
        for LATERAL scopes, but NOT via get_relation() - this is intentional
        because get_relation() is for direct alias lookup, while
        find_attribute_in_relations() is for column resolution where LATERAL
        visibility matters.
        """
        parent = Scope()
        parent_rel = RelationInstance(
            table_id="public.users",
            alias="u",
            schema=users_schema,
        )
        parent.add_relation(parent_rel)

        # Non-LATERAL child should NOT see parent relation
        child_nonlateral = parent.create_child_scope(lateral=False)
        assert child_nonlateral.get_relation("u") is None

        matches = child_nonlateral.find_attribute_in_relations("id")
        assert len(matches) == 0

        # LATERAL child: parent relation visible via find_attribute_in_relations
        child_lateral = parent.create_child_scope(lateral=True)

        # get_relation does NOT return parent relations (by design)
        assert child_lateral.get_relation("u") is None

        # But find_attribute_in_relations DOES see parent relations for LATERAL
        matches = child_lateral.find_attribute_in_relations("id")
        assert len(matches) == 1
        assert matches[0][0] == "_outer.u"

    def test_all_relations_no_duplicate_parent_ctes(self, users_schema):
        """all_relations() should not duplicate parent CTEs for LATERAL scopes."""
        parent = Scope()
        parent_cte = RelationInstance(
            table_id="public.parent_cte",
            alias="parent_cte",
            schema=users_schema,
        )
        parent.add_cte("parent_cte", parent_cte)

        # Create LATERAL child scope
        child = parent.create_child_scope(lateral=True)

        all_rels = child.all_relations()

        # Parent CTE should appear exactly once
        assert "parent_cte" in all_rels
        assert all_rels["parent_cte"].table_id == "public.parent_cte"

        # Count occurrences
        parent_cte_count = sum(
            1 for rel in all_rels.values() if rel.table_id == "public.parent_cte"
        )
        assert parent_cte_count == 1

    def test_resolution_order_cte_before_relation(self, users_schema):
        """Resolution order: CTEs should be checked before relations."""
        scope = Scope()

        # Add relation first
        relation_instance = RelationInstance(
            table_id="public.users",
            alias="x",
            schema=users_schema,
        )
        scope.add_relation(relation_instance)

        # Add CTE with different name but same column
        cte_instance = RelationInstance(
            table_id="public.cte_table",
            alias="y",
            schema=users_schema,
        )
        scope.add_cte("y", cte_instance)

        # Both should be found, but order matters
        matches = scope.find_attribute_in_relations("id")
        assert len(matches) == 2

        # CTE should come first (checked before relations)
        assert matches[0][0] == "y"  # CTE
        assert matches[1][0] == "x"  # Relation

    def test_nested_scopes_resolution_order(self, users_schema):
        """Test resolution order in nested scopes with CTEs and relations."""
        # Parent scope with CTE and relation
        parent = Scope()
        parent_cte = RelationInstance(
            table_id="public.parent_cte",
            alias="parent_cte",
            schema=users_schema,
        )
        parent.add_cte("parent_cte", parent_cte)

        parent_rel = RelationInstance(
            table_id="public.parent_rel",
            alias="parent_rel",
            schema=users_schema,
        )
        parent.add_relation(parent_rel)

        # Child scope (LATERAL) with its own CTE and relation
        child = parent.create_child_scope(lateral=True)
        child_cte = RelationInstance(
            table_id="public.child_cte",
            alias="child_cte",
            schema=users_schema,
        )
        child.add_cte("child_cte", child_cte)

        child_rel = RelationInstance(
            table_id="public.child_rel",
            alias="child_rel",
            schema=users_schema,
        )
        child.add_relation(child_rel)

        # Resolution order should be:
        # 1. child_cte (current CTE)
        # 2. child_rel (current relation)
        # 3. _outer.parent_cte (parent CTE - always visible)
        # 4. _outer.parent_rel (parent relation - LATERAL)
        matches = child.find_attribute_in_relations("id")
        assert len(matches) == 4

        # Verify order
        assert matches[0][0] == "child_cte"
        assert matches[1][0] == "child_rel"
        assert matches[2][0] == "_outer.parent_cte"
        assert matches[3][0] == "_outer.parent_rel"

        # Verify get_relation follows same precedence
        assert child.get_relation("child_cte").table_id == "public.child_cte"
        assert child.get_relation("child_rel").table_id == "public.child_rel"
        assert child.get_relation("parent_cte").table_id == "public.parent_cte"
        assert (
            child.get_relation("parent_rel") is None
        )  # Parent relations not accessible via get_relation (only via LATERAL in find_attribute)
