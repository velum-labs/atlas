"""Tests for algebrakit schema constraints module."""

import pytest

from alma_algebrakit.schema.constraints import (
    ForeignKey,
    PrimaryKey,
    SchemaConstraints,
    UniqueConstraint,
)


class TestPrimaryKey:
    """Tests for PrimaryKey."""

    def test_create_pk_with_single_column_verify_covers_and_matches(self):
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        assert pk.table == "users"
        assert pk.columns == frozenset({"id"})
        assert pk.covers(frozenset({"id"}))
        assert pk.covers(frozenset({"id", "name"}))
        assert pk.matches(frozenset({"id"}))
        assert not pk.matches(frozenset({"id", "name"}))

    def test_create_pk_with_multiple_columns(self):
        pk = PrimaryKey(table="orders", columns=frozenset({"order_id", "line_no"}))
        assert pk.table == "orders"
        assert pk.columns == frozenset({"order_id", "line_no"})
        assert pk.matches(frozenset({"order_id", "line_no"}))

    def test_empty_columns_raises_value_error(self):
        with pytest.raises(ValueError, match="at least one column"):
            PrimaryKey(table="users", columns=frozenset())

    def test_covers_returns_true_when_pk_columns_are_subset(self):
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        assert pk.covers(frozenset({"id", "name", "email"}))
        assert pk.covers(frozenset({"id"}))

    def test_covers_returns_false_when_pk_columns_not_subset(self):
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        assert not pk.covers(frozenset({"name"}))
        assert not pk.covers(frozenset())

    def test_matches_returns_true_only_for_exact_match(self):
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        assert pk.matches(frozenset({"id"}))
        assert not pk.matches(frozenset({"id", "name"}))
        assert not pk.matches(frozenset({"name"}))


class TestForeignKey:
    """Tests for ForeignKey."""

    def test_create_fk_with_single_column_pair(self):
        fk = ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
        assert fk.from_table == "orders"
        assert fk.from_columns == ("user_id",)
        assert fk.to_table == "users"
        assert fk.to_columns == ("id",)

    def test_create_fk_with_multiple_column_pairs(self):
        fk = ForeignKey(
            from_table="order_items",
            from_columns=("order_id", "line_no"),
            to_table="orders",
            to_columns=("order_id", "line_no"),
        )
        assert fk.from_columns == ("order_id", "line_no")
        assert fk.to_columns == ("order_id", "line_no")

    def test_mismatched_column_counts_raises_value_error(self):
        with pytest.raises(ValueError, match="column counts must match"):
            ForeignKey(
                from_table="orders",
                from_columns=("user_id", "other"),
                to_table="users",
                to_columns=("id",),
            )

    def test_empty_columns_raises_value_error(self):
        with pytest.raises(ValueError, match="at least one column"):
            ForeignKey(
                from_table="orders",
                from_columns=(),
                to_table="users",
                to_columns=(),
            )

    def test_references_matches_case_insensitive(self):
        fk = ForeignKey(
            from_table="Orders",
            from_columns=("user_id",),
            to_table="Users",
            to_columns=("id",),
        )
        assert fk.references("orders", "user_id", "users")
        assert fk.references("ORDERS", "USER_ID", "USERS")
        assert fk.references("Orders", "User_Id", "Users")  # case-insensitive

    def test_references_returns_false_for_wrong_table(self):
        fk = ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
        assert not fk.references("orders", "user_id", "products")
        assert not fk.references("products", "user_id", "users")
        assert not fk.references("orders", "other_col", "users")

    def test_get_referenced_column_returns_correct_to_column(self):
        fk = ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
        assert fk.get_referenced_column("user_id") == "id"
        assert fk.get_referenced_column("USER_ID") == "id"
        assert fk.get_referenced_column("other") is None

    def test_get_referenced_column_multiple_columns(self):
        fk = ForeignKey(
            from_table="order_items",
            from_columns=("order_id", "line_no"),
            to_table="orders",
            to_columns=("order_id", "line_no"),
        )
        assert fk.get_referenced_column("order_id") == "order_id"
        assert fk.get_referenced_column("line_no") == "line_no"


class TestUniqueConstraint:
    """Tests for UniqueConstraint."""

    def test_create_unique_constraint(self):
        uc = UniqueConstraint(table="users", columns=frozenset({"email"}))
        assert uc.table == "users"
        assert uc.columns == frozenset({"email"})
        assert uc.name is None

    def test_create_unique_constraint_with_name(self):
        uc = UniqueConstraint(
            table="users",
            columns=frozenset({"email"}),
            name="users_email_key",
        )
        assert uc.name == "users_email_key"

    def test_empty_columns_raises_value_error(self):
        with pytest.raises(ValueError, match="at least one column"):
            UniqueConstraint(table="users", columns=frozenset())

    def test_covers_works_correctly(self):
        uc = UniqueConstraint(table="users", columns=frozenset({"email"}))
        assert uc.covers(frozenset({"email"}))
        assert uc.covers(frozenset({"email", "name"}))
        assert not uc.covers(frozenset({"name"}))
        assert not uc.covers(frozenset())


class TestSchemaConstraints:
    """Tests for SchemaConstraints."""

    def test_add_pk_implies_not_null_on_pk_columns(self):
        constraints = SchemaConstraints()
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        constraints.add_pk(pk)
        assert constraints.is_not_null("users", "id")
        assert constraints.is_not_null("USERS", "ID")  # case-insensitive

    def test_add_pk_multiple_columns_all_not_null(self):
        constraints = SchemaConstraints()
        pk = PrimaryKey(table="orders", columns=frozenset({"order_id", "line_no"}))
        constraints.add_pk(pk)
        assert constraints.is_not_null("orders", "order_id")
        assert constraints.is_not_null("orders", "line_no")

    def test_is_key_preserving_returns_true_when_projected_columns_include_key(self):
        constraints = SchemaConstraints()
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        constraints.add_pk(pk)
        assert constraints.is_key_preserving("users", {"id", "name", "email"})
        assert constraints.is_key_preserving("users", {"id"})

    def test_is_key_preserving_returns_false_when_key_columns_missing(self):
        constraints = SchemaConstraints()
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        constraints.add_pk(pk)
        assert not constraints.is_key_preserving("users", {"name", "email"})
        assert not constraints.is_key_preserving("users", set())

    def test_is_key_preserving_with_unique_constraint(self):
        constraints = SchemaConstraints()
        uc = UniqueConstraint(table="users", columns=frozenset({"email"}))
        constraints.add_unique(uc)
        assert constraints.is_key_preserving("users", {"email", "name"})
        assert not constraints.is_key_preserving("users", {"name"})

    def test_can_infer_inner_join_returns_true_with_fk_and_not_null(self):
        constraints = SchemaConstraints()
        fk = ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
        constraints.add_fk(fk)
        constraints.add_not_null("users", "id")
        can_infer, reason = constraints.can_infer_inner_join("orders", "user_id", "users", "id")
        assert can_infer
        assert "FK + NOT NULL" in reason

    def test_can_infer_inner_join_returns_false_without_fk(self):
        constraints = SchemaConstraints()
        constraints.add_not_null("users", "id")
        can_infer, reason = constraints.can_infer_inner_join("orders", "user_id", "users", "id")
        assert not can_infer
        assert "No FK" in reason or "FK" in reason

    def test_can_infer_inner_join_returns_false_without_not_null(self):
        constraints = SchemaConstraints()
        fk = ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
        constraints.add_fk(fk)
        can_infer, reason = constraints.can_infer_inner_join("orders", "user_id", "users", "id")
        assert not can_infer
        assert "not NOT NULL" in reason

    def test_can_infer_inner_from_right_join_works_symmetrically(self):
        constraints = SchemaConstraints()
        # RIGHT JOIN: orders RIGHT JOIN users ON orders.user_id = users.id
        # Left (orders) is null-extended. Need: orders.user_id NOT NULL, FK users.id -> orders.user_id
        fk = ForeignKey(
            from_table="users",
            from_columns=("id",),
            to_table="orders",
            to_columns=("user_id",),
        )
        constraints.add_fk(fk)
        constraints.add_not_null("orders", "user_id")
        can_infer, reason = constraints.can_infer_inner_from_right_join(
            "orders", "user_id", "users", "id"
        )
        assert can_infer
        assert "FK + NOT NULL" in reason

    def test_can_infer_inner_from_right_join_requires_left_not_null(self):
        constraints = SchemaConstraints()
        fk = ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
        constraints.add_fk(fk)
        # users.id NOT NULL but we need orders.user_id NOT NULL for RIGHT JOIN
        constraints.add_not_null("users", "id")
        can_infer, reason = constraints.can_infer_inner_from_right_join(
            "orders", "user_id", "users", "id"
        )
        assert not can_infer
        assert "orders.user_id" in reason or "not NOT NULL" in reason

    def test_get_keys_returns_both_pk_and_unique(self):
        constraints = SchemaConstraints()
        pk = PrimaryKey(table="users", columns=frozenset({"id"}))
        uc = UniqueConstraint(table="users", columns=frozenset({"email"}))
        constraints.add_pk(pk)
        constraints.add_unique(uc)
        keys = constraints.get_keys("users")
        assert len(keys) == 2
        assert frozenset({"id"}) in keys
        assert frozenset({"email"}) in keys

    def test_from_catalog_metadata_creates_constraints_correctly(self):
        constraints = SchemaConstraints.from_catalog_metadata(
            primary_keys={"users": ["id"], "orders": ["id"]},
            foreign_keys=[
                {
                    "from_table": "orders",
                    "from_columns": ["user_id"],
                    "to_table": "users",
                    "to_columns": ["id"],
                },
            ],
            not_null_columns=[("users", "name")],
        )
        assert constraints.get_pk("users") is not None
        assert constraints.get_pk("users").columns == frozenset({"id"})
        assert constraints.get_pk("orders").columns == frozenset({"id"})
        assert constraints.has_fk("orders", "user_id", "users")
        assert constraints.is_not_null("users", "id")  # from PK
        assert constraints.is_not_null("users", "name")  # explicit

    def test_from_catalog_metadata_with_string_columns(self):
        constraints = SchemaConstraints.from_catalog_metadata(
            foreign_keys=[
                {
                    "from_table": "orders",
                    "from_columns": "user_id",
                    "to_table": "users",
                    "to_columns": "id",
                },
            ],
        )
        assert constraints.has_fk("orders", "user_id", "users")

    def test_has_fk_and_get_fk_work_correctly(self):
        constraints = SchemaConstraints()
        fk = ForeignKey(
            from_table="orders",
            from_columns=("user_id",),
            to_table="users",
            to_columns=("id",),
        )
        constraints.add_fk(fk)
        assert constraints.has_fk("orders", "user_id", "users")
        assert not constraints.has_fk("orders", "other", "users")
        retrieved = constraints.get_fk("orders", "user_id", "users")
        assert retrieved is not None
        assert retrieved.from_table == "orders"
        assert retrieved.to_table == "users"
        assert constraints.get_fk("orders", "other", "users") is None
