"""Tests for algebrakit folding self-join module."""

from alma_algebrakit.folding.self_join import (
    analyze_self_joins,
    check_self_join_rewritable,
    create_multi_instance_column_mapping,
    detect_self_joins,
    get_view_instances_needed,
    group_by_physical_table,
)
from alma_algebrakit.models.algebra import RelationRef
from alma_algebrakit.schema.constraints import PrimaryKey, SchemaConstraints


class TestDetectSelfJoins:
    """Tests for detect_self_joins."""

    def test_detect_self_joins_with_no_self_joins_returns_false_and_groups(self) -> None:
        """detect_self_joins with no self-joins returns (False, groups)."""
        relations = {
            RelationRef(physical_table="users", alias="u"),
            RelationRef(physical_table="orders", alias="o"),
        }
        has_self_joins, groups = detect_self_joins(relations)
        assert has_self_joins is False
        assert len(groups) == 2
        assert len(groups["users"]) == 1
        assert len(groups["orders"]) == 1

    def test_detect_self_joins_with_self_join_returns_true_and_groups(self) -> None:
        """detect_self_joins with self-join returns (True, groups with 2+ instances)."""
        relations = {
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
            RelationRef(physical_table="orders", alias="o"),
        }
        has_self_joins, groups = detect_self_joins(relations)
        assert has_self_joins is True
        assert len(groups["users"]) == 2
        assert len(groups["orders"]) == 1


class TestGroupByPhysicalTable:
    """Tests for group_by_physical_table."""

    def test_group_by_physical_table_groups_correctly_case_insensitive(self) -> None:
        """group_by_physical_table groups correctly (case-insensitive)."""
        relations = {
            RelationRef(physical_table="Users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
            RelationRef(physical_table="ORDERS", alias="o"),
        }
        groups = group_by_physical_table(relations)
        assert "users" in groups
        assert len(groups["users"]) == 2
        assert "orders" in groups
        assert len(groups["orders"]) == 1


class TestCheckSelfJoinRewritable:
    """Tests for check_self_join_rewritable."""

    def test_check_self_join_rewritable_with_key_preserving_view_returns_true(self) -> None:
        """check_self_join_rewritable with key-preserving view returns (True, reason)."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        instances = [
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
        ]
        view_columns = frozenset({"id", "name", "manager_id"})
        is_safe, reason = check_self_join_rewritable(instances, view_columns, constraints)
        assert is_safe is True
        assert "key-preserving" in reason.lower() or "key preserving" in reason.lower()

    def test_check_self_join_rewritable_without_key_returns_false(self) -> None:
        """check_self_join_rewritable without key returns (False, reason)."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        instances = [
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
        ]
        view_columns = frozenset({"name", "email"})
        is_safe, reason = check_self_join_rewritable(instances, view_columns, constraints)
        assert is_safe is False
        assert "key" in reason.lower()

    def test_check_self_join_rewritable_with_single_instance_returns_true(self) -> None:
        """check_self_join_rewritable with single instance returns (True, ...)."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        instances = [RelationRef(physical_table="users", alias="u")]
        view_columns = frozenset({"name"})
        is_safe, reason = check_self_join_rewritable(instances, view_columns, constraints)
        assert is_safe is True
        assert "No self-join" in reason


class TestAnalyzeSelfJoins:
    """Tests for analyze_self_joins."""

    def test_analyze_self_joins_without_self_joins_is_rewritable_true(self) -> None:
        """analyze_self_joins without self-joins returns is_rewritable=True."""
        covered_rels = {
            RelationRef(physical_table="users", alias="u"),
            RelationRef(physical_table="orders", alias="o"),
        }
        view_columns = frozenset({"id", "name"})
        result = analyze_self_joins(covered_rels, view_columns, None)
        assert result.has_self_joins is False
        assert result.is_rewritable is True

    def test_analyze_self_joins_with_self_join_but_no_constraints_rejects(self) -> None:
        """analyze_self_joins with self-join but no constraints returns rejection."""
        covered_rels = {
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
        }
        view_columns = frozenset({"id", "name"})
        result = analyze_self_joins(covered_rels, view_columns, None)
        assert result.has_self_joins is True
        assert result.is_rewritable is False
        assert result.rejection_reason is not None
        assert "constraints" in result.rejection_reason.lower()

    def test_analyze_self_joins_with_self_join_and_key_preserving_succeeds(self) -> None:
        """analyze_self_joins with self-join and key-preserving returns success."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        covered_rels = {
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
        }
        view_columns = frozenset({"id", "name"})
        result = analyze_self_joins(covered_rels, view_columns, constraints)
        assert result.has_self_joins is True
        assert result.is_rewritable is True
        assert "users" in result.instance_aliases
        assert len(result.instance_aliases["users"]) == 2
        view_aliases = set(result.instance_aliases["users"].values())
        assert view_aliases == {"cv_1", "cv_2"}


class TestCreateMultiInstanceColumnMapping:
    """Tests for create_multi_instance_column_mapping."""

    def test_create_multi_instance_column_mapping_with_explicit_lineage(self) -> None:
        """create_multi_instance_column_mapping with explicit lineage."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        covered_rels = {
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
        }
        analysis = analyze_self_joins(
            covered_rels,
            frozenset({"id", "name"}),
            constraints,
            view_alias_prefix="cv",
        )
        column_lineage = {
            ("u1", "id"): "user_id",
            ("u1", "name"): "user_name",
            ("u2", "id"): "user_id",
            ("u2", "name"): "user_name",
        }
        rewrite_map = create_multi_instance_column_mapping(
            analysis, column_lineage, is_select_star=False
        )
        assert "u1.id" in rewrite_map
        assert "u2.id" in rewrite_map
        assert rewrite_map["u1.id"].endswith(".user_id")
        assert rewrite_map["u2.id"].endswith(".user_id")
        assert rewrite_map["u1.id"] != rewrite_map["u2.id"]

    def test_create_multi_instance_column_mapping_with_select_star(self) -> None:
        """create_multi_instance_column_mapping with SELECT *."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        covered_rels = {
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
        }
        analysis = analyze_self_joins(
            covered_rels,
            frozenset({"id", "name"}),
            constraints,
            view_alias_prefix="cv",
        )
        column_lineage = {
            ("users", "id"): "id",
            ("users", "name"): "name",
        }
        rewrite_map = create_multi_instance_column_mapping(
            analysis, column_lineage, is_select_star=True
        )
        assert "u1.id" in rewrite_map
        assert "u2.id" in rewrite_map
        assert rewrite_map["u1.id"].endswith(".id")
        assert rewrite_map["u2.id"].endswith(".id")
        assert rewrite_map["u1.id"] != rewrite_map["u2.id"]


class TestGetViewInstancesNeeded:
    """Tests for get_view_instances_needed."""

    def test_get_view_instances_needed_without_self_joins_returns_one(self) -> None:
        """get_view_instances_needed without self-joins returns 1."""
        covered_rels = {
            RelationRef(physical_table="users", alias="u"),
            RelationRef(physical_table="orders", alias="o"),
        }
        analysis = analyze_self_joins(covered_rels, frozenset({"id"}), None)
        assert get_view_instances_needed(analysis) == 1

    def test_get_view_instances_needed_with_self_joins_returns_count(self) -> None:
        """get_view_instances_needed with self-joins returns total instances."""
        constraints = SchemaConstraints()
        constraints.add_pk(PrimaryKey(table="users", columns=frozenset({"id"})))
        covered_rels = {
            RelationRef(physical_table="users", alias="u1"),
            RelationRef(physical_table="users", alias="u2"),
            RelationRef(physical_table="orders", alias="o"),
        }
        analysis = analyze_self_joins(covered_rels, frozenset({"id", "name"}), constraints)
        assert get_view_instances_needed(analysis) == 3
