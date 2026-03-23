"""Tests for algebrakit naming qualified and aliases modules."""


from alma_algebrakit.naming.aliases import (
    effective_table_name,
    generate_column_alias,
    generate_subquery_alias,
)
from alma_algebrakit.naming.qualified import QualifiedName, normalize_name, parse_parts


class TestParseParts:
    """Tests for parse_parts."""

    def test_parse_parts_id_returns_four_nones_and_id(self) -> None:
        """parse_parts('id') returns (None, None, None, 'id')."""
        result = parse_parts("id")
        assert result == (None, None, None, "id")

    def test_parse_parts_users_id_returns_table_and_column(self) -> None:
        """parse_parts('users.id') returns (None, None, 'users', 'id')."""
        result = parse_parts("users.id")
        assert result == (None, None, "users", "id")

    def test_parse_parts_public_users_id_returns_schema_table_column(self) -> None:
        """parse_parts('public.users.id') returns (None, 'public', 'users', 'id')."""
        result = parse_parts("public.users.id")
        assert result == (None, "public", "users", "id")

    def test_parse_parts_mydb_public_users_id_returns_full(self) -> None:
        """parse_parts('mydb.public.users.id') returns ('mydb', 'public', 'users', 'id')."""
        result = parse_parts("mydb.public.users.id")
        assert result == ("mydb", "public", "users", "id")


class TestNormalizeName:
    """Tests for normalize_name."""

    def test_normalize_name_users_returns_lowercase(self) -> None:
        """normalize_name('Users') returns 'users'."""
        assert normalize_name("Users") == "users"

    def test_normalize_name_quoted_mytable_returns_lowercase_unquoted(self) -> None:
        """normalize_name('\"MyTable\"') returns 'mytable'."""
        assert normalize_name('"MyTable"') == "mytable"


class TestQualifiedName:
    """Tests for QualifiedName."""

    def test_parse_and_to_string_roundtrip(self) -> None:
        """QualifiedName.parse() and to_string() roundtrip."""
        dotted = "mydb.public.users.id"
        qn = QualifiedName.parse(dotted)
        assert qn.to_string() == dotted

    def test_matches_with_partial_qualification(self) -> None:
        """QualifiedName.matches() with partial qualification."""
        full = QualifiedName.parse("public.users.id")
        partial = QualifiedName(schema=None, table="users", name="id")
        assert full.matches(partial) is True
        different = QualifiedName(table="orders", name="id")
        assert full.matches(different) is False


class TestEffectiveTableName:
    """Tests for effective_table_name."""

    def test_effective_table_name_with_alias(self) -> None:
        """effective_table_name with alias returns alias."""
        assert effective_table_name("o", "orders") == "o"

    def test_effective_table_name_without_alias(self) -> None:
        """effective_table_name without alias returns table name."""
        assert effective_table_name(None, "orders") == "orders"


class TestGenerateSubqueryAlias:
    """Tests for generate_subquery_alias."""

    def test_generate_subquery_alias_returns_unique_values_for_different_inputs(
        self,
    ) -> None:
        """generate_subquery_alias returns unique values for different inputs."""
        alias1 = generate_subquery_alias("sq1")
        alias2 = generate_subquery_alias("sq2")
        assert alias1 != alias2
        assert alias1 == "sq1"
        assert alias2 == "sq2"

    def test_generate_subquery_alias_returns_default_for_none(self) -> None:
        """generate_subquery_alias returns default when given None."""
        alias = generate_subquery_alias(None)
        assert alias == "_subquery"


class TestGenerateColumnAlias:
    """Tests for generate_column_alias."""

    def test_generate_column_alias_generates_valid_names(self) -> None:
        """generate_column_alias generates valid names."""
        alias0 = generate_column_alias(0, None)
        alias1 = generate_column_alias(1, None)
        assert alias0 == "_col0"
        assert alias1 == "_col1"
        assert alias0.replace("_", "").isalnum() or "_" in alias0
        assert alias1.replace("_", "").isalnum() or "_" in alias1

    def test_generate_column_alias_uses_existing_when_provided(self) -> None:
        """generate_column_alias uses existing alias when provided."""
        alias = generate_column_alias(0, "user_id")
        assert alias == "user_id"
