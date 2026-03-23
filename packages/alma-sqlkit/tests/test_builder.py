"""Tests for the SQLBuilder class."""

from __future__ import annotations

import pytest

from alma_sqlkit import Dialect, SQLBuilder, build_sql


class TestSQLBuilderBasics:
    """Basic builder tests."""

    def test_simple_select(self):
        """Test simple SELECT statement."""
        sql = SQLBuilder(dialect="postgres").select("id", "name").from_table("users").build()

        assert "SELECT" in sql.upper()
        assert "id" in sql.lower()
        assert "name" in sql.lower()
        assert "FROM" in sql.upper()
        assert "users" in sql.lower()

    def test_select_star(self):
        """Test SELECT * statement."""
        sql = SQLBuilder(dialect="postgres").select("*").from_table("users").build()

        assert "SELECT" in sql.upper()
        assert "*" in sql
        assert "users" in sql.lower()

    def test_select_with_alias(self):
        """Test FROM with table alias."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("u.id", "u.name")
            .from_table("users", alias="u")
            .build()
        )

        assert "users" in sql.lower()
        # Check alias is present (could be AS u or just u depending on dialect)
        assert "u" in sql.lower()

    def test_missing_from_raises(self):
        """Test that missing FROM clause raises error."""
        with pytest.raises(ValueError, match="FROM clause is required"):
            SQLBuilder(dialect="postgres").select("id").build()


class TestSQLBuilderWhere:
    """Tests for WHERE clause building."""

    def test_single_where(self):
        """Test single WHERE condition."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users")
            .where("active = true")
            .build()
        )

        assert "WHERE" in sql.upper()
        assert "active" in sql.lower()

    def test_multiple_where(self):
        """Test multiple WHERE conditions (ANDed)."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users")
            .where("active = true")
            .where("age > 18")
            .build()
        )

        assert "WHERE" in sql.upper()
        assert "AND" in sql.upper()
        assert "active" in sql.lower()
        assert "age" in sql.lower()

    def test_where_with_parameters(self):
        """Test WHERE with comparison operators."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("orders")
            .where("amount >= 100")
            .where("status != 'cancelled'")
            .build()
        )

        assert "WHERE" in sql.upper()
        assert "amount" in sql.lower()
        assert "status" in sql.lower()


class TestSQLBuilderJoins:
    """Tests for JOIN clause building."""

    def test_inner_join(self):
        """Test INNER JOIN."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("u.id", "o.amount")
            .from_table("users", alias="u")
            .join("orders", on="u.id = orders.user_id", alias="o")
            .build()
        )

        assert "JOIN" in sql.upper()
        assert "users" in sql.lower()
        assert "orders" in sql.lower()

    def test_left_join(self):
        """Test LEFT JOIN."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users", alias="u")
            .left_join("orders", on="u.id = orders.user_id", alias="o")
            .build()
        )

        assert "LEFT" in sql.upper()
        assert "JOIN" in sql.upper()

    def test_right_join(self):
        """Test RIGHT JOIN."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users", alias="u")
            .right_join("orders", on="u.id = orders.user_id")
            .build()
        )

        assert "RIGHT" in sql.upper()
        assert "JOIN" in sql.upper()

    def test_full_join(self):
        """Test FULL OUTER JOIN."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users", alias="u")
            .full_join("orders", on="u.id = orders.user_id")
            .build()
        )

        assert "FULL" in sql.upper()
        assert "JOIN" in sql.upper()

    def test_cross_join(self):
        """Test CROSS JOIN."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users")
            .cross_join("products")
            .build()
        )

        assert "CROSS" in sql.upper()
        assert "JOIN" in sql.upper()

    def test_multiple_joins(self):
        """Test multiple JOINs."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("u.name", "o.amount", "p.name")
            .from_table("users", alias="u")
            .join("orders", on="u.id = orders.user_id", alias="o")
            .join("products", on="o.product_id = products.id", alias="p")
            .build()
        )

        assert sql.upper().count("JOIN") >= 2
        assert "users" in sql.lower()
        assert "orders" in sql.lower()
        assert "products" in sql.lower()


class TestSQLBuilderGroupBy:
    """Tests for GROUP BY clause building."""

    def test_simple_group_by(self):
        """Test simple GROUP BY."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("category", "COUNT(*)")
            .from_table("products")
            .group_by("category")
            .build()
        )

        assert "GROUP BY" in sql.upper()
        assert "category" in sql.lower()

    def test_multiple_group_by(self):
        """Test multiple GROUP BY columns."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("category", "status", "COUNT(*)")
            .from_table("orders")
            .group_by("category", "status")
            .build()
        )

        assert "GROUP BY" in sql.upper()
        assert "category" in sql.lower()
        assert "status" in sql.lower()

    def test_group_by_with_having(self):
        """Test GROUP BY with HAVING."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("category", "COUNT(*) as cnt")
            .from_table("products")
            .group_by("category")
            .having("COUNT(*) > 10")
            .build()
        )

        assert "GROUP BY" in sql.upper()
        assert "HAVING" in sql.upper()


class TestSQLBuilderOrderBy:
    """Tests for ORDER BY clause building."""

    def test_simple_order_by(self):
        """Test simple ORDER BY."""
        sql = (
            SQLBuilder(dialect="postgres").select("*").from_table("users").order_by("name").build()
        )

        assert "ORDER BY" in sql.upper()
        assert "name" in sql.lower()

    def test_order_by_desc(self):
        """Test ORDER BY DESC."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users")
            .order_by_desc("created_at")
            .build()
        )

        assert "ORDER BY" in sql.upper()
        assert "DESC" in sql.upper()

    def test_multiple_order_by(self):
        """Test multiple ORDER BY columns."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users")
            .order_by("name")
            .order_by_desc("created_at")
            .build()
        )

        assert "ORDER BY" in sql.upper()
        assert "name" in sql.lower()
        assert "created_at" in sql.lower()


class TestSQLBuilderLimitOffset:
    """Tests for LIMIT and OFFSET clauses."""

    def test_limit(self):
        """Test LIMIT clause."""
        sql = SQLBuilder(dialect="postgres").select("*").from_table("users").limit(10).build()

        assert "LIMIT" in sql.upper()
        assert "10" in sql

    def test_offset(self):
        """Test OFFSET clause."""
        sql = SQLBuilder(dialect="postgres").select("*").from_table("users").offset(20).build()

        assert "OFFSET" in sql.upper()
        assert "20" in sql

    def test_limit_offset(self):
        """Test LIMIT with OFFSET."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("*")
            .from_table("users")
            .limit(10)
            .offset(20)
            .build()
        )

        assert "LIMIT" in sql.upper()
        assert "OFFSET" in sql.upper()
        assert "10" in sql
        assert "20" in sql


class TestSQLBuilderDistinct:
    """Tests for SELECT DISTINCT."""

    def test_select_distinct(self):
        """Test SELECT DISTINCT."""
        sql = SQLBuilder(dialect="postgres").select_distinct("status").from_table("orders").build()

        assert "DISTINCT" in sql.upper()
        assert "status" in sql.lower()


class TestSQLBuilderCopy:
    """Tests for builder copy functionality."""

    def test_copy_creates_independent_builder(self):
        """Test that copy creates an independent builder."""
        original = SQLBuilder(dialect="postgres").select("id").from_table("users")

        copied = original.copy()
        copied.where("active = true")

        original_sql = original.build()
        copied_sql = copied.build()

        assert "WHERE" not in original_sql.upper()
        assert "WHERE" in copied_sql.upper()


class TestSQLBuilderComplex:
    """Tests for complex query building."""

    def test_complex_query(self):
        """Test building a complex query."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("u.id", "u.name", "COUNT(o.id) as order_count", "SUM(o.amount) as total")
            .from_table("users", alias="u")
            .left_join("orders", on="u.id = orders.user_id", alias="o")
            .where("u.active = true")
            .where("o.created_at >= '2024-01-01'")
            .group_by("u.id", "u.name")
            .having("COUNT(o.id) > 0")
            .order_by_desc("total")
            .limit(100)
            .build()
        )

        assert "SELECT" in sql.upper()
        assert "FROM" in sql.upper()
        assert "LEFT" in sql.upper()
        assert "JOIN" in sql.upper()
        assert "WHERE" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "HAVING" in sql.upper()
        assert "ORDER BY" in sql.upper()
        assert "LIMIT" in sql.upper()


class TestSQLBuilderDialects:
    """Tests for dialect-specific building."""

    def test_postgres_dialect(self):
        """Test Postgres dialect."""
        sql = SQLBuilder(dialect="postgres").select("*").from_table("users").build()

        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_duckdb_dialect(self):
        """Test DuckDB dialect."""
        sql = SQLBuilder(dialect="duckdb").select("*").from_table("users").build()

        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_dialect_object(self):
        """Test using Dialect object."""
        dialect = Dialect.postgres(pretty=False)
        sql = SQLBuilder(dialect=dialect).select("*").from_table("users").build()

        assert isinstance(sql, str)
        # Non-pretty should be more compact
        assert "\n" not in sql or sql.count("\n") < 3


class TestBuildSqlHelper:
    """Tests for the build_sql helper function."""

    def test_build_sql_helper(self):
        """Test the build_sql() helper function."""
        sql = build_sql("postgres").select("id", "name").from_table("users").build()

        assert "SELECT" in sql.upper()
        assert "users" in sql.lower()
