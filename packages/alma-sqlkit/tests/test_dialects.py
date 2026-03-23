"""Multi-dialect tests for SQLEmitter and SQLBuilder.

These tests verify that SQL generation works correctly across
different SQL dialects (PostgreSQL, DuckDB, etc.).
"""

from __future__ import annotations

import pytest

from alma_sqlkit import BIGQUERY, DUCKDB, POSTGRES, Dialect, SQLBuilder, SQLEmitter

# =============================================================================
# Mock RA Types for Testing
# =============================================================================


class MockColumnRef:
    """Mock ColumnRef for testing."""

    type = "column_ref"

    def __init__(self, column: str, table: str | None = None):
        self.column = column
        self.table = table

    def fingerprint(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column


class MockLiteral:
    """Mock Literal for testing."""

    type = "literal"

    def __init__(self, value, data_type: str | None = None):
        self.value = value
        self.data_type = data_type


class MockAtomicPredicate:
    """Mock AtomicPredicate for testing."""

    type = "atomic"

    def __init__(self, left, op: str, right=None):
        self.left = left
        self.op = type("Op", (), {"value": op})()
        self.right = right


class MockRelation:
    """Mock Relation for testing."""

    type = "relation"

    def __init__(self, name: str, schema_name: str | None = None, alias: str | None = None):
        self.name = name
        self.schema_name = schema_name
        self.alias = alias


class MockSelection:
    """Mock Selection for testing."""

    type = "selection"

    def __init__(self, predicate, input_expr):
        self.predicate = predicate
        self.input = input_expr


class MockProjection:
    """Mock Projection for testing."""

    type = "projection"

    def __init__(self, columns: list, input_expr, distinct: bool = False):
        self.columns = columns
        self.input = input_expr
        self.distinct = distinct


class MockJoin:
    """Mock Join for testing."""

    type = "join"

    def __init__(self, left, right, join_type: str = "inner", condition=None):
        self.left = left
        self.right = right
        self.join_type = type("JoinType", (), {"value": join_type})()
        self.condition = condition


# =============================================================================
# SQLEmitter Dialect Tests
# =============================================================================


class TestSQLEmitterPostgres:
    """Test SQLEmitter with PostgreSQL dialect."""

    def test_basic_select(self):
        """Test basic SELECT with Postgres dialect."""
        emitter = SQLEmitter(dialect="postgres")
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert isinstance(sql, str)
        assert "users" in sql.lower()

    def test_postgres_string_literal(self):
        """Test string literal handling in Postgres."""
        emitter = SQLEmitter(dialect="postgres")

        pred = MockAtomicPredicate(
            left=MockColumnRef(column="status"),
            op="=",
            right=MockLiteral(value="active", data_type="string"),
        )
        sel = MockSelection(predicate=pred, input_expr=MockRelation(name="orders"))
        sql = emitter.emit(sel)

        assert "active" in sql.lower()
        assert "'" in sql  # String should be quoted

    def test_postgres_boolean_literal(self):
        """Test boolean literal handling in Postgres."""
        emitter = SQLEmitter(dialect="postgres")

        pred = MockAtomicPredicate(
            left=MockColumnRef(column="active"),
            op="=",
            right=MockLiteral(value=True, data_type="boolean"),
        )
        sel = MockSelection(predicate=pred, input_expr=MockRelation(name="users"))
        sql = emitter.emit(sel)

        # Postgres uses TRUE/FALSE or true/false
        assert "true" in sql.lower() or "TRUE" in sql

    def test_postgres_left_join(self):
        """Test LEFT JOIN in Postgres."""
        emitter = SQLEmitter(dialect="postgres")

        join_cond = MockAtomicPredicate(
            left=MockColumnRef(table="u", column="id"),
            op="=",
            right=MockColumnRef(table="o", column="user_id"),
        )
        join = MockJoin(
            left=MockRelation(name="users", alias="u"),
            right=MockRelation(name="orders", alias="o"),
            join_type="left",
            condition=join_cond,
        )
        sql = emitter.emit(join)

        assert "LEFT" in sql.upper()
        assert "JOIN" in sql.upper()


class TestSQLEmitterDuckDB:
    """Test SQLEmitter with DuckDB dialect."""

    def test_basic_select(self):
        """Test basic SELECT with DuckDB dialect."""
        emitter = SQLEmitter(dialect="duckdb")
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert isinstance(sql, str)
        assert "users" in sql.lower()

    def test_duckdb_string_literal(self):
        """Test string literal handling in DuckDB."""
        emitter = SQLEmitter(dialect="duckdb")

        pred = MockAtomicPredicate(
            left=MockColumnRef(column="status"),
            op="=",
            right=MockLiteral(value="active", data_type="string"),
        )
        sel = MockSelection(predicate=pred, input_expr=MockRelation(name="orders"))
        sql = emitter.emit(sel)

        assert "active" in sql.lower()

    def test_duckdb_join(self):
        """Test JOIN in DuckDB."""
        emitter = SQLEmitter(dialect="duckdb")

        join_cond = MockAtomicPredicate(
            left=MockColumnRef(table="u", column="id"),
            op="=",
            right=MockColumnRef(table="o", column="user_id"),
        )
        join = MockJoin(
            left=MockRelation(name="users", alias="u"),
            right=MockRelation(name="orders", alias="o"),
            join_type="inner",
            condition=join_cond,
        )
        sql = emitter.emit(join)

        assert "JOIN" in sql.upper()
        assert "users" in sql.lower()
        assert "orders" in sql.lower()


class TestSQLEmitterDialectConsistency:
    """Test that SQL emitted is valid across dialects."""

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "bigquery"])
    def test_select_columns_across_dialects(self, dialect: str):
        """Test column selection works across dialects."""
        emitter = SQLEmitter(dialect=dialect)

        proj = MockProjection(
            columns=[
                (MockColumnRef(column="id"), None),
                (MockColumnRef(column="name"), "user_name"),
            ],
            input_expr=MockRelation(name="users"),
        )
        sql = emitter.emit(proj)

        assert "id" in sql.lower()
        assert "user_name" in sql.lower()

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "bigquery"])
    def test_where_clause_across_dialects(self, dialect: str):
        """Test WHERE clause works across dialects."""
        emitter = SQLEmitter(dialect=dialect)

        pred = MockAtomicPredicate(
            left=MockColumnRef(column="amount"),
            op=">",
            right=MockLiteral(value=100),
        )
        sel = MockSelection(predicate=pred, input_expr=MockRelation(name="orders"))
        sql = emitter.emit(sel)

        assert "WHERE" in sql.upper()
        assert "amount" in sql.lower()
        assert "100" in sql

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "bigquery"])
    def test_join_across_dialects(self, dialect: str):
        """Test JOIN works across dialects."""
        emitter = SQLEmitter(dialect=dialect)

        join_cond = MockAtomicPredicate(
            left=MockColumnRef(table="a", column="id"),
            op="=",
            right=MockColumnRef(table="b", column="a_id"),
        )
        join = MockJoin(
            left=MockRelation(name="table_a", alias="a"),
            right=MockRelation(name="table_b", alias="b"),
            join_type="inner",
            condition=join_cond,
        )
        sql = emitter.emit(join)

        assert "JOIN" in sql.upper()


# =============================================================================
# SQLBuilder Dialect Tests
# =============================================================================


class TestSQLBuilderPostgres:
    """Test SQLBuilder with PostgreSQL dialect."""

    def test_basic_query(self):
        """Test basic query building with Postgres."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("id", "name")
            .from_table("users")
            .where("active = true")
            .build()
        )

        assert "SELECT" in sql.upper()
        assert "users" in sql.lower()
        assert "WHERE" in sql.upper()

    def test_complex_query(self):
        """Test complex query with Postgres."""
        sql = (
            SQLBuilder(dialect="postgres")
            .select("u.id", "COUNT(o.id) as order_count")
            .from_table("users", alias="u")
            .left_join("orders", on="u.id = orders.user_id", alias="o")
            .where("u.active = true")
            .group_by("u.id")
            .having("COUNT(o.id) > 0")
            .order_by_desc("order_count")
            .limit(100)
            .build()
        )

        assert "LEFT" in sql.upper()
        assert "JOIN" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "HAVING" in sql.upper()
        assert "ORDER BY" in sql.upper()
        assert "LIMIT" in sql.upper()


class TestSQLBuilderDuckDB:
    """Test SQLBuilder with DuckDB dialect."""

    def test_basic_query(self):
        """Test basic query building with DuckDB."""
        sql = (
            SQLBuilder(dialect="duckdb")
            .select("id", "name")
            .from_table("users")
            .where("active = true")
            .build()
        )

        assert "SELECT" in sql.upper()
        assert "users" in sql.lower()
        assert "WHERE" in sql.upper()

    def test_aggregation_query(self):
        """Test aggregation query with DuckDB."""
        sql = (
            SQLBuilder(dialect="duckdb")
            .select("category", "SUM(amount) as total")
            .from_table("orders")
            .group_by("category")
            .order_by_desc("total")
            .build()
        )

        assert "SUM" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "ORDER BY" in sql.upper()


class TestSQLBuilderDialectConsistency:
    """Test SQLBuilder consistency across dialects."""

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "bigquery"])
    def test_simple_query_across_dialects(self, dialect: str):
        """Test simple query works across dialects."""
        sql = SQLBuilder(dialect=dialect).select("*").from_table("users").build()

        assert isinstance(sql, str)
        assert "SELECT" in sql.upper()
        assert "FROM" in sql.upper()
        assert "users" in sql.lower()

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "bigquery"])
    def test_join_query_across_dialects(self, dialect: str):
        """Test JOIN query works across dialects."""
        sql = (
            SQLBuilder(dialect=dialect)
            .select("u.id", "o.amount")
            .from_table("users", alias="u")
            .join("orders", on="u.id = orders.user_id", alias="o")
            .build()
        )

        assert "JOIN" in sql.upper()
        assert "users" in sql.lower()
        assert "orders" in sql.lower()


# =============================================================================
# Dialect Configuration Tests
# =============================================================================


class TestDialectConfiguration:
    """Test Dialect configuration options."""

    def test_dialect_from_name(self):
        """Test creating dialect from name."""
        postgres = Dialect.from_name("postgres")
        assert postgres.name == "postgres"

        duckdb = Dialect.from_name("duckdb")
        assert duckdb.name == "duckdb"

    def test_dialect_pretty_option(self):
        """Test pretty printing option."""
        pretty_dialect = Dialect.postgres(pretty=True)
        not_pretty_dialect = Dialect.postgres(pretty=False)

        emitter_pretty = SQLEmitter(dialect=pretty_dialect)
        emitter_compact = SQLEmitter(dialect=not_pretty_dialect)

        rel = MockRelation(name="users")

        sql_pretty = emitter_pretty.emit(rel)
        sql_compact = emitter_compact.emit(rel)

        # Pretty should have more newlines typically
        # Both should be valid SQL
        assert "users" in sql_pretty.lower()
        assert "users" in sql_compact.lower()

    def test_predefined_dialects(self):
        """Test predefined dialect constants."""
        assert POSTGRES.name == "postgres"
        assert DUCKDB.name == "duckdb"

    def test_custom_dialect(self):
        """Test custom dialect configuration."""
        custom = Dialect(
            name="postgres",
            identifier_quote='"',
            string_quote="'",
            pretty=False,
        )

        emitter = SQLEmitter(dialect=custom)
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert isinstance(sql, str)
        assert "users" in sql.lower()

    def test_bigquery_dialect_from_name(self):
        """Test creating BigQuery dialect from name."""
        bq = Dialect.from_name("bigquery")
        assert bq.name == "bigquery"
        assert bq.identifier_quote == "`"

    def test_bigquery_predefined_constant(self):
        """Test predefined BIGQUERY constant."""
        assert BIGQUERY.name == "bigquery"
        assert BIGQUERY.identifier_quote == "`"


# =============================================================================
# SQLEmitter BigQuery Tests
# =============================================================================


class TestSQLEmitterBigQuery:
    """Test SQLEmitter with BigQuery dialect."""

    def test_basic_select(self):
        """Test basic SELECT with BigQuery dialect."""
        emitter = SQLEmitter(dialect="bigquery")
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert isinstance(sql, str)
        assert "users" in sql.lower()

    def test_bigquery_string_literal(self):
        """Test string literal handling in BigQuery."""
        emitter = SQLEmitter(dialect="bigquery")

        pred = MockAtomicPredicate(
            left=MockColumnRef(column="status"),
            op="=",
            right=MockLiteral(value="active", data_type="string"),
        )
        sel = MockSelection(predicate=pred, input_expr=MockRelation(name="orders"))
        sql = emitter.emit(sel)

        assert "active" in sql.lower()
        assert "'" in sql  # String should be quoted

    def test_bigquery_join(self):
        """Test JOIN in BigQuery."""
        emitter = SQLEmitter(dialect="bigquery")

        join_cond = MockAtomicPredicate(
            left=MockColumnRef(table="u", column="id"),
            op="=",
            right=MockColumnRef(table="o", column="user_id"),
        )
        join = MockJoin(
            left=MockRelation(name="users", alias="u"),
            right=MockRelation(name="orders", alias="o"),
            join_type="inner",
            condition=join_cond,
        )
        sql = emitter.emit(join)

        assert "JOIN" in sql.upper()
        assert "users" in sql.lower()
        assert "orders" in sql.lower()

    def test_bigquery_left_join(self):
        """Test LEFT JOIN in BigQuery."""
        emitter = SQLEmitter(dialect="bigquery")

        join_cond = MockAtomicPredicate(
            left=MockColumnRef(table="u", column="id"),
            op="=",
            right=MockColumnRef(table="o", column="user_id"),
        )
        join = MockJoin(
            left=MockRelation(name="users", alias="u"),
            right=MockRelation(name="orders", alias="o"),
            join_type="left",
            condition=join_cond,
        )
        sql = emitter.emit(join)

        assert "LEFT" in sql.upper()
        assert "JOIN" in sql.upper()


# =============================================================================
# SQLBuilder BigQuery Tests
# =============================================================================


class TestSQLBuilderBigQuery:
    """Test SQLBuilder with BigQuery dialect."""

    def test_basic_query(self):
        """Test basic query building with BigQuery."""
        sql = (
            SQLBuilder(dialect="bigquery")
            .select("id", "name")
            .from_table("users")
            .where("active = true")
            .build()
        )

        assert "SELECT" in sql.upper()
        assert "users" in sql.lower()
        assert "WHERE" in sql.upper()

    def test_aggregation_query(self):
        """Test aggregation query with BigQuery."""
        sql = (
            SQLBuilder(dialect="bigquery")
            .select("category", "SUM(amount) as total")
            .from_table("orders")
            .group_by("category")
            .order_by_desc("total")
            .build()
        )

        assert "SUM" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "ORDER BY" in sql.upper()

    def test_complex_query(self):
        """Test complex query with BigQuery."""
        sql = (
            SQLBuilder(dialect="bigquery")
            .select("u.id", "COUNT(o.id) as order_count")
            .from_table("users", alias="u")
            .left_join("orders", on="u.id = orders.user_id", alias="o")
            .where("u.active = true")
            .group_by("u.id")
            .having("COUNT(o.id) > 0")
            .order_by_desc("order_count")
            .limit(100)
            .build()
        )

        assert "LEFT" in sql.upper()
        assert "JOIN" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "HAVING" in sql.upper()
        assert "ORDER BY" in sql.upper()
        assert "LIMIT" in sql.upper()
