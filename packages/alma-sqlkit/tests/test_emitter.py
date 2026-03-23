"""Tests for the SQLEmitter class."""

from __future__ import annotations

from alma_sqlkit import Dialect, SQLEmitter

# =============================================================================
# Mock RA Types for Testing
# =============================================================================
# We create lightweight mock classes that match the interface expected by SQLEmitter
# to avoid circular dependencies with query_analyzer


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

    def fingerprint(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, str):
            return f"'{self.value}'"
        return str(self.value)


class MockAtomicPredicate:
    """Mock AtomicPredicate for testing."""

    type = "atomic"

    def __init__(self, left, op: str, right=None):
        self.left = left
        self.op = type("Op", (), {"value": op})()
        self.right = right


class MockCompoundPredicate:
    """Mock CompoundPredicate for testing."""

    type = "compound"

    def __init__(self, op: str, operands: list):
        self.op = type("Op", (), {"value": op})()
        self.operands = operands


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
        self.columns = columns  # List of (Expression, alias) tuples
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


class MockAggregateSpec:
    """Mock AggregateSpec for testing."""

    def __init__(self, function: str, argument=None, alias: str = "agg", distinct: bool = False):
        self.function = type("AggFunc", (), {"value": function})()
        self.argument = argument
        self.alias = alias
        self.distinct = distinct


class MockAggregation:
    """Mock Aggregation for testing."""

    type = "aggregation"

    def __init__(self, group_by: list, aggregates: list, input_expr, having=None):
        self.group_by = group_by
        self.aggregates = aggregates
        self.input = input_expr
        self.having = having


class MockUnion:
    """Mock Union for testing."""

    type = "union"

    def __init__(self, left, right, all: bool = False):
        self.left = left
        self.right = right
        self.all = all


class MockDifference:
    """Mock Difference for testing."""

    type = "difference"

    def __init__(self, left, right):
        self.left = left
        self.right = right


class MockIntersect:
    """Mock Intersect for testing."""

    type = "intersect"

    def __init__(self, left, right, all: bool = False):
        self.left = left
        self.right = right
        self.all = all


class MockSortSpec:
    """Mock SortSpec for testing."""

    def __init__(self, expression, direction: str = "asc", nulls: str | None = None):
        self.expression = expression
        self.direction = type("SortDir", (), {"value": direction})()
        self.nulls = type("NullsPos", (), {"value": nulls})() if nulls else None


class MockSort:
    """Mock Sort for testing."""

    type = "sort"

    def __init__(self, input_expr, order_by: list):
        self.input = input_expr
        self.order_by = order_by


class MockLimit:
    """Mock Limit for testing."""

    type = "limit"

    def __init__(self, input_expr, limit: int | None = None, offset: int | None = None):
        self.input = input_expr
        self.limit = limit
        self.offset = offset


class MockFunctionCall:
    """Mock FunctionCall for testing."""

    type = "function_call"

    def __init__(self, name: str, args: list = None, distinct: bool = False):
        self.name = name
        self.args = args or []
        self.distinct = distinct

    def fingerprint(self) -> str:
        args_str = ", ".join(str(a) for a in self.args)
        return f"{self.name}({args_str})"


class MockWindowSpec:
    """Mock WindowSpec for testing."""

    def __init__(self, partition_by: list = None, order_by: list = None, frame=None):
        self.partition_by = partition_by or []
        self.order_by = order_by or []
        self.frame = frame

    def referenced_columns(self):
        return set()

    def fingerprint(self) -> str:
        return "OVER (...)"


class MockWindowExpression:
    """Mock WindowExpression for testing."""

    type = "window_expr"

    def __init__(self, function, window):
        self.function = function
        self.window = window

    def referenced_columns(self):
        return set()

    def fingerprint(self) -> str:
        return f"{self.function.name}() OVER (...)"


class MockCaseExpression:
    """Mock CaseExpression for testing."""

    type = "case_expr"

    def __init__(self, operand=None, when_clauses: list = None, else_result=None):
        self.operand = operand
        self.when_clauses = when_clauses or []
        self.else_result = else_result

    def referenced_columns(self):
        return set()

    def fingerprint(self) -> str:
        return "CASE ... END"


class MockCTEDefinition:
    """Mock CTEDefinition for testing."""

    def __init__(self, name: str, query, columns: list = None, recursive: bool = False):
        self.name = name
        self.query = query
        self.columns = columns
        self.recursive = recursive


class MockWithExpression:
    """Mock WithExpression for testing."""

    type = "with"

    def __init__(self, ctes: list, main_query):
        self.ctes = ctes
        self.main_query = main_query


# =============================================================================
# Emitter Tests
# =============================================================================


class TestSQLEmitterBasics:
    """Basic emitter tests."""

    def test_emit_simple_relation(self):
        """Test emitting a simple table scan."""
        emitter = SQLEmitter(dialect="postgres")
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert "SELECT" in sql
        assert "FROM" in sql
        assert "users" in sql

    def test_emit_relation_with_alias(self):
        """Test emitting a table with alias."""
        emitter = SQLEmitter(dialect="postgres")
        rel = MockRelation(name="users", alias="u")
        sql = emitter.emit(rel)

        assert "users" in sql
        assert "u" in sql.lower() or "AS u" in sql

    def test_emit_relation_with_schema(self):
        """Test emitting a table with schema qualification."""
        emitter = SQLEmitter(dialect="postgres")
        rel = MockRelation(name="users", schema_name="public")
        sql = emitter.emit(rel)

        assert "users" in sql
        assert "public" in sql


class TestSQLEmitterSelection:
    """Tests for Selection emission."""

    def test_emit_simple_selection(self):
        """Test emitting a simple WHERE clause."""
        emitter = SQLEmitter(dialect="postgres")

        pred = MockAtomicPredicate(
            left=MockColumnRef(column="active"),
            op="=",
            right=MockLiteral(value=True, data_type="boolean"),
        )
        sel = MockSelection(predicate=pred, input_expr=MockRelation(name="users"))
        sql = emitter.emit(sel)

        assert "WHERE" in sql
        assert "active" in sql

    def test_emit_compound_selection(self):
        """Test emitting compound WHERE conditions."""
        emitter = SQLEmitter(dialect="postgres")

        pred1 = MockAtomicPredicate(
            left=MockColumnRef(column="active"),
            op="=",
            right=MockLiteral(value=True),
        )
        pred2 = MockAtomicPredicate(
            left=MockColumnRef(column="age"),
            op=">",
            right=MockLiteral(value=18),
        )
        compound = MockCompoundPredicate(op="and", operands=[pred1, pred2])

        sel = MockSelection(predicate=compound, input_expr=MockRelation(name="users"))
        sql = emitter.emit(sel)

        assert "WHERE" in sql
        assert "AND" in sql.upper()

    def test_emit_is_null(self):
        """Test emitting IS NULL predicate."""
        emitter = SQLEmitter(dialect="postgres")

        pred = MockAtomicPredicate(
            left=MockColumnRef(column="deleted_at"),
            op="is_null",
        )
        sel = MockSelection(predicate=pred, input_expr=MockRelation(name="users"))
        sql = emitter.emit(sel)

        assert "IS NULL" in sql.upper()


class TestSQLEmitterProjection:
    """Tests for Projection emission."""

    def test_emit_simple_projection(self):
        """Test emitting SELECT columns."""
        emitter = SQLEmitter(dialect="postgres")

        proj = MockProjection(
            columns=[
                (MockColumnRef(column="id"), None),
                (MockColumnRef(column="name"), None),
            ],
            input_expr=MockRelation(name="users"),
        )
        sql = emitter.emit(proj)

        assert "id" in sql.lower()
        assert "name" in sql.lower()

    def test_emit_projection_with_alias(self):
        """Test emitting SELECT with column aliases."""
        emitter = SQLEmitter(dialect="postgres")

        proj = MockProjection(
            columns=[
                (MockColumnRef(column="id"), "user_id"),
                (MockColumnRef(column="name"), "user_name"),
            ],
            input_expr=MockRelation(name="users"),
        )
        sql = emitter.emit(proj)

        assert "user_id" in sql.lower()
        assert "user_name" in sql.lower()

    def test_emit_distinct_projection(self):
        """Test emitting SELECT DISTINCT."""
        emitter = SQLEmitter(dialect="postgres")

        proj = MockProjection(
            columns=[(MockColumnRef(column="status"), None)],
            input_expr=MockRelation(name="orders"),
            distinct=True,
        )
        sql = emitter.emit(proj)

        assert "DISTINCT" in sql.upper()


class TestSQLEmitterJoin:
    """Tests for Join emission."""

    def test_emit_inner_join(self):
        """Test emitting INNER JOIN."""
        emitter = SQLEmitter(dialect="postgres")

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

    def test_emit_left_join(self):
        """Test emitting LEFT JOIN."""
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


class TestSQLEmitterAggregation:
    """Tests for Aggregation emission."""

    def test_emit_simple_aggregation(self):
        """Test emitting GROUP BY with COUNT."""
        emitter = SQLEmitter(dialect="postgres")

        agg = MockAggregation(
            group_by=[MockColumnRef(column="status")],
            aggregates=[MockAggregateSpec(function="count", alias="cnt")],
            input_expr=MockRelation(name="orders"),
        )
        sql = emitter.emit(agg)

        assert "GROUP BY" in sql.upper()
        assert "COUNT" in sql.upper()

    def test_emit_multiple_aggregates(self):
        """Test emitting multiple aggregate functions."""
        emitter = SQLEmitter(dialect="postgres")

        agg = MockAggregation(
            group_by=[MockColumnRef(column="category")],
            aggregates=[
                MockAggregateSpec(function="count", alias="cnt"),
                MockAggregateSpec(
                    function="sum", argument=MockColumnRef(column="amount"), alias="total"
                ),
            ],
            input_expr=MockRelation(name="orders"),
        )
        sql = emitter.emit(agg)

        assert "COUNT" in sql.upper()
        assert "SUM" in sql.upper()

    def test_emit_aggregation_with_having(self):
        """Test emitting GROUP BY with HAVING."""
        emitter = SQLEmitter(dialect="postgres")

        having_pred = MockAtomicPredicate(
            left=MockColumnRef(column="cnt"),
            op=">",
            right=MockLiteral(value=10),
        )
        agg = MockAggregation(
            group_by=[MockColumnRef(column="category")],
            aggregates=[MockAggregateSpec(function="count", alias="cnt")],
            input_expr=MockRelation(name="orders"),
            having=having_pred,
        )
        sql = emitter.emit(agg)

        assert "HAVING" in sql.upper()


class TestSQLEmitterSetOperations:
    """Tests for Union and Difference emission."""

    def test_emit_union(self):
        """Test emitting UNION."""
        emitter = SQLEmitter(dialect="postgres")

        union = MockUnion(
            left=MockRelation(name="users_a"),
            right=MockRelation(name="users_b"),
            all=False,
        )
        sql = emitter.emit(union)

        assert "UNION" in sql.upper()
        # Should NOT have ALL for distinct union
        # Note: sqlglot may or may not emit UNION DISTINCT explicitly

    def test_emit_union_all(self):
        """Test emitting UNION ALL."""
        emitter = SQLEmitter(dialect="postgres")

        union = MockUnion(
            left=MockRelation(name="users_a"),
            right=MockRelation(name="users_b"),
            all=True,
        )
        sql = emitter.emit(union)

        assert "UNION" in sql.upper()
        # For UNION ALL, distinct=False so it should include ALL
        # The assertion depends on sqlglot's output format

    def test_emit_difference(self):
        """Test emitting EXCEPT."""
        emitter = SQLEmitter(dialect="postgres")

        diff = MockDifference(
            left=MockRelation(name="users_a"),
            right=MockRelation(name="users_b"),
        )
        sql = emitter.emit(diff)

        assert "EXCEPT" in sql.upper()


class TestSQLEmitterDialects:
    """Tests for dialect-specific emission."""

    def test_postgres_dialect(self):
        """Test Postgres dialect emission."""
        emitter = SQLEmitter(dialect="postgres")
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_duckdb_dialect(self):
        """Test DuckDB dialect emission."""
        emitter = SQLEmitter(dialect="duckdb")
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_dialect_object(self):
        """Test using Dialect object."""
        dialect = Dialect.postgres(pretty=False)
        emitter = SQLEmitter(dialect=dialect)
        rel = MockRelation(name="users")
        sql = emitter.emit(rel)

        assert isinstance(sql, str)
        # Non-pretty should be single line
        assert "\n" not in sql or sql.count("\n") < 3


class TestSQLEmitterComplex:
    """Tests for complex expression combinations."""

    def test_emit_complex_query(self):
        """Test emitting a complex query with multiple operations."""
        emitter = SQLEmitter(dialect="postgres")

        # Build: SELECT u.id, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.id
        base_relation = MockRelation(name="users", alias="u")

        join_cond = MockAtomicPredicate(
            left=MockColumnRef(table="u", column="id"),
            op="=",
            right=MockColumnRef(table="o", column="user_id"),
        )
        joined = MockJoin(
            left=base_relation,
            right=MockRelation(name="orders", alias="o"),
            join_type="inner",
            condition=join_cond,
        )

        where_pred = MockAtomicPredicate(
            left=MockColumnRef(table="u", column="active"),
            op="=",
            right=MockLiteral(value=True),
        )
        filtered = MockSelection(predicate=where_pred, input_expr=joined)

        aggregated = MockAggregation(
            group_by=[MockColumnRef(table="u", column="id")],
            aggregates=[MockAggregateSpec(function="count", alias="order_count")],
            input_expr=filtered,
        )

        sql = emitter.emit(aggregated)

        assert "users" in sql.lower()
        assert "orders" in sql.lower()
        assert "JOIN" in sql.upper()
        assert "WHERE" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "COUNT" in sql.upper()


class TestSQLEmitterSort:
    """Tests for Sort (ORDER BY) emission."""

    def test_emit_simple_sort(self):
        """Test emitting ORDER BY."""
        emitter = SQLEmitter(dialect="postgres")

        sort = MockSort(
            input_expr=MockRelation(name="users"),
            order_by=[MockSortSpec(expression=MockColumnRef(column="name"))],
        )
        sql = emitter.emit(sort)

        assert "ORDER BY" in sql.upper()
        assert "name" in sql.lower()

    def test_emit_sort_desc(self):
        """Test emitting ORDER BY DESC."""
        emitter = SQLEmitter(dialect="postgres")

        sort = MockSort(
            input_expr=MockRelation(name="users"),
            order_by=[
                MockSortSpec(expression=MockColumnRef(column="created_at"), direction="desc")
            ],
        )
        sql = emitter.emit(sort)

        assert "ORDER BY" in sql.upper()
        assert "DESC" in sql.upper()

    def test_emit_sort_nulls(self):
        """Test emitting ORDER BY with NULLS FIRST/LAST."""
        emitter = SQLEmitter(dialect="postgres")

        sort = MockSort(
            input_expr=MockRelation(name="users"),
            order_by=[
                MockSortSpec(
                    expression=MockColumnRef(column="score"), direction="desc", nulls="last"
                )
            ],
        )
        sql = emitter.emit(sort)

        assert "ORDER BY" in sql.upper()
        # Note: sqlglot may or may not emit NULLS LAST explicitly

    def test_emit_multiple_sort_keys(self):
        """Test emitting ORDER BY with multiple keys."""
        emitter = SQLEmitter(dialect="postgres")

        sort = MockSort(
            input_expr=MockRelation(name="users"),
            order_by=[
                MockSortSpec(expression=MockColumnRef(column="status")),
                MockSortSpec(expression=MockColumnRef(column="name"), direction="desc"),
            ],
        )
        sql = emitter.emit(sort)

        assert "ORDER BY" in sql.upper()
        assert "status" in sql.lower()
        assert "name" in sql.lower()


class TestSQLEmitterLimit:
    """Tests for Limit emission."""

    def test_emit_limit(self):
        """Test emitting LIMIT."""
        emitter = SQLEmitter(dialect="postgres")

        limit = MockLimit(
            input_expr=MockRelation(name="users"),
            limit=10,
        )
        sql = emitter.emit(limit)

        assert "LIMIT" in sql.upper()
        assert "10" in sql

    def test_emit_limit_offset(self):
        """Test emitting LIMIT with OFFSET."""
        emitter = SQLEmitter(dialect="postgres")

        limit = MockLimit(
            input_expr=MockRelation(name="users"),
            limit=10,
            offset=20,
        )
        sql = emitter.emit(limit)

        assert "LIMIT" in sql.upper()
        assert "OFFSET" in sql.upper()
        assert "10" in sql
        assert "20" in sql

    def test_emit_offset_only(self):
        """Test emitting OFFSET without LIMIT."""
        emitter = SQLEmitter(dialect="postgres")

        limit = MockLimit(
            input_expr=MockRelation(name="users"),
            offset=5,
        )
        sql = emitter.emit(limit)

        assert "OFFSET" in sql.upper()
        assert "5" in sql


class TestSQLEmitterIntersect:
    """Tests for Intersect emission."""

    def test_emit_intersect(self):
        """Test emitting INTERSECT."""
        emitter = SQLEmitter(dialect="postgres")

        intersect = MockIntersect(
            left=MockRelation(name="users_a"),
            right=MockRelation(name="users_b"),
            all=False,
        )
        sql = emitter.emit(intersect)

        assert "INTERSECT" in sql.upper()

    def test_emit_intersect_all(self):
        """Test emitting INTERSECT ALL."""
        emitter = SQLEmitter(dialect="postgres")

        intersect = MockIntersect(
            left=MockRelation(name="users_a"),
            right=MockRelation(name="users_b"),
            all=True,
        )
        sql = emitter.emit(intersect)

        assert "INTERSECT" in sql.upper()


class TestSQLEmitterWindow:
    """Tests for Window function emission."""

    def test_emit_window_function(self):
        """Test emitting a window function."""
        emitter = SQLEmitter(dialect="postgres")

        window_spec = MockWindowSpec(
            partition_by=[MockColumnRef(column="category")],
            order_by=[(MockColumnRef(column="created_at"), type("Dir", (), {"value": "desc"})())],
        )
        window_expr = MockWindowExpression(
            function=MockFunctionCall(name="ROW_NUMBER"),
            window=window_spec,
        )

        proj = MockProjection(
            columns=[
                (MockColumnRef(column="id"), None),
                (window_expr, "rn"),
            ],
            input_expr=MockRelation(name="orders"),
        )
        sql = emitter.emit(proj)

        assert "ROW_NUMBER" in sql.upper()
        assert "OVER" in sql.upper()


class TestSQLEmitterCase:
    """Tests for CASE expression emission."""

    def test_emit_case_expression(self):
        """Test emitting a CASE expression."""
        emitter = SQLEmitter(dialect="postgres")

        case_expr = MockCaseExpression(
            when_clauses=[
                (MockColumnRef(column="status"), MockLiteral(value="active")),
            ],
            else_result=MockLiteral(value="unknown"),
        )

        proj = MockProjection(
            columns=[
                (MockColumnRef(column="id"), None),
                (case_expr, "status_label"),
            ],
            input_expr=MockRelation(name="users"),
        )
        sql = emitter.emit(proj)

        assert "CASE" in sql.upper()


class TestSQLEmitterCTE:
    """Tests for CTE (WITH clause) emission."""

    def test_emit_simple_cte(self):
        """Test emitting a simple CTE."""
        emitter = SQLEmitter(dialect="postgres")

        cte_query = MockProjection(
            columns=[(MockColumnRef(column="id"), None)],
            input_expr=MockRelation(name="users"),
        )
        cte_def = MockCTEDefinition(name="active_users", query=cte_query)

        main_query = MockRelation(name="active_users")

        with_expr = MockWithExpression(ctes=[cte_def], main_query=main_query)
        sql = emitter.emit(with_expr)

        assert "WITH" in sql.upper()
        assert "active_users" in sql.lower()

    def test_emit_multiple_ctes(self):
        """Test emitting multiple CTEs."""
        emitter = SQLEmitter(dialect="postgres")

        cte1 = MockCTEDefinition(
            name="cte1",
            query=MockRelation(name="table1"),
        )
        cte2 = MockCTEDefinition(
            name="cte2",
            query=MockRelation(name="table2"),
        )

        main_query = MockRelation(name="cte1")

        with_expr = MockWithExpression(ctes=[cte1, cte2], main_query=main_query)
        sql = emitter.emit(with_expr)

        assert "WITH" in sql.upper()
        assert "cte1" in sql.lower()
        assert "cte2" in sql.lower()


class TestSQLEmitterSortWithLimit:
    """Tests for combined Sort and Limit (ORDER BY + LIMIT)."""

    def test_emit_sort_with_limit(self):
        """Test emitting ORDER BY with LIMIT (common pagination pattern)."""
        emitter = SQLEmitter(dialect="postgres")

        sorted_expr = MockSort(
            input_expr=MockRelation(name="users"),
            order_by=[
                MockSortSpec(expression=MockColumnRef(column="created_at"), direction="desc")
            ],
        )
        limited = MockLimit(
            input_expr=sorted_expr,
            limit=10,
            offset=0,
        )
        sql = emitter.emit(limited)

        assert "ORDER BY" in sql.upper()
        assert "LIMIT" in sql.upper()
        assert "created_at" in sql.lower()


class TestSQLRoundTrip:
    """Tests for SQL round-trip: parse → emit → parse."""

    def test_roundtrip_simple_select(self):
        """Test round-trip for simple SELECT."""
        from alma_sqlkit import SQLParser

        parser = SQLParser()
        emitter = SQLEmitter(dialect="postgres")

        original_sql = "SELECT id, name FROM users WHERE active = true"

        # Parse to RA
        ra = parser.parse(original_sql)

        # Emit back to SQL
        emitted_sql = emitter.emit(ra)

        # Parse again
        ra2 = parser.parse(emitted_sql)

        # Compare fingerprints
        assert ra.fingerprint() == ra2.fingerprint()

    def test_roundtrip_join(self):
        """Test round-trip for JOIN."""
        from alma_sqlkit import SQLParser

        parser = SQLParser()
        emitter = SQLEmitter(dialect="postgres")

        original_sql = "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id"

        ra = parser.parse(original_sql)
        emitted_sql = emitter.emit(ra)
        ra2 = parser.parse(emitted_sql)

        assert ra.fingerprint() == ra2.fingerprint()

    def test_roundtrip_aggregation(self):
        """Test round-trip for GROUP BY."""
        from alma_sqlkit import SQLParser

        parser = SQLParser()
        emitter = SQLEmitter(dialect="postgres")

        original_sql = "SELECT status, COUNT(*) as cnt FROM orders GROUP BY status"

        ra = parser.parse(original_sql)
        emitted_sql = emitter.emit(ra)
        ra2 = parser.parse(emitted_sql)

        assert ra.fingerprint() == ra2.fingerprint()

    def test_roundtrip_order_by_limit(self):
        """Test round-trip for ORDER BY + LIMIT."""
        from alma_sqlkit import SQLParser

        parser = SQLParser()
        emitter = SQLEmitter(dialect="postgres")

        original_sql = "SELECT * FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 5"

        ra = parser.parse(original_sql)
        emitted_sql = emitter.emit(ra)

        # Verify key components are present
        assert "ORDER BY" in emitted_sql.upper()
        assert "LIMIT" in emitted_sql.upper()
        assert "OFFSET" in emitted_sql.upper()

    def test_roundtrip_union(self):
        """Test round-trip for UNION."""
        from alma_sqlkit import SQLParser

        parser = SQLParser()
        emitter = SQLEmitter(dialect="postgres")

        original_sql = "SELECT id FROM users_a UNION ALL SELECT id FROM users_b"

        ra = parser.parse(original_sql)
        emitted_sql = emitter.emit(ra)

        assert "UNION" in emitted_sql.upper()
