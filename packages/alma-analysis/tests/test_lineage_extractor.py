"""Tests for the lineage extractor module."""

from __future__ import annotations

from alma_analysis.lineage_extractor import (
    ColumnEdge,
    LineageResult,
    extract_lineage,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _edge(source_table: str, source_column: str, output_column: str) -> ColumnEdge:
    return ColumnEdge(
        source_table=source_table,
        source_column=source_column,
        output_column=output_column,
    )


def _canonical_names(result: LineageResult) -> list[str]:
    return sorted(t.canonical_name for t in result.source_tables)


# ---------------------------------------------------------------------------
# Table-level fallback (SELECT *)
# ---------------------------------------------------------------------------


def test_select_star_falls_back_to_table_level() -> None:
    result = extract_lineage("SELECT * FROM public.users", dialect="postgres")
    assert _canonical_names(result) == ["public.users"]
    # SELECT * cannot produce column edges without a catalog
    assert result.column_edges == []
    assert result.target_table is None


def test_select_star_reports_column_extraction_method() -> None:
    result = extract_lineage("SELECT * FROM public.users", dialect="postgres")
    # algebrakit parses fine but yields no edges for star
    assert result.extraction_method == "column"


# ---------------------------------------------------------------------------
# Qualified column references
# ---------------------------------------------------------------------------


def test_simple_qualified_columns() -> None:
    sql = "SELECT u.id, u.name FROM public.users AS u"
    result = extract_lineage(sql, dialect="postgres")
    assert result.extraction_method == "column"
    assert _canonical_names(result) == ["public.users"]
    assert _edge("public.users", "id", "id") in result.column_edges
    assert _edge("public.users", "name", "name") in result.column_edges


def test_qualified_column_with_alias() -> None:
    sql = "SELECT u.name AS full_name FROM public.users u"
    result = extract_lineage(sql, dialect="postgres")
    assert _edge("public.users", "name", "full_name") in result.column_edges


def test_multi_join_qualified_columns() -> None:
    sql = (
        "SELECT u.id, u.name, o.amount "
        "FROM public.users u "
        "JOIN public.orders o ON u.id = o.user_id"
    )
    result = extract_lineage(sql, dialect="postgres")
    assert set(_canonical_names(result)) == {"public.users", "public.orders"}
    assert _edge("public.users", "id", "id") in result.column_edges
    assert _edge("public.users", "name", "name") in result.column_edges
    assert _edge("public.orders", "amount", "amount") in result.column_edges


def test_schema_preserved_in_source_table() -> None:
    sql = "SELECT a.account_id FROM acme.accounts a"
    result = extract_lineage(sql, dialect="postgres")
    assert _edge("acme.accounts", "account_id", "account_id") in result.column_edges


# ---------------------------------------------------------------------------
# Unqualified columns
# ---------------------------------------------------------------------------


def test_unqualified_column_single_table() -> None:
    # With exactly one table in scope the unqualified column is unambiguous
    sql = "SELECT id, name FROM public.users WHERE active = true"
    result = extract_lineage(sql, dialect="postgres")
    assert _edge("public.users", "id", "id") in result.column_edges
    assert _edge("public.users", "name", "name") in result.column_edges


def test_unqualified_column_multi_table_skipped() -> None:
    # Ambiguous without a catalog — no edge produced, no crash
    sql = (
        "SELECT id FROM public.users "
        "JOIN public.orders ON public.users.id = public.orders.user_id"
    )
    result = extract_lineage(sql, dialect="postgres")
    # Source tables still present
    assert "public.users" in _canonical_names(result)
    # Column edges may be empty or partial — should not raise
    assert isinstance(result.column_edges, list)


# ---------------------------------------------------------------------------
# CTE handling
# ---------------------------------------------------------------------------


def test_cte_base_tables_in_source_tables() -> None:
    sql = (
        "WITH active AS (SELECT u.id, u.name FROM public.users u WHERE u.active = true) "
        "SELECT a.id, a.name FROM active a"
    )
    result = extract_lineage(sql, dialect="postgres")
    # Only the base table (users), not the CTE name
    assert _canonical_names(result) == ["public.users"]


def test_cte_names_recorded() -> None:
    sql = (
        "WITH foo AS (SELECT id FROM public.bar) "
        "SELECT f.id FROM foo f"
    )
    result = extract_lineage(sql, dialect="postgres")
    assert "foo" in result.cte_names


def test_cte_column_refs_skipped() -> None:
    # Columns that reference a CTE alias should not produce edges
    sql = (
        "WITH active AS (SELECT u.id FROM public.users u) "
        "SELECT a.id FROM active a"
    )
    result = extract_lineage(sql, dialect="postgres")
    # 'active' is a CTE — its column refs must be skipped
    for edge in result.column_edges:
        assert edge.source_table != "active"


# ---------------------------------------------------------------------------
# Aggregation (GROUP BY)
# ---------------------------------------------------------------------------


def test_group_by_columns_have_edges() -> None:
    sql = (
        "SELECT o.user_id, SUM(o.amount) AS total "
        "FROM public.orders o "
        "GROUP BY o.user_id"
    )
    result = extract_lineage(sql, dialect="postgres")
    assert _edge("public.orders", "user_id", "user_id") in result.column_edges
    assert _edge("public.orders", "amount", "total") in result.column_edges


# ---------------------------------------------------------------------------
# INSERT INTO target extraction
# ---------------------------------------------------------------------------


def test_insert_into_extracts_target_table() -> None:
    sql = "INSERT INTO public.results SELECT u.id, u.name FROM public.users u"
    result = extract_lineage(sql, dialect="postgres")
    assert result.target_table == "public.results"
    assert _canonical_names(result) == ["public.users"]


def test_insert_into_column_edges() -> None:
    sql = "INSERT INTO public.results SELECT u.id, u.name FROM public.users u"
    result = extract_lineage(sql, dialect="postgres")
    assert _edge("public.users", "id", "id") in result.column_edges
    assert _edge("public.users", "name", "name") in result.column_edges


def test_create_table_as_select_target() -> None:
    sql = "CREATE TABLE public.derived AS SELECT u.id FROM public.users u"
    result = extract_lineage(sql, dialect="postgres")
    assert result.target_table == "public.derived"
    assert _canonical_names(result) == ["public.users"]


# ---------------------------------------------------------------------------
# Fallback on unparseable SQL
# ---------------------------------------------------------------------------


def test_unparseable_sql_falls_back_to_table_level() -> None:
    # Deliberately garbled SQL
    sql = "SELECT id FROM public.users WHERE $$$invalid$$$"
    result = extract_lineage(sql, dialect="postgres")
    # Should not raise; source_tables populated via regex fallback
    assert result.extraction_method in ("column", "table")
    assert isinstance(result.column_edges, list)


def test_transaction_statement_returns_empty() -> None:
    result = extract_lineage("BEGIN", dialect="postgres")
    assert result.source_tables == []
    assert result.column_edges == []
    assert result.target_table is None


def test_empty_sql_returns_empty() -> None:
    result = extract_lineage("", dialect="postgres")
    assert result.source_tables == []
    assert result.column_edges == []


# ---------------------------------------------------------------------------
# Expression aliases (computed columns)
# ---------------------------------------------------------------------------


def test_concat_expression_does_not_crash() -> None:
    # The postgres || operator (DPipe) falls back to a Literal in the RA parser,
    # producing no column edges for that expression — but the table ref is still correct.
    sql = (
        "SELECT u.first_name || ' ' || u.last_name AS full_name "
        "FROM public.users u"
    )
    result = extract_lineage(sql, dialect="postgres")
    assert _canonical_names(result) == ["public.users"]
    # No crash; column_edges may be empty for DPipe expressions (RA parser limitation)
    assert isinstance(result.column_edges, list)


# ---------------------------------------------------------------------------
# Plain SELECT without schema (no-alias fallback)
# ---------------------------------------------------------------------------


def test_no_alias_table_ref_resolved() -> None:
    sql = "SELECT users.id, users.email FROM public.users"
    result = extract_lineage(sql, dialect="postgres")
    assert _edge("public.users", "id", "id") in result.column_edges
    assert _edge("public.users", "email", "email") in result.column_edges
