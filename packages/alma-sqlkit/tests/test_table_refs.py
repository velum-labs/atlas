from __future__ import annotations

from alma_sqlkit.parse import extract_tables
from alma_sqlkit.table_refs import extract_table_names, extract_tables_from_sql


def test_extract_tables_from_sql_handles_postgres_refs() -> None:
    refs = extract_tables_from_sql(
        'SELECT * FROM "public"."users" u JOIN public.orders o ON u.id = o.user_id',
        dialect="postgres",
    )
    assert [ref.canonical_name for ref in refs] == ["public.orders", "public.users"]


def test_extract_table_names_handles_bigquery_refs() -> None:
    names = extract_table_names(
        "SELECT * FROM `project.analytics.orders` JOIN `project.analytics.users` USING (user_id)",
        dialect="bigquery",
    )
    assert names == ["analytics.orders", "analytics.users"]


def test_parse_extract_tables_delegates_to_table_ref_helper() -> None:
    assert extract_tables("SELECT * FROM public.users", dialect="postgres") == ["public.users"]
