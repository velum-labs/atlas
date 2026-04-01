"""Thin re-export of canonical sqlkit table-reference helpers."""

from alma_sqlkit.table_refs import TableRef, extract_table_names, extract_tables_from_sql

__all__ = ["TableRef", "extract_table_names", "extract_tables_from_sql"]
