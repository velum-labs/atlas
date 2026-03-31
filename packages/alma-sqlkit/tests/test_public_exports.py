"""Public API export guards for alma_sqlkit."""

from __future__ import annotations

import alma_sqlkit

EXPECTED_ALMA_SQLKIT_EXPORTS = (
    "__version__",
    "SQLParser",
    "ParsingConfig",
    "SQLBinder",
    "BindingError",
    "SQLEmitter",
    "emit_sql",
    "SQLBuilder",
    "build_sql",
    "Dialect",
    "DialectName",
    "DEFAULT_DIALECT",
    "POSTGRES",
    "DUCKDB",
    "SNOWFLAKE",
    "BIGQUERY",
    "parse_sql",
    "extract_tables",
    "normalize_sql",
    "TableRef",
    "extract_table_names",
    "extract_tables_from_sql",
)


def test_alma_sqlkit_exports_snapshot() -> None:
    assert tuple(alma_sqlkit.__all__) == EXPECTED_ALMA_SQLKIT_EXPORTS
