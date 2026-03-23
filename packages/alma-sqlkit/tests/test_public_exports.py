"""Public API export guards for alma_sqlkit."""

from __future__ import annotations

import alma_sqlkit

EXPECTED_ALMA_SQLKIT_EXPORTS = (
    "__version__",
    "sqlglot",
    "exp",
    "ParseError",
    "parse_ast",
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
    "Catalog",
    "TableSchema",
    "Scope",
    "RelationInstance",
    "BoundQuery",
    "AttributeRef",
    "BoundExpression",
    "BoundPredicate",
    "WindowFrameType",
    "WindowFrameBound",
    "WindowFrameSpec",
    "WindowSpec",
    "WindowFunction",
    "CTEDefinition",
    "WithClause",
    "ExtendedProjection",
    "ExtendedJoin",
)


def test_alma_sqlkit_exports_snapshot() -> None:
    assert tuple(alma_sqlkit.__all__) == EXPECTED_ALMA_SQLKIT_EXPORTS
