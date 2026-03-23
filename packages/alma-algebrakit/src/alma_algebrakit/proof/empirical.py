"""Empirical equivalence validation for rewrites.

When static proof is incomplete, use execution-based validation
to verify that rewritten queries produce equivalent results.

Note: This module requires an executor to run queries. The executor
interface is generic and can be implemented for different backends.

Content Hashing Strategy:
- Normalize values (handle NULL, float precision, etc.)
- Sort rows for order-independent comparison (bag semantics)
- Compute SHA256 hash for comparison
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, Field


class ValidationTier(StrEnum):
    """Tier of validation achieved."""

    PROVED = "proved"  # Static proof (containment mapping, SMT)
    VALIDATED = "validated"  # Empirical validation passed
    UNKNOWN = "unknown"  # Could not verify


class QueryExecutor(Protocol):
    """Protocol for executing queries against a data source.

    Implementations can target different backends:
    - PostgreSQL
    - DuckDB (for testing)
    - Snowflake
    - BigQuery
    - etc.
    """

    def execute(self, sql: str) -> QueryResult:
        """Execute a query and return results."""
        ...

    def explain(self, sql: str) -> QueryPlan:
        """Get query execution plan."""
        ...

    def is_available(self) -> bool:
        """Check if executor is available and connected."""
        ...


@dataclass
class QueryResult:
    """Result of executing a query."""

    row_count: int
    columns: list[str]
    rows: list[tuple[Any, ...]] | None = None
    execution_time_ms: float = 0
    error: str | None = None

    def is_success(self) -> bool:
        return self.error is None


@dataclass
class QueryPlan:
    """Query execution plan."""

    plan_text: str
    estimated_cost: float | None = None
    estimated_rows: int | None = None


class ValidationConfig(BaseModel):
    """Configuration for empirical validation."""

    check_row_count: bool = Field(default=True, description="Compare row counts")
    check_hash: bool = Field(default=True, description="Compare content hashes")
    check_null_counts: bool = Field(default=True, description="Compare NULL counts per column")

    use_sampling: bool = Field(default=False, description="Use sampling for large tables")
    sample_fraction: float = Field(default=0.01, ge=0, le=1)
    sample_seed: int = Field(default=42)

    timeout_ms: int = Field(default=30000, description="Query timeout")
    max_rows_to_compare: int = Field(default=1000000)

    row_count_tolerance: float = Field(default=0.0, ge=0, le=1)


class ValidationResult(BaseModel):
    """Result of empirical validation."""

    tier: ValidationTier = Field(description="Validation tier achieved")
    passed: bool = Field(description="Whether validation passed")
    explanation: str = Field(default="")

    row_count_match: bool | None = Field(default=None)
    hash_match: bool | None = Field(default=None)
    null_counts_match: bool | None = Field(default=None)

    original_row_count: int | None = Field(default=None)
    rewritten_row_count: int | None = Field(default=None)
    execution_time_ms: float | None = Field(default=None)

    error: str | None = Field(default=None)


# =============================================================================
# Content Hashing Functions
# =============================================================================


def _normalize_value(v: Any) -> str:
    """Normalize a single value for deterministic comparison.

    Handles:
    - NULL values -> "__NULL__"
    - Float precision -> round to 10 decimal places
    - Decimal -> convert to consistent string format
    - Datetime -> ISO format
    - Other types -> str() conversion
    """
    if v is None:
        return "__NULL__"

    if isinstance(v, float):
        # Handle special float values
        if v != v:  # NaN
            return "__NAN__"
        if v == float("inf"):
            return "__INF__"
        if v == float("-inf"):
            return "__NEG_INF__"
        # Round to avoid floating point precision issues
        return f"{v:.10f}".rstrip("0").rstrip(".")

    if isinstance(v, Decimal):
        return str(v.normalize())

    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"

    if isinstance(v, bytes):
        return v.hex()

    # Handle datetime types
    if hasattr(v, "isoformat"):
        return v.isoformat()

    return str(v)


def _normalize_result_set(rows: Sequence[tuple[Any, ...]]) -> list[tuple[str, ...]]:
    """Normalize result set for deterministic comparison.

    Converts all values to comparable strings and sorts rows
    for order-independent comparison (bag semantics).
    """
    normalized = [tuple(_normalize_value(v) for v in row) for row in rows]
    # Sort for order-independent comparison
    return sorted(normalized)


def _compute_content_hash(rows: Sequence[tuple[Any, ...]]) -> str:
    """Compute deterministic hash of query results.

    The hash is computed from a stable string representation of
    the normalized and sorted result set.

    Args:
        rows: Query result rows

    Returns:
        SHA256 hex digest of the content
    """
    if not rows:
        return hashlib.sha256(b"__EMPTY__").hexdigest()

    normalized = _normalize_result_set(rows)

    # Create stable string representation
    # Use pipe separator between columns, newline between rows
    content_lines = ["|".join(row) for row in normalized]
    content = "\n".join(content_lines)

    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def check_content_equivalence(
    rows_a: Sequence[tuple[Any, ...]], rows_b: Sequence[tuple[Any, ...]]
) -> bool:
    """Check if two result sets have identical content.

    This is the public API for comparing result sets.
    Uses deterministic hashing for comparison.

    Args:
        rows_a: First result set
        rows_b: Second result set

    Returns:
        True if content hashes match
    """
    hash_a = _compute_content_hash(rows_a)
    hash_b = _compute_content_hash(rows_b)
    return hash_a == hash_b


class EmpiricalValidator:
    """Validates query rewrites through execution.

    Generates and executes comparison queries to verify that
    the original and rewritten queries produce equivalent results.

    Usage:
        validator = EmpiricalValidator(executor)
        result = validator.validate(original_sql, rewritten_sql)
        if result.tier == ValidationTier.VALIDATED:
            # Safe to use rewrite
    """

    def __init__(
        self,
        executor: QueryExecutor | None = None,
        config: ValidationConfig | None = None,
    ):
        self.executor = executor
        self.config = config or ValidationConfig()

    def validate(
        self,
        original_sql: str,
        rewritten_sql: str,
        key_columns: list[str] | None = None,
    ) -> ValidationResult:
        """Validate that two queries produce equivalent results."""
        if self.executor is None or not self.executor.is_available():
            return ValidationResult(
                tier=ValidationTier.UNKNOWN,
                passed=False,
                explanation="No executor available for empirical validation",
            )

        try:
            checks_passed = True
            details: dict[str, Any] = {}

            if self.config.check_row_count:
                count_result = self._check_row_counts(original_sql, rewritten_sql)
                details.update(count_result)
                if not count_result.get("row_count_match", False):
                    checks_passed = False

            if self.config.check_hash and checks_passed:
                hash_result = self._check_content_hash(original_sql, rewritten_sql)
                details.update(hash_result)
                if not hash_result.get("hash_match", False):
                    checks_passed = False

            tier = ValidationTier.VALIDATED if checks_passed else ValidationTier.UNKNOWN
            explanation = (
                "All validation checks passed" if checks_passed else "Some validation checks failed"
            )

            return ValidationResult(
                tier=tier,
                passed=checks_passed,
                explanation=explanation,
                row_count_match=details.get("row_count_match"),
                hash_match=details.get("hash_match"),
                null_counts_match=details.get("null_counts_match"),
                original_row_count=details.get("original_row_count"),
                rewritten_row_count=details.get("rewritten_row_count"),
            )

        except Exception as e:
            return ValidationResult(
                tier=ValidationTier.UNKNOWN,
                passed=False,
                explanation=f"Validation failed with error: {e}",
                error=str(e),
            )

    def _check_row_counts(self, original_sql: str, rewritten_sql: str) -> dict[str, Any]:
        """Compare row counts between original and rewritten queries."""
        count_sql_original = f"SELECT COUNT(*) AS cnt FROM ({original_sql}) AS _orig"
        count_sql_rewritten = f"SELECT COUNT(*) AS cnt FROM ({rewritten_sql}) AS _rewrite"

        result_orig = self.executor.execute(count_sql_original)
        result_rewrite = self.executor.execute(count_sql_rewritten)

        if not result_orig.is_success() or not result_rewrite.is_success():
            return {
                "row_count_match": False,
                "error": result_orig.error or result_rewrite.error,
            }

        count_orig = result_orig.rows[0][0] if result_orig.rows else 0
        count_rewrite = result_rewrite.rows[0][0] if result_rewrite.rows else 0

        if self.config.row_count_tolerance > 0:
            diff = abs(count_orig - count_rewrite) / max(count_orig, 1)
            match = diff <= self.config.row_count_tolerance
        else:
            match = count_orig == count_rewrite

        return {
            "row_count_match": match,
            "original_row_count": count_orig,
            "rewritten_row_count": count_rewrite,
        }

    def _check_content_hash(self, original_sql: str, rewritten_sql: str) -> dict[str, Any]:
        """Compare content hashes between queries.

        Uses deterministic normalization and SHA256 hashing to verify
        that both queries produce identical result sets (under bag semantics).

        The comparison is:
        1. Execute both queries
        2. Normalize all values for consistent comparison
        3. Sort rows for order-independent comparison
        4. Compute SHA256 hash of normalized content
        5. Compare hashes
        """
        if self.executor is None:
            return {"hash_match": False, "error": "No executor available"}

        # Execute both queries to get full results
        result_orig = self.executor.execute(original_sql)
        result_rewrite = self.executor.execute(rewritten_sql)

        if not result_orig.is_success():
            return {"hash_match": False, "error": f"Original query failed: {result_orig.error}"}
        if not result_rewrite.is_success():
            return {"hash_match": False, "error": f"Rewritten query failed: {result_rewrite.error}"}

        # Check if we have row data
        if result_orig.rows is None or result_rewrite.rows is None:
            return {"hash_match": False, "error": "No row data available for comparison"}

        # Apply max rows limit
        rows_orig = result_orig.rows[: self.config.max_rows_to_compare]
        rows_rewrite = result_rewrite.rows[: self.config.max_rows_to_compare]

        # Compute content hashes
        hash_orig = _compute_content_hash(rows_orig)
        hash_rewrite = _compute_content_hash(rows_rewrite)

        return {
            "hash_match": hash_orig == hash_rewrite,
            "original_hash": hash_orig[:16],  # Truncate for logging
            "rewritten_hash": hash_rewrite[:16],
        }

    def generate_validation_queries(
        self, original_sql: str, rewritten_sql: str
    ) -> list[tuple[str, str, str]]:
        """Generate SQL queries for manual validation."""
        queries = []

        queries.append(
            (
                "row_count",
                f"SELECT COUNT(*) AS cnt FROM ({original_sql}) AS _orig",
                f"SELECT COUNT(*) AS cnt FROM ({rewritten_sql}) AS _rewrite",
            )
        )

        return queries


class DuckDBExecutor:
    """DuckDB executor for testing and local validation."""

    def __init__(self, db_path: str = ":memory:"):
        self._db_path = db_path
        self._conn = None

    def _get_connection(self):
        if self._conn is None:
            try:
                import duckdb

                self._conn = duckdb.connect(self._db_path)
            except ImportError as err:
                raise RuntimeError("DuckDB not installed. Run: pip install duckdb") from err
        return self._conn

    def execute(self, sql: str) -> QueryResult:
        try:
            import time

            conn = self._get_connection()
            start = time.time()
            result = conn.execute(sql).fetchall()
            elapsed = (time.time() - start) * 1000

            return QueryResult(
                row_count=len(result),
                columns=[],
                rows=result,
                execution_time_ms=elapsed,
            )
        except Exception as e:
            return QueryResult(
                row_count=0,
                columns=[],
                error=str(e),
            )

    def explain(self, sql: str) -> QueryPlan:
        try:
            conn = self._get_connection()
            result = conn.execute(f"EXPLAIN {sql}").fetchall()
            plan_text = "\n".join(row[0] for row in result)
            return QueryPlan(plan_text=plan_text)
        except Exception as e:
            return QueryPlan(plan_text=f"Error: {e}")

    def is_available(self) -> bool:
        import importlib.util

        return importlib.util.find_spec("duckdb") is not None

    def create_table(self, name: str, data: list[dict[str, Any]]) -> None:
        """Helper to create a table from dict data for testing."""
        if not data:
            return

        conn = self._get_connection()
        cols = list(data[0].keys())
        conn.execute(f"DROP TABLE IF EXISTS {name}")
        conn.execute(
            f"CREATE TABLE {name} AS SELECT * FROM (VALUES {self._values_clause(data, cols)})"
        )

    def _values_clause(self, data: list[dict[str, Any]], cols: list[str]) -> str:
        """Generate VALUES clause for INSERT."""
        rows = []
        for row in data:
            vals = []
            for col in cols:
                val = row.get(col)
                if val is None:
                    vals.append("NULL")
                elif isinstance(val, str):
                    vals.append(f"'{val}'")
                else:
                    vals.append(str(val))
            rows.append(f"({', '.join(vals)})")

        return ", ".join(rows) + " AS t(" + ", ".join(cols) + ")"
