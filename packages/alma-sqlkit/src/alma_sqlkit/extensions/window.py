"""Window function extensions for SQL.

Window functions (with OVER clause) are SQL-specific extensions to
relational algebra that provide row-level computations across partitions.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal as TypeLiteral

from alma_algebrakit import Expression, FunctionCall
from pydantic import BaseModel, Field


class WindowFrameType(StrEnum):
    """Window frame type."""

    ROWS = "rows"
    RANGE = "range"
    GROUPS = "groups"


class WindowFrameBound(StrEnum):
    """Window frame boundary."""

    UNBOUNDED_PRECEDING = "unbounded_preceding"
    CURRENT_ROW = "current_row"
    UNBOUNDED_FOLLOWING = "unbounded_following"
    PRECEDING = "preceding"  # N PRECEDING
    FOLLOWING = "following"  # N FOLLOWING


class WindowFrameSpec(BaseModel):
    """Window frame specification (ROWS/RANGE BETWEEN...)."""

    frame_type: WindowFrameType = Field(default=WindowFrameType.RANGE)
    start_bound: WindowFrameBound = Field(default=WindowFrameBound.UNBOUNDED_PRECEDING)
    start_offset: int | None = Field(default=None, description="Offset for PRECEDING/FOLLOWING")
    end_bound: WindowFrameBound = Field(default=WindowFrameBound.CURRENT_ROW)
    end_offset: int | None = Field(default=None, description="Offset for PRECEDING/FOLLOWING")

    def fingerprint(self) -> str:
        def bound_str(bound: WindowFrameBound, offset: int | None) -> str:
            if bound == WindowFrameBound.UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING"
            if bound == WindowFrameBound.UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING"
            if bound == WindowFrameBound.CURRENT_ROW:
                return "CURRENT ROW"
            if bound == WindowFrameBound.PRECEDING:
                return f"{offset} PRECEDING"
            if bound == WindowFrameBound.FOLLOWING:
                return f"{offset} FOLLOWING"
            return str(bound)

        start = bound_str(self.start_bound, self.start_offset)
        end = bound_str(self.end_bound, self.end_offset)
        return f"{self.frame_type.value.upper()} BETWEEN {start} AND {end}"


class WindowSpec(BaseModel):
    """Window function specification (OVER clause)."""

    partition_by: list[Expression] = Field(
        default_factory=list, description="PARTITION BY expressions"
    )
    order_by: list[tuple[Expression, str]] = Field(
        default_factory=list, description="ORDER BY expressions with direction (ASC/DESC)"
    )
    frame: WindowFrameSpec | None = Field(default=None, description="Window frame specification")

    def referenced_columns(self) -> set[str]:
        result: set[str] = set()
        for expr in self.partition_by:
            result |= expr.referenced_columns()
        for expr, _ in self.order_by:
            result |= expr.referenced_columns()
        return result

    def fingerprint(self) -> str:
        parts = []
        if self.partition_by:
            partition_str = ", ".join(e.fingerprint() for e in self.partition_by)
            parts.append(f"PARTITION BY {partition_str}")
        if self.order_by:
            order_str = ", ".join(f"{e.fingerprint()} {d}" for e, d in self.order_by)
            parts.append(f"ORDER BY {order_str}")
        if self.frame:
            parts.append(self.frame.fingerprint())
        return " ".join(parts)


class WindowFunction(Expression):
    """A window function call (function with OVER clause)."""

    type: TypeLiteral["window_function"] = "window_function"
    function: FunctionCall = Field(description="The function being called")
    window: WindowSpec = Field(description="Window specification (OVER clause)")
    alias: str | None = Field(default=None, description="Output column alias")

    def referenced_columns(self) -> set[str]:
        return self.function.referenced_columns() | self.window.referenced_columns()

    def fingerprint(self) -> str:
        window_str = self.window.fingerprint()
        over_clause = f"OVER ({window_str})" if window_str else "OVER ()"
        return f"{self.function.fingerprint()} {over_clause}"
