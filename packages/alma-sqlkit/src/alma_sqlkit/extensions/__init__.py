"""SQL-specific extensions to alma_algebrakit's relational algebra types.

These extensions add SQL-specific features that are not part of pure
relational algebra but are needed for SQL parsing and emission:
- Window functions (OVER clause)
- CTEs (WITH clause)
- PostgreSQL-specific extensions (DISTINCT ON, LATERAL)
"""

from alma_sqlkit.extensions.cte import (
    CTEDefinition,
    WithClause,
)
from alma_sqlkit.extensions.postgres import (
    ExtendedJoin,
    ExtendedProjection,
)
from alma_sqlkit.extensions.window import (
    WindowFrameBound,
    WindowFrameSpec,
    WindowFrameType,
    WindowFunction,
    WindowSpec,
)

__all__ = [
    # Window functions
    "WindowFrameType",
    "WindowFrameBound",
    "WindowFrameSpec",
    "WindowSpec",
    "WindowFunction",
    # CTEs
    "CTEDefinition",
    "WithClause",
    # PostgreSQL extensions
    "ExtendedProjection",
    "ExtendedJoin",
]
