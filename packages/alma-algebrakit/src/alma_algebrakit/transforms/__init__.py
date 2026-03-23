"""Query transformation utilities for algebrakit.

This module provides transformations that rewrite queries into equivalent
forms that are more amenable to optimization or analysis.
"""

from alma_algebrakit.transforms.decorrelate import (
    DecorrelationResult,
    decorrelate_query,
)

__all__ = [
    "DecorrelationResult",
    "decorrelate_query",
]
