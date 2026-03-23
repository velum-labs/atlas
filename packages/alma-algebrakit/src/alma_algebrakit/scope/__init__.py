"""Algebraic scoping (works for any query language)."""

from alma_algebrakit.scope.instance import RelationInstance
from alma_algebrakit.scope.resolution import (
    AmbiguousColumnError,
    UnresolvedColumnError,
    resolve,
    resolve_star,
)
from alma_algebrakit.scope.scope import Scope

__all__ = [
    "RelationInstance",
    "Scope",
    "resolve",
    "resolve_star",
    "AmbiguousColumnError",
    "UnresolvedColumnError",
]
