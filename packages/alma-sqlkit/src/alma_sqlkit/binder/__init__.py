"""SQL binding module - thin adapter over alma_algebrakit's binding primitives."""

from alma_sqlkit.binder.sql_binder import BindingError, SQLBinder

__all__ = ["SQLBinder", "BindingError"]
