"""Concrete connector adapters.

Import adapters directly from their owning modules, for example:
- `alma_connectors.adapters.postgres.PostgresAdapter`
- `alma_connectors.adapters.bigquery.BigQueryAdapter`
- `alma_connectors.adapters.sqlite.SQLiteAdapter`
"""

from alma_connectors.adapters.sqlite import SQLiteAdapter

__all__ = ["SQLiteAdapter"]
