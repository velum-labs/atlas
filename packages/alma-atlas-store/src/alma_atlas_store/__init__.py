"""alma-atlas-store — SQLite persistence layer for Alma Atlas.

Provides repository implementations backed by SQLite for all Alma Atlas
domain objects: assets, edges, schemas, queries, consumers, and contracts.

The store is a thin persistence layer with no business logic. All domain
logic lives in alma-analysis and alma-atlas.
"""

__version__ = "0.1.0"
