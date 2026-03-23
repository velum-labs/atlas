"""alma-atlas-store — SQLite persistence layer for Alma Atlas.

Provides repository implementations backed by SQLite for all Alma Atlas
domain objects: assets, edges, schemas, queries, consumers, and contracts.

The store is a thin persistence layer with no business logic. All domain
logic lives in alma-analysis and alma-atlas.
"""

__version__ = "0.1.0"

from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.consumer_repository import ConsumerRepository
from alma_atlas_store.contract_repository import ContractRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import EdgeRepository
from alma_atlas_store.query_repository import QueryRepository
from alma_atlas_store.schema_repository import SchemaRepository

__all__ = [
    "AssetRepository",
    "ConsumerRepository",
    "ContractRepository",
    "Database",
    "EdgeRepository",
    "QueryRepository",
    "SchemaRepository",
]
