"""Repository bundle for session-scoped Atlas store access."""

from __future__ import annotations

from dataclasses import dataclass

from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.consumer_repository import ConsumerRepository
from alma_atlas_store.contract_repository import ContractRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import EdgeRepository
from alma_atlas_store.query_repository import QueryRepository
from alma_atlas_store.schema_repository import SchemaRepository
from alma_atlas_store.violation_repository import ViolationRepository


@dataclass
class AtlasStoreSession:
    """Convenience repository bundle for one database session."""

    db: Database
    assets: AssetRepository
    annotations: AnnotationRepository
    consumers: ConsumerRepository
    contracts: ContractRepository
    edges: EdgeRepository
    queries: QueryRepository
    schemas: SchemaRepository
    violations: ViolationRepository

    @classmethod
    def from_db(cls, db: Database) -> AtlasStoreSession:
        return cls(
            db=db,
            assets=AssetRepository(db),
            annotations=AnnotationRepository(db),
            consumers=ConsumerRepository(db),
            contracts=ContractRepository(db),
            edges=EdgeRepository(db),
            queries=QueryRepository(db),
            schemas=SchemaRepository(db),
            violations=ViolationRepository(db),
        )
