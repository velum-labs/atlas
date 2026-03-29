"""Enrichment orchestrator — identifies unenriched edges and calls the pipeline agent.

After cross-system edge discovery (schema_match / dbt_source_ref edges), this
module enriches those edges with transport metadata inferred from a code
repository by the :mod:`alma_atlas.agents.pipeline_analyzer` agent.

Enrichment is idempotent: edges that already carry ``enrichment_status=enriched``
in their metadata are skipped.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.edge_repository import Edge, EdgeRepository

if TYPE_CHECKING:
    from alma_atlas.agents.provider import LLMProvider
    from alma_atlas_store.db import Database

logger = logging.getLogger(__name__)

# Edge kinds that are candidates for pipeline enrichment.
_ENRICHABLE_KINDS: frozenset[str] = frozenset({"schema_match", "dbt_source_ref"})


def get_unenriched_edges(db: Database) -> list[Edge]:
    """Return edges that have not yet been enriched by the pipeline analysis agent.

    An edge qualifies when its :attr:`~alma_atlas_store.edge_repository.Edge.kind`
    is one of the enrichable kinds (``schema_match`` or ``dbt_source_ref``) and
    its metadata does not contain ``enrichment_status: enriched``.

    Args:
        db: Open :class:`~alma_atlas_store.db.Database` connection.

    Returns:
        List of unenriched :class:`~alma_atlas_store.edge_repository.Edge` objects.
    """
    repo = EdgeRepository(db)
    return [
        e
        for e in repo.list_all()
        if e.kind in _ENRICHABLE_KINDS
        and e.metadata.get("enrichment_status") != "enriched"
    ]


def _object_part(asset_id: str) -> str:
    """Extract the ``schema.table`` portion from an asset ID like ``src::schema.table``."""
    return asset_id.split("::", 1)[-1] if "::" in asset_id else asset_id


async def run_enrichment(
    db: Database,
    repo_path: Path,
    provider: LLMProvider,
) -> int:
    """Enrich unenriched cross-system edges with pipeline transport metadata.

    Collects all unenriched edges, calls the pipeline analysis agent, and
    persists the returned :class:`~alma_atlas.agents.schemas.EdgeEnrichment`
    data back to the store.  Edges for which the agent returns no result are
    left unchanged.  Persistence failures are logged as warnings and do not
    abort the run.

    Args:
        db:        Open :class:`~alma_atlas_store.db.Database` connection.
        repo_path: Filesystem path to the code repository to scan.
        provider:  Configured :class:`~alma_atlas.agents.provider.LLMProvider`.

    Returns:
        Number of edges successfully enriched and persisted.
    """
    from alma_atlas.agents.pipeline_analyzer import analyze_edges

    unenriched = get_unenriched_edges(db)
    if not unenriched:
        logger.info("run_enrichment: no unenriched edges found")
        return 0

    logger.info(
        "run_enrichment: enriching %d edge(s) from %s",
        len(unenriched),
        repo_path,
    )

    enrichments = await analyze_edges(unenriched, repo_path, provider)
    if not enrichments:
        logger.info("run_enrichment: agent returned no enrichment results")
        return 0

    # Index enrichments by (source_table, dest_table) for O(1) lookup.
    enrichment_index: dict[tuple[str, str], object] = {
        (e.source_table, e.dest_table): e for e in enrichments
    }

    repo = EdgeRepository(db)
    enriched_count = 0

    for edge in unenriched:
        src_obj = _object_part(edge.upstream_id)
        dst_obj = _object_part(edge.downstream_id)
        enrichment = enrichment_index.get((src_obj, dst_obj))
        if enrichment is None:
            logger.debug(
                "run_enrichment: no match for %s → %s",
                edge.upstream_id,
                edge.downstream_id,
            )
            continue

        updated_metadata: dict = {
            **edge.metadata,
            "transport_kind": enrichment.transport_kind,
            "schedule": enrichment.schedule,
            "strategy": enrichment.strategy,
            "write_disposition": enrichment.write_disposition,
            "watermark_column": enrichment.watermark_column,
            "owner": enrichment.owner,
            "confidence_note": enrichment.confidence_note,
            "enrichment_status": "enriched",
        }
        try:
            repo.upsert(
                Edge(
                    upstream_id=edge.upstream_id,
                    downstream_id=edge.downstream_id,
                    kind=edge.kind,
                    metadata=updated_metadata,
                )
            )
            enriched_count += 1
            logger.debug(
                "run_enrichment: persisted enrichment for %s → %s",
                edge.upstream_id,
                edge.downstream_id,
            )
        except Exception as exc:
            logger.warning(
                "run_enrichment: failed to persist enrichment for %s → %s: %s",
                edge.upstream_id,
                edge.downstream_id,
                exc,
            )

    logger.info("run_enrichment: %d edge(s) enriched", enriched_count)
    return enriched_count


def get_unannotated_assets(db: Database, *, limit: int = 100) -> list[str]:
    """Return asset IDs that have no annotation record yet."""
    from alma_atlas_store.annotation_repository import AnnotationRepository

    return AnnotationRepository(db).list_unannotated(limit=limit)


async def run_asset_enrichment(
    db: Database,
    repo_path: Path,
    provider: LLMProvider,
    *,
    provider_name: str,
    model: str,
    limit: int = 100,
    batch_size: int = 20,
) -> int:
    """Enrich assets with supplementary business metadata annotations.

    This is the P2 "Codex enrichment" path. It selects assets that have not yet
    been annotated, builds a context payload per asset (schema + basic lineage),
    calls the asset enrichment agent, and persists results to the store.

    Args:
        db:            Open Atlas database.
        repo_path:      Filesystem path to the code repository.
        provider:       Configured LLM provider.
        provider_name:  Provider identifier (e.g. 'anthropic'). Used for provenance.
        model:          Model identifier. Used for provenance.
        limit:          Max assets to annotate in this run.
        batch_size:     Max assets per LLM call.

    Returns:
        Number of assets successfully annotated.
    """
    from alma_atlas.agents.asset_enricher import analyze_assets
    from alma_atlas_store.annotation_repository import AnnotationRecord, AnnotationRepository
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_ids = get_unannotated_assets(db, limit=limit)
    if not asset_ids:
        logger.info("run_asset_enrichment: no unannotated assets found")
        return 0

    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)
    schema_repo = SchemaRepository(db)
    ann_repo = AnnotationRepository(db)

    # Preload edges once; we only need immediate neighbors.
    edges = edge_repo.list_all()
    upstream_by_asset: dict[str, list[str]] = {}
    downstream_by_asset: dict[str, list[str]] = {}
    for e in edges:
        downstream_by_asset.setdefault(e.upstream_id, []).append(e.downstream_id)
        upstream_by_asset.setdefault(e.downstream_id, []).append(e.upstream_id)

    annotated_by = f"agent:{provider_name}:{model}"
    annotated_count = 0

    # Batch asset contexts to respect context limits.
    for i in range(0, len(asset_ids), batch_size):
        batch_ids = asset_ids[i : i + batch_size]
        contexts: list[dict] = []
        for asset_id in batch_ids:
            asset = asset_repo.get(asset_id)
            if asset is None:
                continue
            schema = schema_repo.get_latest(asset_id)
            contexts.append(
                {
                    "asset_id": asset.id,
                    "source": asset.source,
                    "kind": asset.kind,
                    "name": asset.name,
                    "description": asset.description,
                    "tags": asset.tags,
                    "schema": (
                        {
                            "fingerprint": schema.fingerprint,
                            "columns": [
                                {"name": c.name, "type": c.type, "nullable": c.nullable}
                                for c in (schema.columns[:50] if schema else [])
                            ],
                        }
                        if schema
                        else None
                    ),
                    "lineage": {
                        "upstream": upstream_by_asset.get(asset_id, [])[:25],
                        "downstream": downstream_by_asset.get(asset_id, [])[:25],
                    },
                }
            )

        if not contexts:
            continue

        annotations = await analyze_assets(contexts, repo_path, provider)
        if not annotations:
            continue

        for ann in annotations:
            # Persist with provenance.
            try:
                ann_repo.upsert(
                    AnnotationRecord(
                        asset_id=ann.asset_id,
                        ownership=ann.ownership,
                        granularity=ann.granularity,
                        join_keys=ann.join_keys,
                        freshness_guarantee=ann.freshness_guarantee,
                        business_logic_summary=ann.business_logic_summary,
                        sensitivity=ann.sensitivity,
                        annotated_by=annotated_by,
                    )
                )
                annotated_count += 1
            except Exception as exc:
                logger.warning(
                    "run_asset_enrichment: failed to persist annotation for %s: %s",
                    ann.asset_id,
                    exc,
                )

    logger.info("run_asset_enrichment: %d asset(s) annotated", annotated_count)
    return annotated_count
