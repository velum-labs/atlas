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
