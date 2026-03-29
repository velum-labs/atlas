"""Enrichment orchestrator — identifies unenriched edges and calls the pipeline agent.

After cross-system edge discovery (schema_match / dbt_source_ref edges), this
module enriches those edges with transport metadata inferred from a code
repository by the :mod:`alma_atlas.agents.pipeline_analyzer` agent.

Enrichment is idempotent: edges that already carry ``enrichment_status=enriched``
in their metadata are skipped.

Lead/Specialist pattern
-----------------------
When an :class:`~alma_atlas.config.EnrichmentConfig` is provided the orchestrator
uses a *lead/specialist* multi-agent pattern:

1. The **explorer** agent (cheap model) performs a two-pass file selection to
   find repository files relevant to the batch of edges or assets.
2. The **specialist** agent(s) receive only the pre-filtered files, reducing
   token usage and improving signal.

Both :func:`run_enrichment` and :func:`run_asset_enrichment` accept either a
single ``provider`` argument (legacy path) **or** a ``config`` keyword argument
(new path).  Both calling conventions are supported simultaneously.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.edge_repository import Edge, EdgeRepository

if TYPE_CHECKING:
    from alma_atlas.agents.provider import LLMProvider
    from alma_atlas.config import AgentConfig, EnrichmentConfig
    from alma_atlas_store.db import Database

logger = logging.getLogger(__name__)

# Edge kinds that are candidates for pipeline enrichment.
#
# We include `depends_on` because dbt/SQL lineage edges are often the ones that have
# meaningful transport metadata (schedule, strategy, owner) in the surrounding pipeline code.
_ENRICHABLE_KINDS: frozenset[str] = frozenset({"schema_match", "dbt_source_ref", "depends_on"})


def _provider_from_agent_config(agent_cfg: AgentConfig) -> LLMProvider:
    """Instantiate an LLMProvider from an :class:`~alma_atlas.config.AgentConfig`."""
    from alma_atlas.agents.provider import make_provider

    api_key: str | None = None
    if agent_cfg.api_key_env:
        api_key = os.environ.get(agent_cfg.api_key_env)
    return make_provider(
        agent_cfg.provider,
        model=agent_cfg.model,
        api_key=api_key,
        timeout=float(agent_cfg.timeout),
        max_tokens=agent_cfg.max_tokens,
    )


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
        # Skip identity edges like pg::raw.users -> dbt::raw.users; there's no "transport" to infer.
        and e.upstream_id.split("::", 1)[-1] != e.downstream_id.split("::", 1)[-1]
        and e.metadata.get("enrichment_status") != "enriched"
    ]


def _object_part(asset_id: str) -> str:
    """Extract the ``schema.table`` portion from an asset ID like ``src::schema.table``."""
    return asset_id.split("::", 1)[-1] if "::" in asset_id else asset_id


async def run_enrichment(
    db: Database,
    repo_path: Path,
    provider: LLMProvider | None = None,
    *,
    config: EnrichmentConfig | None = None,
) -> int:
    """Enrich unenriched cross-system edges with pipeline transport metadata.

    Collects all unenriched edges, calls the pipeline analysis agent, and
    persists the returned :class:`~alma_atlas.agents.schemas.EdgeEnrichment`
    data back to the store.  Edges for which the agent returns no result are
    left unchanged.  Persistence failures are logged as warnings and do not
    abort the run.

    When *config* is provided the lead/specialist pattern is used:
    - The explorer agent (``config.explorer``) pre-filters repository files.
    - The pipeline analyzer (``config.pipeline_analyzer``) receives only
      the relevant files.

    Args:
        db:        Open :class:`~alma_atlas_store.db.Database` connection.
        repo_path: Filesystem path to the code repository to scan.
        provider:  Configured :class:`~alma_atlas.agents.provider.LLMProvider`
                   (legacy path, used when *config* is ``None``).
        config:    Per-agent :class:`~alma_atlas.config.EnrichmentConfig`
                   (new path; takes precedence over *provider*).

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

    if config is not None:
        # Lead/specialist pattern.
        from alma_atlas.agents.codebase_explorer import explore_for_edges

        explorer_provider = _provider_from_agent_config(config.explorer)
        analyzer_provider = _provider_from_agent_config(config.pipeline_analyzer)

        pre_filtered = await explore_for_edges(unenriched, repo_path, explorer_provider)
        logger.debug(
            "run_enrichment: explorer pre-filtered %d file(s)",
            len(pre_filtered),
        )

        enrichments = await analyze_edges(
            unenriched,
            repo_path,
            analyzer_provider,
            pre_filtered_files=pre_filtered,
        )
    elif provider is not None:
        # Legacy single-provider path.
        enrichments = await analyze_edges(unenriched, repo_path, provider)
    else:
        raise ValueError("run_enrichment requires either 'provider' or 'config'")

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
    provider: LLMProvider | None = None,
    *,
    config: EnrichmentConfig | None = None,
    provider_name: str | None = None,
    model: str | None = None,
    limit: int = 100,
    batch_size: int = 20,
) -> int:
    """Enrich assets with supplementary business metadata annotations.

    This is the P2 "Codex enrichment" path. It selects assets that have not yet
    been annotated, builds a context payload per asset (schema + basic lineage),
    calls the asset enrichment agent, and persists results to the store.

    When *config* is provided the lead/specialist pattern is used:
    - The explorer agent (``config.explorer``) pre-filters repository files.
    - The asset enricher (``config.asset_enricher``) receives only relevant files.

    Args:
        db:            Open Atlas database.
        repo_path:      Filesystem path to the code repository.
        provider:       Configured LLM provider (legacy path).
        config:         Per-agent EnrichmentConfig (new path; takes precedence).
        provider_name:  Provider identifier used for provenance (legacy path).
        model:          Model identifier used for provenance (legacy path).
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

    # Resolve provider and provenance from config or legacy args.
    if config is not None:
        enricher_provider = _provider_from_agent_config(config.asset_enricher)
        _provider_name = config.asset_enricher.provider
        _model = config.asset_enricher.model
    elif provider is not None:
        enricher_provider = provider
        _provider_name = provider_name or "unknown"
        _model = model or "unknown"
    else:
        raise ValueError("run_asset_enrichment requires either 'provider' or 'config'")

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

    annotated_by = f"agent:{_provider_name}:{_model}"
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

        # Run explorer once per batch when using the lead/specialist pattern.
        if config is not None:
            from alma_atlas.agents.codebase_explorer import explore_for_assets

            explorer_provider = _provider_from_agent_config(config.explorer)
            pre_filtered = await explore_for_assets(contexts, repo_path, explorer_provider)
            logger.debug(
                "run_asset_enrichment: explorer pre-filtered %d file(s) for batch %d",
                len(pre_filtered),
                i // batch_size,
            )
            annotations = await analyze_assets(
                contexts,
                repo_path,
                enricher_provider,
                pre_filtered_files=pre_filtered,
            )
        else:
            annotations = await analyze_assets(contexts, repo_path, enricher_provider)

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
