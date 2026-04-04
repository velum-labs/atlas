"""Learning orchestrator for ACP-backed Atlas workflows.

After cross-system edge discovery (schema_match / dbt_source_ref edges), this
module learns those edges with transport metadata inferred from a code
repository by the :mod:`alma_atlas.agents.pipeline_analyzer` workflow.

Learning is idempotent: edges that already carry ``learning_status=learned``
in their metadata are skipped.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from alma_atlas.application.learning.runtime import (
    asset_annotation_is_enabled,
    edge_learning_is_enabled,
)
from alma_atlas.application.learning.runtime import (
    close_owned_learning_runtime as _close_owned_learning_runtime,
)
from alma_atlas.application.learning.runtime import (
    effective_provider_name as _effective_provider_name,
)
from alma_atlas.application.learning.runtime import (
    provider_from_agent_config as _provider_from_agent_config,
)
from alma_atlas.application.learning.runtime import (
    shared_runtime_for_configs as _shared_runtime_for_configs,
)
from alma_atlas.application.learning.runtime import (
    supports_direct_repo_exploration as _supports_direct_repo_exploration,
)
from alma_atlas.application.learning.store_updates import (
    build_asset_annotation_contexts,
    persist_annotations,
    persist_edge_learning,
)
from alma_ports.edge import Edge

if TYPE_CHECKING:
    from alma_atlas.config import LearningConfig
    from alma_atlas_store.db import Database

logger = logging.getLogger(__name__)

# Edge kinds that are candidates for pipeline learning.
#
# We include `depends_on` because dbt/SQL lineage edges are often the ones that have
# meaningful transport metadata (schedule, strategy, owner) in the surrounding pipeline code.
_LEARNABLE_KINDS: frozenset[str] = frozenset({"schema_match", "dbt_source_ref", "depends_on"})


def get_unlearned_edges(db: Database, *, source_prefix: str | None = None) -> list[Edge]:
    """Return edges that have not yet been learned by the pipeline analysis workflow.

    An edge qualifies when its :attr:`~alma_atlas_store.edge_repository.Edge.kind`
    is one of the learnable kinds (``schema_match`` or ``dbt_source_ref``) and
    its metadata does not contain ``learning_status: learned``.

    Args:
        db: Open :class:`~alma_atlas_store.db.Database` connection.
        source_prefix: When provided, only return edges where at least one of
            upstream_id or downstream_id starts with ``<source_prefix>::``.

    Returns:
        List of unlearned :class:`~alma_atlas_store.edge_repository.Edge` objects.
    """
    from alma_atlas_store.edge_repository import EdgeRepository

    prefix = f"{source_prefix}::" if source_prefix is not None else None
    repo = EdgeRepository(db)
    return [
        e
        for e in repo.list_all()
        if e.kind in _LEARNABLE_KINDS
        # Cross-system edges (schema_match, dbt_source_ref) are ALWAYS learnable even when
        # schema.table matches — they represent real data flow between different systems
        # (e.g. pg::raw.users → dbt::raw.users = "how does raw data get loaded?").
        # Only skip true self-loops within the same source system.
        and not (
            e.upstream_id.split("::", 1)[0] == e.downstream_id.split("::", 1)[0]
            and e.upstream_id.split("::", 1)[-1] == e.downstream_id.split("::", 1)[-1]
        )
        and e.metadata.get("learning_status") != "learned"
        and (
            prefix is None
            or e.upstream_id.startswith(prefix)
            or e.downstream_id.startswith(prefix)
        )
    ]


def _object_part(asset_id: str) -> str:
    """Extract the ``schema.table`` portion from an asset ID like ``src::schema.table``."""
    return asset_id.split("::", 1)[-1] if "::" in asset_id else asset_id


async def run_edge_learning(
    db: Database,
    repo_path: Path,
    *,
    config: LearningConfig,
) -> int:
    """Learn unlearned cross-system edges with pipeline transport metadata.

    Collects all unlearned edges, calls the pipeline analysis workflow, and
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
        config: Per-agent :class:`~alma_atlas.config.LearningConfig`.

    Returns:
        Number of edges successfully learned and persisted.
    """
    from alma_atlas.agents.pipeline_analyzer import analyze_edges

    unlearned = get_unlearned_edges(db)
    if not unlearned:
        logger.info("run_edge_learning: no unlearned edges found")
        return 0

    logger.info(
        "run_edge_learning: learning %d edge(s) from %s",
        len(unlearned),
        repo_path,
    )

    if not edge_learning_is_enabled(config):
        raise ValueError(
            "run_edge_learning requires non-mock explorer and pipeline_analyzer agent configs"
        )
    from alma_atlas.agents.codebase_explorer import explore_for_edges

    shared_runtime = _shared_runtime_for_configs(
        [config.explorer, config.pipeline_analyzer],
        repo_path=repo_path,
    )
    explorer_provider = _provider_from_agent_config(
        config.explorer,
        repo_path=repo_path,
        runtime=shared_runtime,
    )
    analyzer_provider = _provider_from_agent_config(
        config.pipeline_analyzer,
        repo_path=repo_path,
        runtime=shared_runtime,
    )
    if shared_runtime is not None:
        logger.debug("run_edge_learning: using shared ACP runtime for explorer and analyzer")

    try:
        seen_pairs: set[tuple[str, str]] = set()
        deduped: list[Edge] = []
        for edge in unlearned:
            pair = (_object_part(edge.upstream_id), _object_part(edge.downstream_id))
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                deduped.append(edge)

        if len(deduped) < len(unlearned):
            logger.info(
                "run_edge_learning: deduplicated %d → %d edges for agent",
                len(unlearned),
                len(deduped),
            )

        edge_batch_size = 10
        enrichments = []
        use_direct_repo = _supports_direct_repo_exploration(analyzer_provider)
        if use_direct_repo:
            logger.debug("run_edge_learning: analyzer will inspect repository directly via ACP")
        for batch_start in range(0, len(deduped), edge_batch_size):
            batch = deduped[batch_start : batch_start + edge_batch_size]
            logger.info(
                "run_edge_learning: batch %d/%d (%d edges)",
                batch_start // edge_batch_size + 1,
                (len(deduped) + edge_batch_size - 1) // edge_batch_size,
                len(batch),
            )

            if use_direct_repo:
                pre_filtered = None
            else:
                pre_filtered = await explore_for_edges(batch, repo_path, explorer_provider)
                logger.debug(
                    "run_edge_learning: explorer pre-filtered %d file(s)",
                    len(pre_filtered),
                )

            batch_enrichments = await analyze_edges(
                batch,
                repo_path,
                analyzer_provider,
                pre_filtered_files=pre_filtered,
                allow_repo_exploration=use_direct_repo,
            )
            enrichments.extend(batch_enrichments)
    finally:
        await _close_owned_learning_runtime(
            runtime=shared_runtime,
            providers=[explorer_provider, analyzer_provider],
        )

    if not enrichments:
        logger.info("run_edge_learning: agent returned no results")
        return 0

    learned_count = persist_edge_learning(db, unlearned, enrichments)
    logger.info("run_edge_learning: %d edge(s) learned", learned_count)
    return learned_count


def get_unannotated_assets(db: Database, *, limit: int = 100, source_prefix: str | None = None) -> list[str]:
    """Return asset IDs that have no annotation record yet.

    Args:
        db: Open Atlas database connection.
        limit: Maximum number of asset IDs to return.
        source_prefix: When provided, only return assets from this source
            (i.e. whose ID starts with ``<source_prefix>::``).
    """
    from alma_atlas_store.annotation_repository import AnnotationRepository

    return AnnotationRepository(db).list_unannotated(limit=limit, source_prefix=source_prefix)


async def run_asset_annotation(
    db: Database,
    repo_path: Path,
    *,
    config: LearningConfig,
    limit: int = 100,
    batch_size: int = 20,
) -> int:
    """Annotate assets with supplementary business metadata.

    This is the P2 "Codex learning" path. It selects assets that have not yet
    been annotated, builds a context payload per asset (schema + basic lineage),
    calls the annotator workflow, and persists results to the store.

    Args:
        db: Open Atlas database.
        repo_path: Filesystem path to the code repository.
        config: Per-agent LearningConfig.
        limit: Max assets to annotate in this run.
        batch_size: Max assets per LLM call.

    Returns:
        Number of assets successfully annotated.
    """
    from alma_atlas.agents.annotator import analyze_assets
    asset_ids = get_unannotated_assets(db, limit=limit)
    if not asset_ids:
        logger.info("run_asset_annotation: no unannotated assets found")
        return 0

    if not asset_annotation_is_enabled(config):
        raise ValueError(
            "run_asset_annotation requires non-mock explorer and annotator agent configs"
        )

    provider_name = _effective_provider_name(config.annotator)
    model_name = config.annotator.model
    annotated_by = f"agent:{provider_name}:{model_name}"
    annotated_count = 0

    shared_runtime = _shared_runtime_for_configs(
        [config.explorer, config.annotator],
        repo_path=repo_path,
    )
    enricher_provider = _provider_from_agent_config(
        config.annotator,
        repo_path=repo_path,
        runtime=shared_runtime,
    )
    explorer_provider = _provider_from_agent_config(
        config.explorer,
        repo_path=repo_path,
        runtime=shared_runtime,
    )
    if shared_runtime is not None:
        logger.debug("run_asset_annotation: using shared ACP runtime for explorer and annotator")

    try:
        # Batch asset contexts to respect context limits.
        use_direct_repo = _supports_direct_repo_exploration(enricher_provider)
        if use_direct_repo:
            logger.debug("run_asset_annotation: annotator will inspect repository directly via ACP")
        from alma_atlas_store.profiling_repository import ProfilingRepository as _ProfilingRepository
        profiling_repo = _ProfilingRepository(db)
        for i in range(0, len(asset_ids), batch_size):
            batch_ids = asset_ids[i : i + batch_size]
            column_profiles = {
                asset_id: profiles
                for asset_id in batch_ids
                if (profiles := profiling_repo.list_for_asset(asset_id))
            }
            contexts = build_asset_annotation_contexts(db, batch_ids, column_profiles=column_profiles or None)
            if not contexts:
                continue

            from alma_atlas.agents.codebase_explorer import explore_for_assets

            if use_direct_repo:
                pre_filtered = None
            else:
                pre_filtered = await explore_for_assets(contexts, repo_path, explorer_provider)
                logger.debug(
                    "run_asset_annotation: explorer pre-filtered %d file(s) for batch %d",
                    len(pre_filtered),
                    i // batch_size,
                )
            annotations = await analyze_assets(
                contexts,
                repo_path,
                enricher_provider,
                pre_filtered_files=pre_filtered,
                allow_repo_exploration=use_direct_repo,
            )

            if not annotations:
                continue

            annotated_count += persist_annotations(
                db,
                annotations=annotations,
                annotated_by=annotated_by,
            )
    finally:
        await _close_owned_learning_runtime(
            runtime=shared_runtime,
            providers=[explorer_provider, enricher_provider],
        )

    logger.info("run_asset_annotation: %d asset(s) annotated", annotated_count)
    return annotated_count
