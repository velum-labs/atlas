"""Post-scan orchestration helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from alma_atlas.config import AtlasConfig, SourceConfig


async def fire_drift_hooks(
    cfg: AtlasConfig,
    *,
    source_id: str,
    asset_count: int,
    blocked: bool,
    has_violations: bool,
) -> None:
    """Fire drift hooks when a scan produced enforcement violations."""
    if not has_violations or not cfg.hooks:
        return

    from alma_atlas.hooks import HookExecutor, make_drift_detected_event

    executor = HookExecutor(cfg.hooks)
    event = make_drift_detected_event(
        source_id=source_id,
        blocked=blocked,
        asset_count=asset_count,
    )
    await executor.fire(event)


async def run_multi_source_post_scan(
    *,
    sources: list[SourceConfig],
    results: list[Any],
    cfg: AtlasConfig,
    repo_path: Path | None,
    no_learn: bool,
) -> int:
    """Run cross-system edge discovery and optional learning after scans finish."""
    from alma_atlas.pipeline.cross_system_edges import (
        discover_cross_system_edges,
        resolve_dbt_source_edges,
    )
    from alma_atlas.pipeline.learn import (
        asset_annotation_is_enabled,
        edge_learning_is_enabled,
        run_asset_annotation,
        run_edge_learning,
    )
    from alma_atlas_store.db import Database

    if cfg.db_path is None:
        raise ValueError("Atlas db_path is not configured")

    snapshots = {
        result.source_id: result.snapshot
        for result in results
        if getattr(result, "snapshot", None) is not None
    }
    kind_by_id = {source.id: source.kind for source in sources}
    dbt_snapshots = {
        source_id: snapshot
        for source_id, snapshot in snapshots.items()
        if kind_by_id.get(source_id) == "dbt"
    }
    warehouse_snapshots = {
        source_id: snapshot
        for source_id, snapshot in snapshots.items()
        if kind_by_id.get(source_id) != "dbt"
    }

    cross_system_edge_count = 0
    if len(snapshots) >= 2:
        with Database(cfg.db_path) as db, db.transaction():
            cross_system_edge_count = discover_cross_system_edges(snapshots, db)
            cross_system_edge_count += resolve_dbt_source_edges(dbt_snapshots, warehouse_snapshots, db)

    if repo_path is not None and not no_learn:
        with Database(cfg.db_path) as db, db.transaction():
            if edge_learning_is_enabled(cfg.learning):
                await run_edge_learning(db, repo_path, config=cfg.learning)
            if asset_annotation_is_enabled(cfg.learning):
                await run_asset_annotation(db, repo_path, config=cfg.learning)

    return cross_system_edge_count
