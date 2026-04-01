"""Application-facing entrypoints for scan workflows."""

from __future__ import annotations


async def run_scan_async(source, cfg, *, timeout=300.0, dry_run=False):
    from alma_atlas.pipeline.scan import run_scan_async as pipeline_run_scan_async

    return await pipeline_run_scan_async(source, cfg, timeout=timeout, dry_run=dry_run)


def run_scan(source, cfg, *, timeout=300.0):
    from alma_atlas.pipeline.scan import run_scan as pipeline_run_scan

    return pipeline_run_scan(source, cfg, timeout=timeout)


def run_scan_all(
    sources,
    cfg,
    *,
    max_concurrent=4,
    timeout=300.0,
    repo_path=None,
    no_learn=False,
):
    from alma_atlas.pipeline.scan import run_scan_all as pipeline_run_scan_all

    return pipeline_run_scan_all(
        sources,
        cfg,
        max_concurrent=max_concurrent,
        timeout=timeout,
        repo_path=repo_path,
        no_learn=no_learn,
    )
