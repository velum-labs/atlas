#!/usr/bin/env python3
"""Print the curated set of source files that must pass `ty check`."""

from __future__ import annotations

import argparse
import shlex

TARGETS = [
    "packages/alma-atlas/src/alma_atlas/config.py",
    "packages/alma-atlas/src/alma_atlas/config_store.py",
    "packages/alma-atlas/src/alma_atlas/source_registry.py",
    "packages/alma-atlas/src/alma_atlas/source_runtime.py",
    "packages/alma-atlas/src/alma_atlas/local_secrets.py",
    "packages/alma-atlas/src/alma_atlas/async_utils.py",
    "packages/alma-atlas/src/alma_atlas/http_utils.py",
    "packages/alma-atlas/src/alma_atlas/graph_service.py",
    "packages/alma-atlas/src/alma_atlas/contract_validation.py",
    "packages/alma-atlas/src/alma_atlas/cli/common.py",
    "packages/alma-atlas/src/alma_atlas/cli/connect.py",
    "packages/alma-atlas/src/alma_atlas/cli/learn.py",
    "packages/alma-atlas/src/alma_atlas/cli/scan.py",
    "packages/alma-atlas/src/alma_atlas/cli/team.py",
    "packages/alma-atlas/src/alma_atlas/cli/serve.py",
    "packages/alma-atlas/src/alma_atlas/pipeline/learn.py",
    "packages/alma-atlas/src/alma_atlas/pipeline/scan.py",
    "packages/alma-atlas/src/alma_atlas/pipeline/scanner_v2.py",
    "packages/alma-atlas/src/alma_atlas/pipeline/stitch.py",
    "packages/alma-atlas/src/alma_atlas/enforcement/runtime.py",
    "packages/alma-atlas/src/alma_atlas/enforcement/engine.py",
    "packages/alma-atlas/src/alma_atlas/sync/client.py",
    "packages/alma-atlas-store/src/alma_atlas_store/schema_repository.py",
    "packages/alma-atlas-store/src/alma_atlas_store/violation_repository.py",
    "packages/alma-atlas-store/src/alma_atlas_store/consumer_repository.py",
    "packages/alma-sqlkit/src/alma_sqlkit/table_refs.py",
    "packages/alma-sqlkit/src/alma_sqlkit/parser/sql_parser.py",
    "packages/alma-sqlkit/src/alma_sqlkit/binder/sql_binder.py",
    "packages/alma-analysis/src/alma_analysis/lineage_extractor.py",
    "packages/alma-analysis/src/alma_analysis/lineage_inference.py",
    "packages/alma-analysis/src/alma_analysis/derived_analytics.py",
    "packages/alma-connectors/src/alma_connectors/source_adapter.py",
    "packages/alma-connectors/src/alma_connectors/source_adapter_service.py",
    "packages/alma-connectors/src/alma_connectors/adapters/bigquery.py",
    "packages/alma-connectors/src/alma_connectors/adapters/snowflake.py",
    "packages/alma-algebrakit/src/alma_algebrakit/rewriting/predicates.py",
    "scripts/sync-versions.py",
    "scripts/workspace_packages.py",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shell", action="store_true", help="Print shell-quoted targets on one line.")
    args = parser.parse_args()

    if args.shell:
        print(" ".join(shlex.quote(target) for target in TARGETS))
        return

    for target in TARGETS:
        print(target)


if __name__ == "__main__":
    main()
