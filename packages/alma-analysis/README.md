# alma-analysis

`alma-analysis` contains Atlas' graph and query-analysis logic.

## What lives here

- lineage extraction and inference
- schema-match and edge discovery heuristics
- consumer identity and derived analytics helpers
- table extraction and graph traversal utilities

## Architecture notes

- This package is intended to stay largely stateless and computation-focused.
- It consumes connector/store domain records but should avoid owning runtime orchestration or persistence policy.
- Analysis results are meant to feed the canonical Atlas runtime in `alma-atlas`, not create a second execution path.
