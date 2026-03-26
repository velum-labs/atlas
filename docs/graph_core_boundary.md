# Graph Core Boundary

This document defines the long-term ownership boundary for lineage graph logic
across Atlas, Alma, and `bq-extraction`.

## Core decision

The reusable graph core lives in:

- `packages/alma-graph`

That package is the canonical home for:

- graph DTOs
- graph bundle / evidence DTOs
- graph transform contracts
- pure graph operations
- graph-tool serialization helpers

## Repo ownership

### Atlas

Atlas owns the reusable graph/discovery infrastructure:

- `alma-graph`
- `alma-analysis`
- `alma-atlas-store`
- `alma-connectors`
- `alma-atlas`

Atlas is the source of truth for the reusable graph contract and the reusable
graph operations.

### Alma

Alma owns:

- graph UI contracts
- grouped and progressive graph presentation
- router integration
- node / edge rendering and product workflows

Alma consumes Atlas-owned graph bundles. It does not own graph extraction or
graph serialization.

### `bq-extraction`

`bq-extraction` owns:

- BigQuery-specific artifact loading
- offline lineage construction from saved extraction artifacts
- local graph-cleaning policies that are too warehouse-specific to generalize
- notebook and CLI workflows

Examples of local policy that should remain here until generalized:

- `temp_incremental_tables.*_temp_<timestamp>` collapse
- BigQuery `INFORMATION_SCHEMA` probe detection
- job-history-specific interpretation rules

## Practical rule of thumb

If a capability answers one of these questions, it probably belongs in Atlas:

- What is the canonical graph type?
- How do I transform one graph into another graph?
- How do I traverse or extract a subgraph?
- How do I serialize a graph for external tools or downstream systems?

If a capability answers one of these questions, it probably belongs in
`bq-extraction`:

- How do I interpret these BigQuery extraction files?
- How do I normalize this warehouse-specific naming convention?
- How do I build a graph from these saved offline artifacts?

If a capability answers one of these questions, it probably belongs in Alma:

- How do I present this graph to a user?
- How do I group or expand it for the UI?
- How do I connect it to product workflows like search, drill-down, and review?
