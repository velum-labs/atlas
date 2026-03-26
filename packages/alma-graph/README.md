# alma-graph

`alma-graph` is the canonical reusable graph core for Atlas and Alma.

It owns:

- graph DTOs (`LineageNode`, `LineageEdge`, `LineageGraph`, etc.)
- evidence/bundle DTOs (`EvidenceOverlay`, `GraphBundle`, `Provenance`)
- transform contracts (`GraphTransform`, `TransformPipeline`)
- pure graph operations (`to_networkx_digraph`, bounded subgraphs)
- graph-tool serialization (`GraphML`, `GEXF`, `NDJSON`, chunked JSON)

It deliberately does **not** own:

- SQL parsing and lineage inference
- connector-specific artifact loading
- persistence backends
- product/UI concerns

Those remain in:

- `alma-analysis` for SQL/statistical lineage logic
- `alma-connectors` for source adapters
- `alma-atlas-store` for SQLite persistence
- `alma-atlas` for CLI/MCP orchestration
- `alma` for graph UI and product workflows
- `bq-extraction` for BigQuery-specific offline extraction and local graph-cleaning policies
