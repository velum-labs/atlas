# Atlas Architecture

## Principles
- One concept has one canonical model.
- One behavior has one implementation path.
- Libraries compute; the application layer orchestrates.
- Configuration and secrets live at the edges.
- Persistence implementations obey the ports they claim to implement.
- Public package surfaces stay narrow and explicit.

## Canonical Owners
- `alma-ports`: canonical storage contracts and shared persisted record types.
- `alma-atlas-store`: SQLite implementations of those contracts.
- `alma-connectors`: source adapter configs, capability protocols, and external-system extraction.
- `alma-analysis`: pure graph and SQL-derived transformations.
- `alma-sqlkit`: canonical SQL normalization and shared SQL reference extraction.
- `alma-algebrakit`: relational algebra and semantic core.
- `alma-atlas`: orchestration, application services, CLI, MCP, sync, and config precedence.

## Runtime Boundaries
- Source kind definitions, allowed params, and secret paths are owned by `alma_atlas.source_registry`.
- Local filesystem persistence is owned by `alma_atlas.config_store`.
- Local secret encryption-at-rest is owned by `alma_atlas.local_secrets`.
- Read-oriented graph access is owned by `alma_atlas.graph_service`.
- HTTP retry behavior is owned by `alma_atlas.http_utils`.

## Storage Model
- The persisted Atlas graph uses `alma_ports.edge.GraphEdge` as the canonical edge shape.
- Connector-specific transport models may be richer, but they adapt into the graph edge model before persistence.
- Schema, asset, query, annotation, and violation records should likewise cross the storage boundary through `alma-ports`.
