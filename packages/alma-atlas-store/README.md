# alma-atlas-store

`alma-atlas-store` is the SQLite-backed persistence layer for Atlas.

## What lives here

- the database wrapper and migrations in `src/alma_atlas_store/db.py`
- repository implementations for assets, edges, queries, schemas, contracts, consumers, annotations, and violations
- the concrete storage behavior used by the Atlas CLI, MCP tools, learning pipeline, and team sync flows

## Architecture notes

- Store record types are aligned with the shared contracts in `alma-ports`.
- Repositories are intentionally small and focused: each one maps closely to a single table or table family.
- Migrations under `src/alma_atlas_store/migrations/` define the on-disk schema and remain the source of truth for SQLite structure.
