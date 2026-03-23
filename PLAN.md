# Atlas Overnight Build Plan — Mar 23, 2026

## Current State
- Package scaffold ✅ (7 packages, uv workspace)
- SQLite store ✅ (6 repos, 1 migration, all code written)
- CLI skeleton ✅ (connect, scan, search, lineage, export, serve, status)
- MCP server ✅ (4 core tools)
- BigQuery adapter ✅ (extracted from velum monorepo)
- Postgres adapter ✅ (extracted)
- Analysis functions ✅ (lineage_extractor, edge_discovery, consumer_identity)

## Critical Broken Things
1. **Tests don't collect** — 11 errors. `__pycache__` conflicts + missing module imports
2. **Phantom modules** — `alma_analysis.edges` and `alma_analysis.lineage` referenced in stitch.py, lineage CLI, MCP tools — DON'T EXIST. Real modules: `edge_discovery`, `lineage_extractor`
3. **`alma_connectors.domain`** — referenced in stitch.py but doesn't exist. `TrafficObservationResult` is in `alma_connectors.source_adapter`
4. **Adapter interface mismatch** — scan.py calls `adapter.list_assets()` / `adapter.get_traffic()` but real protocol uses `introspect_schema()` / `observe_traffic()`
5. **No dbt adapter** — scan.py imports `alma_connectors.dbt.DbtAdapter` which doesn't exist
6. **No Snowflake adapter** — same
7. **No tests for store, CLI, MCP, pipeline**

## Build Order (parallel where possible)

### Phase 1: Fix Foundation (Agent 1)
- Fix all phantom imports (edges→edge_discovery, lineage→lineage_extractor, domain→source_adapter)
- Fix test collection (__pycache__ conflicts, conftest)
- Get all extracted tests passing (algebrakit, sqlkit, connectors, analysis)

### Phase 2: New Adapters + Pipeline (Agents 2-3, parallel)
- **Agent 2**: dbt adapter (ENG-383) — manifest.json parser, v12+v20 support, tests
- **Agent 3**: Fix scan pipeline to use real SourceAdapter protocol + write Snowflake adapter stub

### Phase 3: Tests + MCP Polish (Agent 4)
- Comprehensive store tests (all 6 repos)
- MCP analysis tools (ENG-387)
- Integration smoke test (connect → scan → search → lineage)

### Phase 4: Documentation (ENG-392)
- README with quickstart
- MCP tool reference
- Config reference

## Linear Tickets Covered
- ENG-378 ✅ (already done)
- ENG-379 ✅ (already done, needs tests)  
- ENG-380 ✅ (already done, needs tests)
- ENG-381 ✅ (already done, needs fixes)
- ENG-382: BQ adapter CLI integration (Phase 1 fixes)
- ENG-383: dbt adapter (Phase 2)
- ENG-384: Scan pipeline (Phase 2)
- ENG-385: Identity stitching (Phase 2)
- ENG-386: MCP core tools (Phase 1 fixes)
- ENG-387: MCP analysis tools (Phase 3)
- ENG-388: CLI search + lineage (Phase 1 fixes)
- ENG-389: Snowflake adapter (Phase 2)
- ENG-392: Documentation (Phase 4)
