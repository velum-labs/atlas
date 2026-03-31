# alma-atlas

The `alma-atlas` package is the canonical runtime for Atlas.

It owns:

- CLI entrypoints under `src/alma_atlas/cli/`
- the authoritative scan orchestration in `src/alma_atlas/pipeline/scan.py`
- learning / enrichment orchestration in `src/alma_atlas/pipeline/learn.py`
- MCP server and tools in `src/alma_atlas/mcp/`
- team sync flows in `src/alma_atlas/sync/`
- runtime configuration in `src/alma_atlas/config.py`

## Runtime architecture

The current runtime model is:

- source registration rules and defaults live in `src/alma_atlas/source_specs.py`
- source-to-runtime adapter construction lives in `src/alma_atlas/source_runtime.py`
- graph read/query helpers shared by CLI and MCP live in `src/alma_atlas/graph_service.py`
- `src/alma_atlas/pipeline/scan.py` remains the single authoritative scan spine
- `src/alma_atlas/pipeline/scanner_v2.py` is a compatibility facade plus capability-planning helpers

## Notes

- Learning is ACP-native: `learning.agent.command` (or `provider: acp`) defines the runtime boundary for real learning runs
- `explorer`, `pipeline_analyzer`, and `annotator` are workflow roles, not necessarily separate subprocesses
- When those roles resolve to the same ACP subprocess settings, Atlas reuses one ACP session per learning invocation
- When the ACP runtime exposes direct repo access, Atlas can skip the custom codebase explorer fallback and let the external agent inspect the repo directly
- `sources.json` is the persisted registry for `alma-atlas connect`
- `atlas.yml` and `--connections` can override runtime sources without mutating persisted state
