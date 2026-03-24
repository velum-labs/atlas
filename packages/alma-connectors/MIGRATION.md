# alma-connectors: v1 → v2 Migration Guide

## Overview

The v1 `SourceAdapter` protocol is **deprecated as of 0.2.0** and will be **removed in 1.0.0**.
All new adapter code should implement `SourceAdapterV2` from `alma_connectors.source_adapter_v2`.

## Method Mapping

| v1 method (`SourceAdapter`) | v2 equivalent (`SourceAdapterV2`) | Notes |
|---|---|---|
| `test_connection()` | `probe()` | v2 `probe()` returns per-capability `CapabilityProbeResult` tuples, not a single `ConnectionTestResult` |
| `introspect_schema()` | `extract_schema()` | Returns `SchemaSnapshotV2` with richer `SchemaObject` (routines, ML models, freshness) instead of `SchemaSnapshot` |
| `observe_traffic()` | `extract_traffic()` | Returns `TrafficExtractionResult` with `ExtractionMeta` provenance instead of `TrafficObservationResult` |
| `execute_query()` | `execute_query()` | Signature is compatible; v2 adds a `dry_run` keyword argument |
| `get_setup_instructions()` | `get_setup_instructions()` | Identical |
| *(not present)* | `probe()` | New: runtime capability probing against `declared_capabilities` |
| *(not present)* | `discover()` | New: enumerate containers/namespaces (`DISCOVER` capability) |
| *(not present)* | `extract_definitions()` | New: DDL/compiled SQL extraction (`DEFINITIONS` capability) |
| *(not present)* | `extract_lineage()` | New: data-flow edge extraction (`LINEAGE` capability) |
| *(not present)* | `extract_orchestration()` | New: DAG/task extraction (`ORCHESTRATION` capability) |

## Migration Path

### 1. Implement `SourceAdapterV2` instead of `SourceAdapter`

```python
# Before (v1)
from alma_connectors.source_adapter import SourceAdapter, SchemaSnapshot

class MyAdapter:
    kind = SourceAdapterKind.POSTGRES
    capabilities = SourceAdapterCapabilities(...)

    async def introspect_schema(self, adapter) -> SchemaSnapshot: ...
    async def observe_traffic(self, adapter, *, since=None) -> TrafficObservationResult: ...
    async def test_connection(self, adapter) -> ConnectionTestResult: ...

# After (v2)
from alma_connectors.source_adapter_v2 import (
    SourceAdapterV2, AdapterCapability, SchemaSnapshotV2, TrafficExtractionResult,
    CapabilityProbeResult,
)

class MyAdapter:
    declared_capabilities = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.SCHEMA,
        AdapterCapability.TRAFFIC,
    })

    async def probe(self, adapter, capabilities=None) -> tuple[CapabilityProbeResult, ...]: ...
    async def extract_schema(self, adapter) -> SchemaSnapshotV2: ...
    async def extract_traffic(self, adapter, *, since=None) -> TrafficExtractionResult: ...
```

### 2. Update capability checks

```python
# Before (v1)
if adapter.capabilities.can_introspect_schema:
    snapshot = await my_adapter.introspect_schema(adapter)

# After (v2)
probe_results = await my_adapter.probe(adapter)
schema_result = next((r for r in probe_results if r.capability == AdapterCapability.SCHEMA), None)
if schema_result and schema_result.available:
    snapshot = await my_adapter.extract_schema(adapter)
```

### 3. Update imports

```python
# Before (v1)
from alma_connectors import SourceAdapter, SchemaSnapshot, TrafficObservationResult

# After (v2)
from alma_connectors import (
    SourceAdapterV2,
    SchemaSnapshotV2,
    TrafficExtractionResult,
    AdapterCapability,
    CapabilityProbeResult,
)
```

## Timeline

| Version | Status |
|---|---|
| 0.2.0 | `SourceAdapter` v1 deprecated; `SourceAdapterV2` available |
| 1.0.0 | `SourceAdapter` v1 **removed** |
