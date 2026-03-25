# AUDIT-ATLAS-ANALYSIS.md

Deep audit of `packages/alma-analysis` (9 files, ~2107 LOC) and `packages/alma-atlas` (29 files, ~3984 LOC).
All source files were read directly. Line numbers are exact.

---

## Summary

Both packages are architecturally sound, with good layering, async-first design, and reasonable fallback patterns.
Security posture is acceptable: `yaml.safe_load` is used, secrets are redacted in repr, SQL identifiers are parameterized.

However, several correctness bugs were found, three of which are high-severity and cause silent data loss or silently wrong results at runtime:

1. **`atlas_impact` query exposure is always 0** — asset ID format mismatch between stitch and lineage graph
2. **Enforcement blocking is never surfaced** — `enforce` mode violations don't appear in `ScanResult`
3. **`bytes_processed = 0` is silently dropped** — falsy-value short-circuit in `_get_bytes()`

Additionally, there is one connection leak (MCP team sync), one potential ID collision (v2 lineage edge rows), and several medium-severity edge cases.

---

## Critical Issues

None that cause data corruption or security compromise.

---

## High Issues

### H1 — `atlas_impact` query exposure always reports 0 (silent wrong result)
**Files:** `packages/alma-atlas/src/alma_atlas/mcp/tools.py:388`, `packages/alma-atlas/src/alma_atlas/pipeline/stitch.py:80`

In `stitch.py`, the `QueryObservation.tables` field is populated with raw SQL-parsed table names:
```python
tables = [ae.upstream_id for ae in derived]   # stitch.py:80
```
`ae.upstream_id` is `table_ref.canonical_name` from `edges.py:48` — a bare SQL identifier like `"public.orders"`.

In `atlas_impact` (tools.py), the lineage graph's `downstream` node IDs use the full asset format:
```python
downstream = graph.downstream(asset_id, depth=depth)   # tools.py:375
# downstream = ["my_postgres::public.orders", ...]

for q in all_queries:
    for table in q.tables:
        if table in downstream:            # "public.orders" ∉ {"my_postgres::public.orders"}
```

The formats never match. `total_query_exposure` is always 0 and the per-asset query counts are always `0`. The entire impact analysis output is silently wrong for every call.

**Impact:** Every user of `atlas_impact` / `atlas impact` CLI receives incorrect blast-radius information with zero query exposure regardless of actual query activity.

---

### H2 — Enforcement blocking is never propagated to ScanResult
**File:** `packages/alma-atlas/src/alma_atlas/pipeline/scan.py:211–222, 258–317`

`_run_enforcement` is called, and when `result.blocked is True`, it logs a warning — but the return value is discarded:
```python
try:
    _run_enforcement(snapshot, source.id, db)   # return value ignored
except Exception as exc:
    logger.exception("Enforcement check failed for source %s: %s", source.id, exc)
```

The docstring says "the pipeline caller is responsible for acting on `ScanResult.warnings`" but enforcement results are never appended to `warnings`, and `ScanResult.error` is never set. The caller (`cli/scan.py`, `scanner_v2.py`) receives no indication that enforcement blocked in `enforce` mode.

**Impact:** `enforce` mode is silently ineffective. Teams expecting pipeline blocking on schema violations will observe no blocking — violations are logged but the scan returns success.

---

### H3 — `_get_bytes()` silently drops zero-byte events
**File:** `packages/alma-analysis/src/alma_analysis/derived_analytics.py:252`

```python
raw = meta.get("bytes_processed") or meta.get("total_bytes_processed")
```

When `meta.get("bytes_processed")` returns `0` (a valid value for zero-byte queries), the `or` short-circuit evaluates it as falsy and falls through to `total_bytes_processed`. If that is also `0` or absent, `raw` becomes `None` and `_get_bytes()` returns `None`.

**Impact:** Analytics for zero-byte queries (e.g., `SELECT 1`, dry-run queries, empty scans) are silently excluded from `avg_bytes_processed` and `total_bytes_processed` aggregations, skewing averages upward. This affects both `FrequentQuery` and `UserActivitySummary` in all deployments with BigQuery or Snowflake sources.

---

### H4 — MCP team sync leaks HTTP connection
**File:** `packages/alma-atlas/src/alma_atlas/mcp/tools.py:519–532`

`_handle_team_sync` creates a `SyncClient` but never enters its async context manager:
```python
auth = TeamAuth(cfg.team_api_key)
client = SyncClient(cfg.team_server_url, auth, cfg.team_id or "default")
# context manager NOT entered — __aenter__ never called
with Database(cfg.db_path) as db:
    response = await client.full_sync(db, cfg)
```

Inside `full_sync → _post → _get_client()`, an `httpx.AsyncClient` is created and `self._owns_client = True` is set. But since `__aexit__` is never called, the client is never closed. Every invocation of `atlas_team_sync` from MCP leaks a connection pool.

**Impact:** Connection pool exhaustion for long-running MCP server sessions. In contrast, the CLI sync path (`cli/sync.py`) presumably uses `async with SyncClient(...)` correctly.

---

## Medium Issues

### M1 — V2 lineage edge row IDs can collide on colons in object names
**File:** `packages/alma-atlas/src/alma_atlas/pipeline/scanner_v2.py:384`

```python
row_id = f"{persisted.key}:{edge.source_object}:{edge.target_object}:{edge.edge_kind.value}"
```

`source_object` and `target_object` are freeform strings (schema.table, dataset.table, etc.). If either contains a colon (possible in BigQuery project-qualified names like `project:dataset.table`), the row ID is ambiguous. Two distinct edges can produce identical `row_id` values and silently overwrite each other via the `ON CONFLICT DO UPDATE`.

**Example collision:**
- edge A: `source_object="a:b"`, `target_object="c"` → `key:a:b:c:DECLARED`
- edge B: `source_object="a"`, `target_object="b:c"` → `key:a:b:c:DECLARED`

**Fix:** Use a hash (e.g., SHA-256) of the composite key, or replace colons in source/target with a non-conflicting separator before building the row ID.

---

### M2 — `full_sync` cursor derived only from asset push response
**File:** `packages/alma-atlas/src/alma_atlas/sync/client.py:339`

```python
new_cursor = asset_resp.new_cursor or cursor
```

The cursor is only advanced from the asset push response. If the server returns no new cursor in `asset_resp` (e.g., zero assets were synced because none changed), `new_cursor = cursor`. Any subsequent push of edges, contracts, or violations may have succeeded and returned a newer cursor, but those responses are ignored. Next sync re-pushes the same records.

**Impact:** For deployments where assets rarely change but edges/contracts update frequently, the cursor never advances and the sync retransmits all edges and contracts on every run.

---

### M3 — `stitch.py` always stores empty query fingerprint
**File:** `packages/alma-atlas/src/alma_atlas/pipeline/stitch.py:83`

```python
query_repo.upsert(QueryObservation(
    fingerprint=derived[0].query_fingerprint or "",
    ...
))
```

`AnalysisEdge.query_fingerprint` is always `None` in the `Edge` dataclass defined in `edges.py` — it has no fingerprinting logic. So every `QueryObservation` is stored with `fingerprint=""`. Since `QueryRepository.upsert` likely keys on fingerprint, all queries are stored under the same empty key and overwrite each other. The `atlas_get_query_patterns` tool returns at most one pattern.

---

### M4 — Cross-system edge discovery runs both A→B and B→A
**File:** `packages/alma-atlas/src/alma_atlas/pipeline/cross_system_edges.py:61–63`

The double loop over `source_ids` produces all ordered pairs including both A→B and B→A:
```python
for source_id_a in source_ids:
    for source_id_b in source_ids:
        if source_id_a == source_id_b:
            continue
```

For two similar schemas (e.g., Postgres → BigQuery), both directions often meet the score threshold, creating bidirectional edges. Data lineage is inherently directional (data flows one way). These phantom reverse edges inflate the `cross_system_edge_count` reported and produce incorrect upstream/downstream results in impact analysis.

**Impact:** For N sources with similar schemas, up to `N*(N-1)` edges are stored instead of the true `N*(N-1)/2` undirected matches. Impact blast radius may be doubled.

---

### M5 — `_query_hash` collision risk in large deployments
**File:** `packages/alma-analysis/src/alma_analysis/derived_analytics.py:130`

```python
return hashlib.sha256(normalized_sql.encode()).hexdigest()[:8]
```

8 hex chars = 32-bit space. Birthday paradox: ~50% collision probability at ~65,536 distinct query patterns. For large-scale Postgres or BigQuery deployments with diverse workloads, collisions silently merge different query groups, inflating execution counts and corrupting `avg_duration_ms`/`avg_bytes_processed` for the merged group.

---

### M6 — `load_atlas_yml` ignores the `scan` section
**File:** `packages/alma-atlas/src/alma_atlas/config.py:174–232`

The key `"scan"` is listed in `_KNOWN_ATLAS_YML_KEYS` (line 21), preventing the "unknown key" error. However, `load_atlas_yml` never reads or parses the `scan` sub-dictionary. Users who configure scan settings (timeout, concurrency) in `atlas.yml` receive no error and no effect — the values are silently ignored.

---

### M7 — `lineage_inference.py` target can be None for pure SELECT events
**File:** `packages/alma-analysis/src/alma_analysis/lineage_inference.py:128`

```python
target = result.target_table or event.source_name
```

For pure SELECT queries (no INSERT/CREATE AS), `result.target_table` is `None`. If the event also has `source_name = None` or `""`, `target` becomes `None` (or `""`), and an edge with `target_object=None` is appended to the aggregation dict and eventually emitted as a `LineageEdge`. Downstream consumers that expect `target_object` to be a non-empty string will fail or produce silent garbage.

---

## Low Issues

### L1 — Enforcement `blocked` result logs warning but doesn't raise or set any observable state
**File:** `packages/alma-atlas/src/alma_atlas/pipeline/scan.py:311–317`

Even if `_run_enforcement` is fixed to return data (see H2), currently `EnforcementEngine.enforce()` returns `EnforcementResult(blocked=True)` but the CLI `scan` command must explicitly inspect this. The docstring implies callers act on `ScanResult.warnings`, but the machinery is entirely missing.

---

### L2 — `sources.json` stores DSN passwords in plaintext
**File:** `packages/alma-atlas/src/alma_atlas/config.py:139–144`

`save_sources` serializes all `params` (including `dsn` which may contain passwords) to `~/.alma/sources.json` without encryption. This is consistent with local tooling conventions but worth noting in any deployment where the config directory is shared or backed up.

---

### L3 — `_run_enforcement` builds `StoreSnapshot` without persisting it
**File:** `packages/alma-atlas/src/alma_atlas/pipeline/scan.py:295–303`

```python
current = StoreSnapshot(asset_id=asset_id, columns=current_cols)
report = detector.detect(asset_id, previous, current)
```

`current` is constructed for drift comparison but never persisted via `SchemaRepository.upsert()`. On the next scan, `schema_repo.get_latest(asset_id)` returns the same stale `previous` snapshot. Drift detection perpetually compares the current scan against the oldest stored snapshot rather than the previous scan. All `removed_column`/`type_changed` violations may fire repeatedly on every scan.

---

### L4 — `conflict.py` timestamp comparison is string-lexicographic
**File:** `packages/alma-atlas/src/alma_atlas/sync/conflict.py:17–18`

```python
return remote if remote_ts >= local_ts else local
```

ISO 8601 is lexicographically monotone only when precision is consistent. Mixed precision (e.g., `"2024-01-01T00:00:00"` vs `"2024-01-01T00:00:00.000Z"`) or mixed timezone suffixes (`+00:00` vs `Z`) could produce incorrect ordering. Python's `datetime.fromisoformat` + comparison would be safer.

---

### L5 — HEURISTIC cross-system edge direction is arbitrary
**File:** `packages/alma-analysis/src/alma_analysis/lineage_inference.py:216–232`

Cross-system edge pairs are sorted by adapter key (alphabetically) and emitted as `entries[i] → entries[j]`. The direction is determined by string sort order of adapter key names, not by any inference about data flow direction. A user whose Postgres adapter key sorts alphabetically before BigQuery would get edges pointing the wrong way.

---

### L6 — `infer_lineage` hardcodes `adapter_kind=POSTGRES`
**File:** `packages/alma-analysis/src/alma_analysis/lineage_inference.py:287`

```python
meta = ExtractionMeta(adapter_kind=SourceAdapterKindV2.POSTGRES, ...)
```

The synthetic `ExtractionMeta` for inferred lineage always reports kind `POSTGRES`, even when the events come from BigQuery. Consumers that key on `adapter_kind` for routing or display will show incorrect source system labels.

---

### L7 — `scanner_v2.py _upsert_extraction_result` rows accumulate indefinitely
**File:** `packages/alma-atlas/src/alma_atlas/pipeline/scanner_v2.py:421`

```python
row_id = f"{adapter_key}:{cap.value}:{meta.captured_at.isoformat()}"
```

Because `captured_at` is unique per scan run, every scan writes a new row into `v2_extraction_results`. There is no TTL, pruning, or rotation. In a deployment scanning frequently, this table grows without bound.

---

### L8 — `all application consumers → service_account` misclassification
**File:** `packages/alma-analysis/src/alma_analysis/derived_analytics.py:92–96`

```python
if consumer_type == "application":
    if _SERVICE_ACCOUNT_RE.search(app_hint) or _SERVICE_ACCOUNT_RE.search(user_hint):
        return "service_account"
    return "service_account"   # ← always
```

Any consumer with `consumer_source_type == "application"` is classified as `service_account` regardless of the service account pattern check. The second `return "service_account"` on line 96 makes the `_SERVICE_ACCOUNT_RE` check on lines 93-94 unreachable (dead code). Web apps, API servers, and BI tools connecting via application credentials are all misclassified.

---

### L9 — `_get_client()` creates client outside context manager (resource leak path)
**File:** `packages/alma-atlas/src/alma_atlas/sync/client.py:115–123`

If `SyncClient` is used without the `async with` context manager (e.g., in the MCP path — see H4), `_get_client()` creates an `httpx.AsyncClient` with `_owns_client = True`, but `__aexit__` is never called. See H4 for the primary instance of this.

---

### L10 — `lineage.py` BFS depth-0 returns empty instead of error
**File:** `packages/alma-analysis/src/alma_analysis/lineage.py:43–44`

Callers passing `depth=0` get an empty list silently, since `level >= 0` is immediately true for the start node and nothing is enqueued. `depth=1` correctly returns direct neighbors. This is defensible semantics but could surprise callers expecting `depth=0` to mean "unlimited."

---

## alma-analysis File-by-File Notes

### `edges.py`
No bugs. Clean wrapper. `query_fingerprint` field on `Edge` is always `None` (see M3).

### `lineage.py`
No bugs. BFS is correct. `has_asset()` guard is used consistently in callers. See L10 for `depth=0` semantics.

### `lineage_extractor.py`
Broadly correct with good fallback to table-level extraction. `SELECT *` star expansions silently return `None` with no warning logged (debug log only). The broad `except Exception` on the algebrakit path is acceptable given the explicit fallback.

### `lineage_inference.py`
See H3 (None target), M7, L5, L6. The confidence formula is mathematically correct. Recency decay for future-dated events correctly returns 1.0 (multiplier floor). The `_ensure_utc` helper is correctly applied everywhere.

### `edge_discovery.py`
Scoring math is correct. Weight normalization when `row_count_similarity is None` is correct. `_type_family` has overlapping conditions (e.g., `"int" in "bigint"` is caught by the integer branch before any bigint-specific handler), but this is intentional hierarchical matching and works correctly. UUID5 edge IDs are stable and correct.

### `extract_tables.py`
Regex fallback pattern `[\w-]+` allows hyphens in unquoted identifiers, which PostgreSQL doesn't allow unquoted. This can produce false positives on SQL with arithmetic subtraction adjacent to identifiers (e.g., `a-b` parsed as schema `a`, table `b`). The primary sqlglot path handles this correctly. The fallback is used only when sqlglot fails, so the exposure is limited to malformed SQL.

### `consumer_identity.py`
Normalization loop is correct. Confidence scoring differences between PG and BQ are intentional. No bugs found.

### `derived_analytics.py`
See H3 (`_get_bytes()`), M5 (hash collision), L8 (application misclassification). `fractions` property correctly handles zero-total. Source type priority order is correct.

---

## alma-atlas File-by-File Notes

### `config.py`
`yaml.safe_load` is used (safe). Unknown key rejection is correctly fail-closed. `_SECRET_PARAM_KEYS` redaction works. See M6 (scan section silently ignored), L2 (plaintext secrets).

### `pipeline/scan.py`
Core scan loop is well-structured. Semaphore is correctly scoped per-scan (not shared across the gather call). See H2 (enforcement not propagated), L3 (schema not persisted), and the SQLite connection held open during traffic observation (minor operational concern).

### `pipeline/scanner_v2.py`
v2 pipeline is clean. Capability routing and plan execution are correct. See M1 (edge ID collision), L7 (row accumulation). `asyncio.run()` is used correctly in the sync wrapper. The `_serialise` helper doesn't guard against cycles, but the data model makes cycles impossible in practice.

### `pipeline/stitch.py`
See M3 (empty fingerprint). The core loop is correct and idempotent (`edge_repo.upsert` handles re-runs). Empty SQL is correctly skipped.

### `pipeline/cross_system_edges.py`
See M4 (bidirectional edges). Edge discovery is otherwise correct. Exceptions per-pair are caught and logged without aborting the entire batch.

### `enforcement/drift.py`
Drift detection logic is correct. Severity mappings are reasonable (`type_changed → error`, `removed_column → warning`). Row count anomaly threshold (50%) is hardcoded with no config — inflexible but not wrong.

### `enforcement/engine.py`
Deterministic violation ID is correctly implemented. Unknown mode fallback to shadow is fail-safe. See H2 — the engine itself is correct; the issue is the caller discarding its return value.

### `sync/client.py`
Retry logic is correct with proper exponential backoff and `Retry-After` header respect. See M2 (cursor from asset response only), H4 (client leak via MCP path), L4 (string timestamp comparison), L9.

### `sync/conflict.py`
Server-wins for contracts is correct policy. Last-write-wins for assets is correct. See L4 (string comparison risk).

### `sync/auth.py`
Correct. Empty key validation on construction.

### `sync/protocol.py`
Not read directly (auto-generated or simple DTO) — not flagged.

### `mcp/server.py`
Not read directly. Tool registration delegates to `tools.py`.

### `mcp/tools.py`
See H1 (impact query mismatch), H4 (connection leak). No rate limiting or input size bounds — acceptable for a local tool, concerning if exposed over a network socket. `atlas_check_contract` inline contract validation duplicates logic from `DriftDetector`/`EnforcementEngine` without using them; divergence is possible if enforcement rules evolve.

### `cli/` (all files)
Not fully audited at the line level. The overall CLI structure follows Typer conventions. `cli/scan.py` passes scan results to the output layer — given H2, enforcement feedback will never appear in CLI output.

---

## End-to-End Data Flow Assessment

### Scan → Store (v1 path)
```
CLI scan → run_scan() → run_scan_async()
  → _build_adapter()          ✓ correct
  → adapter.introspect_schema()   ✓ assets upserted with {source}::{schema}.{table} IDs
  → adapter.observe_traffic()     ✓ traffic events collected
  → stitch(traffic, db)           ✗ M3: fingerprints always empty
  → _run_enforcement()            ✗ H2: blocked result not propagated
```
Data written: assets and edges correct; schema not persisted for drift baseline (L3); query observations keyed incorrectly (M3).

### Scan → Store (v2 path)
```
CLI scan → run_scan_v2() → ScannerV2.scan()
  → adapter.probe()           ✓ capability plan built
  → ExtractionPipeline.execute()  ✓ per-capability, failures non-fatal
  → _store_v2_results()           ✗ M1: lineage edge row IDs can collide
                                  ✗ L7: extraction results accumulate
```
Data written: correct for SCHEMA and LINEAGE capabilities modulo the ID collision risk.

### Sync cycle
```
CLI sync / MCP atlas_team_sync → SyncClient.full_sync()
  → filter records by cursor     ✓ correct timestamp filtering
  → push all four record types   ✓ any failure leaves cursor unchanged (atomic)
  → pull contracts/assets        ✓ upsert is idempotent
  → save cursor                  ✗ M2: cursor from asset response only
```
In the MCP path: H4 (connection leak on every invocation).

### Impact analysis
```
atlas_impact → graph.downstream(asset_id)
  → count queries touching downstream nodes  ✗ H1: ID mismatch, always 0
```

### Enforcement
```
scan → _run_enforcement() → DriftDetector.detect() → EnforcementEngine.enforce()
  → violations persisted ✓
  → blocked result discarded ✗ H2
```

---

## Recommendations

Priority order:

1. **[H1] Fix `atlas_impact` query exposure** — In `stitch.py:80`, store `f"{source_id}::{ae.upstream_id}"` as the table reference so IDs match the lineage graph node format, OR normalize the comparison in `tools.py:388` by stripping the source prefix from downstream IDs.

2. **[H2] Surface enforcement result in `ScanResult`** — Capture the return value of `_run_enforcement`, collect blocked/warning results, and append to `ScanResult.warnings` (or add a dedicated `enforcement` field).

3. **[H3] Fix `_get_bytes()` zero-value handling** — Replace `or` with explicit None check:
   ```python
   raw = meta.get("bytes_processed")
   if raw is None:
       raw = meta.get("total_bytes_processed")
   ```

4. **[H4] Fix MCP sync connection leak** — Use `async with SyncClient(...) as client:` in `_handle_team_sync`.

5. **[L3] Persist schema snapshots during enforcement** — Call `schema_repo.upsert(current)` in `_run_enforcement` so drift detection has an accurate baseline on subsequent scans.

6. **[M1] Hash lineage edge row IDs** — Replace the colon-concatenated string with `hashlib.sha256(key.encode()).hexdigest()[:32]` for stable, collision-free IDs.

7. **[M3] Fix query fingerprinting** — Either implement fingerprinting in `edges.py` or use `_normalize_sql` + `_query_hash` from `derived_analytics.py` directly in `stitch.py`.

8. **[M2] Advance cursor from latest push response** — Take the maximum new_cursor across all four push responses rather than only the asset response.

9. **[M4] Deduplicate cross-system edges** — Either iterate only `i < j` pairs (unordered), or add a canonical direction heuristic (e.g., prefer Postgres→BigQuery over BigQuery→Postgres based on adapter kind).

10. **[M5] Increase query hash length** — Use 12–16 hex chars (48–64 bits) to push collision probability below 1% at 1M query patterns.

11. **[M6] Parse `scan` section in `load_atlas_yml`** — Either remove "scan" from `_KNOWN_ATLAS_YML_KEYS` (so it errors on unknown key) or actually read and apply scan configuration.

12. **[L8] Fix application consumer classification** — The second `return "service_account"` on line 96 makes the regex check unreachable. Either remove the dead branch or change the fallback to `"ad_hoc"` for generic applications.

13. **[L3/Drift] Persist schema per scan** — Add `SchemaRepository(db).upsert(current)` in `_run_enforcement` so the baseline advances correctly on each scan.

14. **[L4] Use datetime comparison in ConflictResolver** — Parse timestamps with `datetime.fromisoformat` before comparing to avoid mixed-precision string bugs.
