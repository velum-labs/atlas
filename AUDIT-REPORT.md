# Historical Audit Note

This document is retained for context only.

The repo has since been refactored, several findings here are fixed, and some others changed shape as the runtime moved to the canonical v2-backed scan path. Do not treat this file as the current source of truth; use the live code, tests, and current docs instead.

# Overnight AI Agent Code Audit
**Date:** 2026-03-24
**Auditor:** Claude Sonnet 4.6 (post-session review)
**Scope:** packages/alma-connectors, packages/alma-atlas, packages/alma-analysis

---

## Summary

The core database adapters (BigQuery, Postgres, Snowflake) are structurally sound but the community adapters (Looker, Fivetran, Metabase) contain a **show-stopping async correctness bug** that will block the event loop in any production deployment, and the sync client leaks HTTP connections on every call.

**Severity counts: CRITICAL: 2 | HIGH: 5 | MEDIUM: 6 | LOW: 4**

---

## Critical Issues (blocks shipping)

### CRIT-1 — Looker, Fivetran, Metabase: synchronous `httpx` calls inside `async` methods

**Files:**
- `adapters/looker.py` lines 161, 186, 197 — `httpx.post()`, `httpx.get()` (×2)
- `adapters/fivetran.py` lines 125, 162 — `httpx.get()`
- `adapters/metabase.py` lines 143, 154 — `httpx.post()`, `httpx.get()`

**What happens:** Every public method on these adapters is `async`, but the internal HTTP helpers are synchronous. Python's `asyncio` event loop runs on a single thread; a synchronous `httpx` call inside an `async` function does **not** yield — it blocks the entire event loop until the HTTP response arrives. In a process serving concurrent scan jobs or a FastAPI/Starlette server, a single 2-second Looker API call freezes all other coroutines for 2 seconds.

```python
# looker.py L161–170  — called from async test_connection / probe / extract_*
def _get_access_token(self) -> str:
    ...
    resp = httpx.post(           # ← BLOCKING — freezes event loop
        f"{self._base_url()}/login",
        ...
    )

# fivetran.py L125–132
def _api_get(self, path, ...):
    resp = httpx.get(...)        # ← BLOCKING
    resp.raise_for_status()

# metabase.py L143–150
def _get_auth_headers(self):
    resp = httpx.post(           # ← BLOCKING — called on every request
        f"{self._instance_url}/api/session", ...
    )
```

**Fix required:** Convert `_api_get` (and `_get_access_token` / `_get_auth_headers`) to `async def` and use `await client.get(...)` / `await client.post(...)` with `httpx.AsyncClient`, or wrap synchronous calls in `await asyncio.to_thread(...)`.

---

### CRIT-2 — `SyncClient`: HTTP connection leak on every call

**File:** `packages/alma-atlas/src/alma_atlas/sync/client.py` lines 74–95

```python
def _get_client(self) -> httpx.AsyncClient:
    if self._http_client is not None:
        return self._http_client
    return httpx.AsyncClient(timeout=30)   # ← new client every call, never closed

async def _post(self, path, body):
    client = self._get_client()            # ← gets a brand-new client
    response = await client.post(...)      # ← uses it once
    # client.aclose() is never called      # ← connection pool leaks
```

`httpx.AsyncClient` maintains an internal connection pool and background tasks. Creating one per `_post`/`_get` call (which happens on every `push_assets`, `push_edges`, etc.) leaks socket connections and asyncio resources. Under a full sync cycle (4 push + 2 pull = 6 HTTP calls), this creates 6 leaked clients per sync.

**Fix required:** Either hold `self._http_client` as a persistent client created in `__init__`, or use `async with httpx.AsyncClient() as client:` scoped inside each method.

---

## High Issues (fix before production traffic)

### HIGH-1 — Snowflake `observe_traffic`: `since` parameter silently ignored

**File:** `adapters/snowflake.py` lines 322–350

```python
async def observe_traffic(self, adapter, *, since: datetime | None = None):
    config = self._get_config(adapter)
    lookback_hours = config.lookback_hours   # ← always uses fixed window
    max_rows = config.max_query_rows

    traffic_sql = f"""
    ...
    WHERE START_TIME >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
    ...
    """
    # `since` is never used anywhere in this method
```

The `since` parameter is accepted by the method signature and passed through from `extract_traffic` (L1058), but the SQL always uses the static `lookback_hours` config value. Incremental scans that pass a cursor-derived `since` value will silently re-fetch the full window every time, causing duplicate event processing and unnecessary Snowflake credit consumption.

The same bug exists in `extract_lineage` (lines 1093–1103) where no `since` parameter is even accepted but the same hardcoded window applies.

---

### HIGH-2 — Community adapters unreachable from scan pipeline

**File:** `packages/alma-atlas/src/alma_atlas/pipeline/scan.py` line 375

```python
raise ValueError(
    f"Unknown source kind: {kind!r}. Supported: bigquery, dbt, postgres, snowflake"
)
```

`_build_adapter` handles only `bigquery`, `postgres`, `dbt`, `snowflake`. The Airflow, Looker, Fivetran, and Metabase adapters exist and implement the v2 protocol but have **no wiring** into this function. Any `atlas.yml` entry with `kind: airflow` (etc.) raises a `ValueError` and fails silently as a `ScanResult(error=...)`. These adapters are completely dead code from the pipeline's perspective.

---

### HIGH-3 — `asyncio.run()` called in `run_scan` — crashes in async contexts

**File:** `packages/alma-atlas/src/alma_atlas/pipeline/scan.py` lines 67, 107

```python
snapshot = asyncio.run(adapter.introspect_schema(persisted))   # L67
...
traffic = asyncio.run(adapter.observe_traffic(persisted))       # L107
```

`asyncio.run()` creates and destroys a new event loop. If `run_scan` is called from a context that already has a running event loop (FastAPI route handler, pytest with `asyncio` mode, Celery with async workers, or any `await run_scan(...)` call), this raises:

```
RuntimeError: This event loop is already running
```

This makes `run_scan` impossible to call from any async context without an adapter shim. Given the adapters are all `async`, this is the wrong abstraction layer.

---

### HIGH-4 — Postgres log parser raises `ValueError` on unknown timezone abbreviations

**File:** `adapters/postgres.py` lines 109–136

```python
def _parse_postgres_log_timestamp(raw_value: str) -> datetime:
    ...
    offset = _POSTGRES_LOG_TIMEZONE_OFFSETS.get(timezone_suffix.upper())
    if offset is None:
        raise ValueError(
            f"unsupported postgres log timezone abbreviation: {timezone_suffix}"
        )
```

The TZ table (lines 96–106) covers only 11 abbreviations: `UTC, PDT, PST, MST, MDT, CST, CDT, EST, EDT, CET, CEST`. Any Postgres instance configured with `log_timezone` set to `Asia/Tokyo` (IST, JST), `Australia/Sydney` (AEST, AEDT), `America/Chicago` (CT), `America/Denver` (MT), or dozens of others will cause `_parse_postgres_log_timestamp` to raise. This exception propagates up through `_observe_from_logs` (called at line 447) and crashes the entire traffic observation run — turning a bad log line into a full scan failure.

---

### HIGH-5 — Metabase session token never refreshed on 401

**File:** `adapters/metabase.py` lines 138–160

```python
def _get_auth_headers(self) -> dict[str, str]:
    if self._api_key:
        return {"x-api-key": self._api_key}
    if not self._session_token:          # ← only fetches if token is None
        resp = httpx.post(.../api/session ...)
        self._session_token = resp.json()["id"]
    return {"X-Metabase-Session": self._session_token}

def _api_get(self, path, ...):
    resp = httpx.get(..., headers=self._get_auth_headers(), ...)
    resp.raise_for_status()             # ← raises on 401, no retry
```

Metabase session tokens expire after ~2 weeks by default. Once expired, every `_api_get` call raises an `HTTPStatusError(401)` and propagates to the caller as a scan failure. Unlike `LookerAdapter` (which has a 401-retry loop at lines 192–204), `MetabaseAdapter` has no session refresh on expiry. Long-lived adapter instances will fail silently after the initial session ages out.

---

## Medium Issues (fix soon)

### MED-1 — `full_sync` cursor filtering uses string comparison on timestamps

**File:** `sync/client.py` lines 162–165

```python
assets    = [a for a in all_assets    if (a.last_seen   or "") >= cursor]
edges     = [e for e in all_edges     if (e.last_seen   or "") >= cursor]
contracts = [c for c in all_contracts if (c.updated_at  or "") >= cursor]
violations= [v for v in all_violations if (v.detected_at or "") >= cursor]
```

ISO 8601 strings compare correctly only when they have identical formatting and timezone representation. The cursor comes from `asset_resp.new_cursor` (a server-supplied string), while the local timestamps come from SQLite (format TBD). A mismatch like `"2026-03-24T10:00:00Z"` vs `"2026-03-24T10:00:00+00:00"` will cause all records to be either fully included or fully excluded. This should use `datetime` parsing and comparison.

---

### MED-2 — `full_sync` discards push responses for edges, contracts, violations

**File:** `sync/client.py` lines 177–180

```python
asset_resp = await self.push_assets(...)
await self.push_edges(...)        # ← response discarded
await self.push_contracts(...)    # ← response discarded
await self.push_violations(...)   # ← response discarded
```

The `SyncResponse` contains `rejected` items (server-side validation failures). Only `asset_resp` is captured; rejected edges, contracts, and violations are silently lost. Operators will see no indication that e.g. 50 edges were rejected due to missing upstream assets.

---

### MED-3 — `_run_enforcement` opens a second `Database` connection inside `run_scan`

**File:** `scan.py` lines 71 and 150

```python
with Database(cfg.db_path) as db:           # L71 — outer connection
    ...
    _run_enforcement(snapshot, source.id, cfg)

def _run_enforcement(snapshot, source_id, cfg):
    with Database(cfg.db_path) as db:       # L150 — second connection to same file
        ...
```

SQLite in WAL mode handles concurrent readers, but two write connections to the same database from the same process in the same thread can deadlock or cause `database is locked` errors depending on write patterns. The enforcement engine writes violations; the outer scan writes assets and edges. These should share one connection.

---

### MED-4 — Snowflake SQL uses f-string interpolation (low-risk injection surface)

**File:** `adapters/snowflake.py` lines 332–350 and 1093–1103

```python
traffic_sql = f"""
...
WHERE START_TIME >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
  ...
LIMIT {max_rows}
"""
```

`lookback_hours` and `max_rows` come from `SnowflakeAdapterConfig`, which is deserialized from `atlas.yml`. The Snowflake connector API doesn't support parameterized queries for `DATEADD` arguments or `LIMIT` clauses, so full parameterization isn't possible here — but the values should be validated as integers before interpolation. Currently an attacker who can write `atlas.yml` (or inject values through config deserialization) can execute arbitrary SQL.

Same pattern applies to `extract_lineage` (L1101–1102) and `extract_definitions` uses `db_prefix` (L940, L946–962) without sanitization.

---

### MED-5 — v2 `extract_traffic` / `extract_schema` call deprecated v1 methods, emitting warnings

**Files:**
- `bigquery.py` L1216: `await self.observe_traffic(adapter, since=since)`
- `postgres.py` L909 (extract_schema calls introspect_schema)

These v2 methods are implemented by delegating to the v1 `observe_traffic()` / `introspect_schema()`, which both emit `DeprecationWarning` via `warnings.warn(...)`. Every v2 scan call will spam the process warning log. The deprecation wrapper exists to catch external callers — it should not fire when called from within the same class.

---

### MED-6 — Snowflake lineage column-mapping silently mismatches unequal-length column lists

**File:** `adapters/snowflake.py` line 1184

```python
col_mappings = tuple(zip(source_cols, target_cols, strict=False))
```

`strict=False` (the default) silently truncates the longer list when source and target column counts differ. A query that selects 5 columns into a 3-column target would produce 3 mappings with no indication that 2 were dropped. Use `strict=True` and catch the resulting `ValueError`, or record the mismatch in metadata.

---

## Low Issues (nice to have)

### LOW-1 — `cross_system_edges.py`: O(n²) pairs with no batch limit

**File:** `cross_system_edges.py` lines 58–97

With N sources, this runs N×(N-1) `EdgeDiscoveryEngine` instances. For N=20 sources, that's 380 full schema comparisons. No concurrency limit, no timeout, no way to skip known-unrelated pairs. Not a bug but will be slow and unbounded as sources grow.

---

### LOW-2 — Looker OAuth token stored in mutable instance state (thread-unsafe)

**File:** `adapters/looker.py` lines 135–136, 168–170

```python
self._access_token: str | None = None
self._token_expires_at: float = 0.0
```

If a `LookerAdapter` instance is shared across concurrent coroutines (e.g. two simultaneous `extract_schema` calls), both could simultaneously evaluate `now < self._token_expires_at - 60` as `False`, both call `_get_access_token`, and issue two login requests. The second write wins and the first token is orphaned. This is also a blocking-HTTP bug (CRIT-1) in practice but the state mutation is independently unsafe.

---

### LOW-3 — `scan.py` error message claims only 4 supported source kinds

**File:** `scan.py` L375

```python
raise ValueError(f"Unknown source kind: {kind!r}. Supported: bigquery, dbt, postgres, snowflake")
```

Once HIGH-2 is fixed and community adapters are wired in, this message will be wrong and confusing. Maintain a `_SUPPORTED_KINDS` set to generate the message dynamically.

---

### LOW-4 — Postgres TZ table missing common abbreviations even for US deployments

**File:** `adapters/postgres.py` lines 96–106

`MST` is listed but `MT` and `Mountain` are not. `CST`/`CDT` are listed but `CT` is not. This is a narrow table — Postgres itself recognizes ~450 TZ abbreviations. The table should either be much larger (pull from `pytz`/`zoneinfo`), or the function should fall back to treating the suffix as a numeric offset and log a warning rather than raise.

---

## File-by-File Notes

### `adapters/bigquery.py` (~1413 LOC)
**What's good:** Solid pagination logic, proper credential caching via `functools.lru_cache` on `_credentials`, good use of `_missing_permissions_from_exc` to surface actionable errors, observation cursor is persisted correctly.
**Concerns:**
- L1216: calls deprecated `observe_traffic()` — see MED-5
- `extract_lineage` (L1375) raises `NotImplementedError` — correct for BQ since lineage is inferred from traffic, but `LINEAGE` is **not** in `declared_capabilities` (L761–769) so this is consistent. Good.
- SQL for `extract_definitions` uses `f"{region}.INFORMATION_SCHEMA.VIEWS"` where `region` is built from `project_id` and `config.location` — inputs should be validated to not contain backticks.

### `adapters/postgres.py` (~1219 LOC)
**What's good:** Inode-based log cursor is a clever and correct approach to handle log rotation. The `pg_stat_statements` fallback is well-structured.
**Concerns:**
- L125: `ValueError` on unknown TZ abbreviation — see HIGH-4. This is the most dangerous bug in this file.
- L439: `observe_traffic()` emits DeprecationWarning even when called from `extract_traffic` — see MED-5.
- The `statement_timeout=30000` in `execute_query` (L713–740) is a good practice.

### `adapters/snowflake.py` (~1212 LOC)
**What's good:** Clean `_connect`/`_get_config` pattern, graceful degradation when ACCESS_HISTORY is unavailable (returns empty rather than crashing), good column-level lineage extraction from ACCESS_HISTORY.
**Concerns:**
- L345–349: `since` parameter ignored — see HIGH-1. This is silent data duplication.
- L1093–1103: lineage SQL also uses hardcoded lookback, no `since` parameter at all.
- L1184: column mapping truncation — see MED-6.
- `_connect` creates a new connection on every call with no pooling; Snowflake connections are slow (~2–5s). For extract_definitions this means two sequential connections are opened (one for views, one is reused but the outer `try/finally` closes it).

### `source_adapter_v2.py`
**What's good:** Well-structured protocol with clear separation of capabilities, good use of `Protocol` typing.
**Concerns:** `extract_lineage` and `extract_orchestration` return type is `None` in the stubs that raise `NotImplementedError`, annotated with `# type: ignore[override]`. This is a protocol violation that mypy is being told to ignore — callers who check the return type will get `None` instead of a `LineageSnapshot`.

### `pipeline/scan.py` (~376 LOC)
**What's good:** Clean separation of schema, traffic, and enforcement phases; errors produce `ScanResult` rather than crashing.
**Concerns:**
- L67, L107: double `asyncio.run()` — see HIGH-3.
- L375: community adapters missing — see HIGH-2.
- L150: second DB connection in enforcement — see MED-3.
- The `scan_all` function (if it exists) should pass the `db` connection down rather than each sub-function opening its own.

### `adapters/airflow.py` (~491 LOC)
**What's good:** Correct async `httpx.AsyncClient` usage (unlike Looker/Fivetran/Metabase). Pagination loop has a correct termination condition (`not page`). `test_connection` uses `/api/v1/health` appropriately.
**Concerns:**
- Not wired into `_build_adapter` (HIGH-2).
- `_api_get` creates a new `AsyncClient` per call (lines 155–161) — not a blocking bug like Looker but still wasteful. Should be a session-level client.
- The pagination loop at L180 uses `data.get("total_entries", len(results))` — if the server doesn't return `total_entries`, it defaults to `len(results)` which equals `offset + page_size`, guaranteeing one extra fetch at the end. Harmless but inefficient.

### `adapters/looker.py` (~520 LOC)
**What's good:** OAuth token caching with expiry is the right approach. 401-retry on token expiry is correct.
**Concerns:**
- CRIT-1: all HTTP is synchronous.
- Not wired into `_build_adapter` (HIGH-2).
- Token state is not thread-safe (LOW-2).
- `_base_url()` (L145–146) is a method, not a property, called repeatedly — minor style issue.

### `adapters/fivetran.py` (~408 LOC)
**What's good:** Cursor-based pagination in `_get_all_connectors` is correct for the Fivetran API. Schema extraction from connector metadata is reasonable.
**Concerns:**
- CRIT-1: all HTTP is synchronous.
- Not wired into `_build_adapter` (HIGH-2).
- `probe` (L179–183) swallows all exceptions with a bare `except Exception: available = False` — no logging of why the probe failed makes debugging hard.

### `adapters/metabase.py` (~436 LOC)
**What's good:** Handles both API-key and username/password auth. Correctly checks for enterprise features before reporting TRAFFIC capability.
**Concerns:**
- CRIT-1: all HTTP is synchronous.
- HIGH-5: session token never refreshed.
- Not wired into `_build_adapter` (HIGH-2).
- `_api_get` has no retry on 401 unlike Looker.
- `resp.json()["id"]` at L149 will `KeyError` if the session endpoint returns an unexpected shape (e.g. on wrong credentials returning a 200 with error body). Should use `.get("id")` with a fallback error.

### `pipeline/cross_system_edges.py` (~103 LOC)
**What's good:** Clean, focused, single-responsibility. The `meets_threshold` guard at L79 is correct.
**Concerns:** O(n²) scale concern (LOW-1). No error handling around `engine.discover_edges()` — an exception from one pair aborts all remaining pairs.

### `analysis/lineage.py` (~68 LOC)
**What's good:** Correct NetworkX BFS. Returns a proper list. No obvious bugs.
**Concerns:** NetworkX is a heavy import for simple BFS. No cycle detection — if the lineage graph has a cycle (materialized view referencing itself via a chain), BFS will not loop (NetworkX handles this) but the results will be silently incomplete for nodes on cycles.

### `sync/client.py` (~245 LOC)
**What's good:** Dependency injection of `http_client` is good for testing. `ConflictResolver` strategy pattern is clean.
**Concerns:**
- CRIT-2: connection leak.
- MED-1: string cursor comparison.
- MED-2: push response discarding.
- `full_sync` is not atomic — if `pull_contracts` raises after all pushes succeed, the new cursor is never saved, causing re-push of all records on the next cycle.

### `sync/conflict.py` (~32 LOC)
**What's good:** Simple, predictable last-write-wins + server-wins strategy, easy to reason about.
**Concerns:** No unit of measure on timestamp comparison — assumes both local and remote use the same timestamp format/timezone. Silent behavior on `None` timestamps (field is optional).

---

## Test Coverage Assessment

Tests were not directly audited, but based on the code patterns:

**Almost certainly mock-only (low confidence):**
- Community adapter tests likely assert that `_api_get` returns mocked responses, not that the async/sync contract is correct. The CRIT-1 bug would pass all mock-based tests because mock calls don't block the event loop.
- `SyncClient` tests likely inject a mock `http_client`, bypassing the connection leak entirely (CRIT-2 is invisible in tests).

**Untested scenarios that could break in prod:**
- Postgres log parser with non-US timezones (HIGH-4)
- Snowflake incremental sync with a `since` value (HIGH-1) — would pass tests because `since` is accepted without error
- `asyncio.run()` from within an async caller (HIGH-3)
- Metabase session expiry (HIGH-5)
- `full_sync` cursor race / non-atomic save (MED-1, MED-2)

---

## Recommendations (Top 5 before shipping)

1. **Fix CRIT-1 first.** Convert `LookerAdapter._api_get`, `FivetranAdapter._api_get`, and `MetabaseAdapter._get_auth_headers`/`_api_get` to use `httpx.AsyncClient` with `async def`. This requires making `_get_access_token` async in Looker and `_get_auth_headers` async in Metabase. Every integration test for these adapters should run inside `asyncio.run()` and verify no event loop warnings.

2. **Fix CRIT-2: close the httpx client in SyncClient.** Hold a single `httpx.AsyncClient` as a class-level resource or use `async with` scoping. Add a `aclose()` / `__aenter__`/`__aexit__` to `SyncClient` and call it from wherever `full_sync` is invoked.

3. **Wire community adapters into `_build_adapter` (HIGH-2).** Airflow, Looker, Fivetran, and Metabase adapters are complete implementations with no path to execution. Add `if kind == "airflow"` / `"looker"` / `"fivetran"` / `"metabase"` branches with appropriate config extraction from `source.params`.

4. **Fix Snowflake `since` being ignored (HIGH-1).** In `observe_traffic`, when `since` is not `None`, compute the `lookback_hours` dynamically: `lookback_hours = max(1, int((datetime.now(UTC) - since).total_seconds() / 3600) + 1)` or build the `WHERE` clause using an ISO literal. Otherwise every incremental Snowflake scan re-processes the full history window.

5. **Fix Postgres timezone parsing (HIGH-4).** Replace the `raise ValueError` with a fallback: log a warning and default to UTC rather than crashing the entire log scan. A single unrecognized timezone abbreviation should not abort observation for thousands of other log lines.
