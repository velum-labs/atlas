# Historical Audit Note

This document is retained for context only.

Connector/runtime packaging and scan orchestration have changed since this audit. Use the repaired runtime, current tests, and current docs as the source of truth.

# Deep Audit: alma-connectors
**Date:** 2026-03-24
**Auditor:** Claude Sonnet 4.6 — second-pass, post-hardening
**Scope:** `packages/alma-connectors/src/` (all 16 files, 10,177 LOC) + `packages/alma-connectors/tests/` (17 test files) + `packages/alma-ports/src/alma_ports/sql_safety.py`
**Previous audit:** `AUDIT-REPORT.md` (2026-03-24, same session)

---

## Summary

The hardening commits have resolved the three most dangerous issues from the first audit:
CRIT-1 (blocking HTTP in async community adapters), HIGH-1 (Snowflake `since` ignored), and
HIGH-4 (Postgres timezone parse crash). The async adapter code is now structurally sound.

However, a **new HIGH-severity issue** was found during this deeper pass:
`SourceAdapterService` silently omits `SnowflakeAdapter` and `DbtAdapter` from its internal
registry — making those two adapter kinds completely inaccessible through the service layer.

A second high-severity issue was also found: `apply_probe_routing_override()` in
`source_adapter.py` will crash at runtime when called on a dbt adapter because `DbtAdapterConfig`
has no `probe_target` field.

Additionally, the Snowflake and BigQuery retry helpers are synchronous (`time.sleep`), meaning
every retry delay blocks the entire event loop — a performance correctness bug that degrades
concurrent scan throughput.

**Severity counts (net, this audit): HIGH: 2 | MEDIUM: 7 | LOW: 6**

---

## Critical Issues

None. CRIT-1 (blocking HTTP) and CRIT-2 (SyncClient leak, out of this package) are addressed or
already filed. No new critical issues found.

---

## High Issues

### HIGH-C1 — `SourceAdapterService` registry omits Snowflake and dbt

**File:** `source_adapter_service.py` lines 65–72

```python
self._registry: dict[SourceAdapterKind, SourceAdapter] = {
    SourceAdapterKind.POSTGRES: PostgresAdapter(resolve_secret=self.resolve_secret),
    SourceAdapterKind.BIGQUERY: BigQueryAdapter(resolve_secret=self.resolve_secret),
    # ← SnowflakeAdapter and DbtAdapter are ABSENT
}
```

Every service method that dispatches through `self._registry[adapter.kind]` — `get_capabilities`,
`get_setup_instructions`, `test_connection`, `introspect_schema`, `observe_traffic`,
`execute_query` — raises an unhandled `KeyError` for Snowflake and dbt adapters. This makes
those two adapter kinds completely unusable through `SourceAdapterService`.

Compounding this, `serialize_definition` (lines 107–156) and `row_to_adapter` (lines 158–266)
both use an implicit else-is-BigQuery fallback:

```python
if definition.kind == SourceAdapterKind.POSTGRES:
    ...
else:
    # Any non-Postgres kind (including SNOWFLAKE, DBT) falls here
    adapter_config = BigQueryAdapterConfig(...)  # ← wrong fields for Snowflake
```

A `SnowflakeAdapterConfig` deserialized through `row_to_adapter` will be silently reconstructed
as a `BigQueryAdapterConfig` with nonsense field values. Similarly `serialize_definition` for a
Snowflake adapter persists BigQuery field names (e.g., `project_id`, `max_job_rows`).

**Impact:** Snowflake and dbt adapters are dead code from the service layer's perspective.
Any database row with `kind='snowflake'` or `kind='dbt'` will be deserialized incorrectly.

**Fix:** Register `SnowflakeAdapter` and `DbtAdapter` in `__init__`, and add explicit branches
for `SourceAdapterKind.SNOWFLAKE` and `SourceAdapterKind.DBT` in `serialize_definition` and
`row_to_adapter`.

---

### HIGH-C2 — `apply_probe_routing_override` crashes on dbt adapters

**File:** `source_adapter.py` lines 390–396

```python
# Non-Postgres path
updated_config = replace(
    adapter.config,
    probe_target=(override.probe_target if ... else adapter.config.probe_target),
    #                                                   ^^^^^^^^^^^^^^^^^^^^^^^^
    # AttributeError: DbtAdapterConfig has no 'probe_target' field
)
```

`DbtAdapterConfig` has no `probe_target` field. If `apply_probe_routing_override` is called
with a dbt adapter, `adapter.config.probe_target` raises `AttributeError`, and the `replace()`
call raises `TypeError: __init__() got an unexpected keyword argument 'probe_target'`.

`SnowflakeAdapterConfig` does have `probe_target`, so Snowflake is unaffected. Only dbt is broken.

The function signature accepts `SourceAdapterProbeRoutingOverride` whose `kind` field is
`SourceAdapterKind`, which includes `DBT`. There is no guard preventing the dbt case.

**Fix:** Add an explicit check for dbt (which has no meaningful probe routing override) and
raise `ValueError("dbt adapters do not support probe routing overrides")` before attempting
the `replace()`.

---

## Medium Issues

### MED-C1 — Snowflake and BigQuery retry helpers block the event loop

**Files:**
- `adapters/snowflake.py` lines 92–118: `_retry_with_backoff` uses `time.sleep(delay)`
- `adapters/bigquery.py` lines 94–120: same pattern

Both Snowflake and BigQuery adapters expose `async def` protocol methods but internally
call synchronous snowflake-connector and google-cloud-bigquery APIs. Every connection,
query, and retry `sleep` blocks the event loop for the full duration. In a process with
concurrent scan jobs, a 5-second Snowflake connection attempt blocks all other coroutines.

This is not new (the adapters have always used sync SDKs), but the retry logic added during
hardening makes it worse: a retried connection with `max_attempts=3` and `base_delay=2.0`
can block the loop for up to `2 + 4 = 6` additional seconds per retry cycle.

**Fix:** Wrap sync SDK calls in `asyncio.to_thread()` or move the retry logic inside the
thread, so the event loop remains unblocked during connections and sleeps.

---

### MED-C2 — `AirflowAdapter._api_get` creates a new `AsyncClient` per call

**File:** `adapters/airflow.py` lines 155–161

```python
async def _api_get(self, path: str, ...) -> Any:
    url = f"{self._base_url}/api/v1/{path}"
    async with httpx.AsyncClient(
        headers=self._auth_headers(),
        timeout=self._timeout_seconds,
    ) as client:                          # ← new client every request
        resp = await client.get(url, params=params)
```

This is correctly async (unlike the original CRIT-1), but creates and destroys an
`AsyncClient` — with its internal connection pool — on every single API call. For
`extract_traffic` over N DAGs with M runs each: O(1 + N + N×M + N×M×page_tasks) separate
clients are created. Connection pooling benefits (keep-alive, TLS session reuse) are
completely lost.

Contrast with `LookerAdapter`, `FivetranAdapter`, and `MetabaseAdapter`, which all hold
a persistent `self._client: httpx.AsyncClient` instance.

**Fix:** Add `self._client: httpx.AsyncClient | None = None` to `AirflowAdapter.__init__`
and a `_get_client()` helper matching the Looker/Fivetran/Metabase pattern.

---

### MED-C3 — `MetabaseAdapter._get_auth_headers` uses `resp.json()["id"]` without `.get()`

**File:** `adapters/metabase.py` line 161

```python
resp.raise_for_status()
self._session_token = resp.json()["id"]   # ← KeyError on wrong-shaped 200 response
```

Metabase returns `HTTP 200` with a body of `{"errors": {"password": ["did not match stored password"]}}`
when credentials are wrong (this is Metabase-specific behaviour). In this case `["id"]` raises
`KeyError`, which propagates as an uncaught exception — not a clean `ConnectionTestResult(success=False)`.
The caller receives a cryptic `KeyError: 'id'` rather than a useful error message.

**Fix:** Use `resp.json().get("id")` and raise an explicit `ValueError("Metabase session endpoint did not return an 'id' — check credentials")` when `None`.

---

### MED-C4 — `MetabaseAdapter.extract_traffic` silently falls back from enterprise to OSS on ANY exception

**File:** `adapters/metabase.py` lines 386–411

```python
try:
    ...
    ee_response = await self._api_get("ee/audit-app/query_execution", ...)
    enterprise_ok = True
except Exception:   # ← catches network timeouts, auth errors, connection refused
    pass
```

A `httpx.ReadTimeout`, `ConnectionError`, or `asyncio.TimeoutError` during the enterprise
endpoint request silently causes fallback to the OSS activity feed (which provides only the
last ~1,000 events with no pagination). An operator whose enterprise audit endpoint is
temporarily unreachable will see a partial traffic snapshot with no indication that the
authoritative endpoint was skipped.

**Fix:** Catch only `httpx.HTTPStatusError` with `status_code in (403, 404)` for the
"not-a-feature" case. Let network errors propagate.

---

### MED-C5 — Metabase OSS `since` filtering is always client-side against a 1,000-row cap

**File:** `adapters/metabase.py` lines 414–439

```python
activity = await self._api_get("activity", params={"limit": 1000})
...
if since and captured_at < since:
    continue
```

The OSS activity endpoint returns at most 1,000 records and does not accept a `start_date`
filter server-side (unlike the enterprise endpoint). This means:

1. Every incremental scan fetches 1,000 items regardless of cursor.
2. If there are > 1,000 activity records, records older than the 1,000th are silently dropped
   — the `since` filter never sees them.

A high-activity Metabase instance (> 1,000 queries between scans) will silently miss events.

**Fix:** Document this limitation prominently. If the enterprise endpoint is unavailable
and the instance is high-volume, surface a warning in the `TrafficExtractionResult.errors`
tuple rather than silently dropping events.

---

### MED-C6 — `quote_bq_identifier` does not escape backslashes — theoretical injection

**File:** `packages/alma-ports/src/alma_ports/sql_safety.py` lines 11–29

```python
def quote_bq_identifier(value: str) -> str:
    escaped = value.replace("`", "\\`")   # ← only escapes backticks
    return f"`{escaped}`"
```

BigQuery backtick-quoted identifiers must escape both backticks (as `` \` ``) and
backslashes (as `\\`). The current implementation only escapes backticks. An identifier
containing the two-character sequence `\`` (backslash then backtick) would produce:

```
Input:  "foo\`bar"
Output: "`foo\\\`bar`"
```

Which BigQuery parses as the identifier `foo\` (terminated by the unescaped backtick)
followed by `bar`` — a syntax error or, in adversarial cases, SQL injection.

**Practical risk is low:** BigQuery project IDs (`^[a-z][a-z0-9\-]{4,28}[a-z0-9]$`),
dataset IDs, and table IDs cannot contain backslashes, so this vulnerability is
unexploitable with real identifiers from the BigQuery API. However, the function is
a general utility; if called with user-supplied strings that may contain backslashes,
it is unsafe.

**Fix:** `escaped = value.replace("\\", "\\\\").replace("`", "\\`")` — escape backslashes
before backticks.

---

### MED-C7 — Snowflake `extract_traffic` (v2) discards observation cursor

**File:** `adapters/snowflake.py` lines 1226–1229

```python
return TrafficExtractionResult(
    ...
    observation_cursor=None,   # ← cursor is never set
)
```

The v1 `observe_traffic` correctly returns `observation_cursor=None` (Snowflake's
`QUERY_HISTORY` view has a stable `START_TIME` used for incremental scans via the `since`
parameter). However, the v2 `extract_traffic` wrapper does not propagate the cursor from
the v1 result. This means callers using the v2 protocol cannot persist a cursor and must
recompute the `since` window on every scan from adapter-level config (`lookback_hours`)
rather than from a persisted position.

Airflow `extract_traffic` has the same issue — no observation cursor is returned.

---

## Low Issues

### LOW-C1 — Looker, Fivetran, Metabase have no async lifecycle protocol

**Files:** `looker.py` line 156, `fivetran.py` line 129, `metabase.py` line 144

All three adapters expose a `close()` coroutine to close the persistent `httpx.AsyncClient`,
but none implement `__aenter__` / `__aexit__`. If an adapter is garbage-collected without
an explicit `await adapter.close()` call (the common case in tests and one-off scripts),
the underlying `AsyncClient` is leaked. Python's GC runs synchronously and cannot await
coroutines, so the client's internal connection pool and background tasks are never cleaned up.

**Fix:** Implement `async def __aenter__` / `__aexit__` on all three adapters.

---

### LOW-C2 — Looker makes 3× redundant `GET /api/4.0/lookml_models` calls

**File:** `adapters/looker.py` lines 308 (`discover`), 361 (`extract_schema`), 417 (`extract_definitions`), 473 (`extract_lineage`)

All four implemented capabilities independently call `await self._api_get("lookml_models")`.
For a Looker instance with 50 models × 20 explores each, a full extraction cycle makes
3 identical model-inventory calls plus 3 × 50 × 20 = 3,000 explore-detail API calls, where
2 × 50 × 20 = 2,000 of those are redundant because `extract_schema` and `extract_definitions`
fetch the same explore data with `params={"fields": "fields"}`.

---

### LOW-C3 — Fivetran `probe` does not set `message` on failure

**File:** `adapters/fivetran.py` lines 191–195

```python
try:
    await self._api_get("v1/connectors", params={"limit": 1})
    available = True
except Exception:
    available = False   # ← no message set
```

The `CapabilityProbeResult` for all Fivetran capabilities will have `available=False` and
`message=None`. Operators have no way to distinguish "wrong API key" from
"firewall blocked outbound" from "rate limited" without log inspection.

---

### LOW-C4 — `SnowflakeAdapterConfig` excludes `INFORMATION_SCHEMA` by default but `include_schemas` filter is applied AFTER fetching all columns

**File:** `adapters/snowflake.py` lines 326–338

The `columns_sql` query hard-codes `WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')` but
the broader `exclude_schemas` config (which may include user-defined schemas) is applied
in Python post-fetch (lines 392–396). For large Snowflake databases with many schemas,
this means fetching and deserializing all column rows before filtering — potentially
millions of rows when only a handful of schemas are wanted.

---

### LOW-C5 — Airflow `extract_traffic` skips task instances without `start_date`, but not whole runs

**File:** `adapters/airflow.py` lines 319–321

Task instances without `start_date` (queued/scheduled state) are skipped. However, a
DAG run that is partially-started still causes a full page of task-instance API calls.
For an Airflow instance with many large in-progress DAG runs, this can return a large
number of useless API pages.

---

### LOW-C6 — dbt adapter sets v1 `kind = SourceAdapterKind.DBT` but also extends `BaseAdapterV2`

**File:** `adapters/dbt.py` lines 96–110

```python
class DbtAdapter(BaseAdapterV2):
    kind = SourceAdapterKind.DBT           # v1 kind (StrEnum "dbt")
    ...
    declared_capabilities: frozenset[...] # v2 capabilities
```

`BaseAdapterV2` expects `kind` to be `SourceAdapterKindV2`, not `SourceAdapterKind`.
The `_make_meta` call (via `BaseAdapterV2._make_meta`) assigns `adapter_kind=self.kind`
to `ExtractionMeta.adapter_kind`, which is typed as `SourceAdapterKindV2`. At runtime
this works because both are `StrEnum` with the same `"dbt"` value, but mypy/pyright
will flag a type error and it's semantically confusing. `DbtAdapter` should set
`kind = SourceAdapterKindV2.DBT`.

---

## File-by-File Notes

### `source_adapter.py` (767 lines)
**Status:** Correct and well-structured. Frozen dataclasses, good validation.
**Issue:** `apply_probe_routing_override` crashes on dbt — see HIGH-C2.
**Issue:** `SourceAdapterKind` has only 4 members (no community adapters). `SourceAdapterKindV2` in `source_adapter_v2.py` is the v2 canonical enum; the v1 enum is intentionally a subset.

### `source_adapter_v2.py` (674 lines)
**Status:** Excellent. Well-typed Protocol with clear capability enumeration. No issues.

### `source_adapter_service.py` (334 lines)
**Status:** Critically incomplete — see HIGH-C1. Registry missing Snowflake and dbt. The
`serialize_definition` and `row_to_adapter` fall-through to BigQuery for any non-Postgres
kind is a data corruption bug.

### `credentials.py` (26 lines)
**Status:** Correct. Fernet symmetric encryption, no issues.

### `adapters/_base.py` (197 lines)
**Status:** Good. Uniform `_make_meta`, consistent `NotImplementedError` stubs.
`_make_scope` always returns `ExtractionScope.GLOBAL` — subclasses override
`_scope_identifiers()` correctly. No issues.

### `adapters/postgres.py` (~1,219 lines)
**Status:** Significantly improved since hardening.

**Fixed since prior audit:**
- HIGH-4: TZ table expanded to 25+ abbreviations; unknown TZs now log warning and default to UTC instead of crashing.
- Retry helper uses `asyncio.sleep` (not `time.sleep`) — event loop safe.
- `_validate_postgres_dsn` added.

**Remaining concerns:**
- `test_connection` (v1) uses synchronous `psycopg.connect()` (blocking call inside an `async def`). Should use `await psycopg.AsyncConnection.connect()`.
- The v1 deprecation path is correctly guarded — `extract_schema` does not call the deprecated `introspect_schema`.
- Identifier quoting uses `psycopg.sql.Identifier` for schema/table names — confirmed correct.

### `adapters/snowflake.py` (~1,212 lines)
**Status:** Substantially hardened. SQL identifiers now use `quote_sf_identifier()`.

**Fixed since prior audit:**
- HIGH-1: `since` parameter now correctly used (line 437: `hours_since = max(1, int(...) + 1)`).
- `db_prefix` now uses `quote_sf_identifier(database)` — previously an f-string.
- `_validate_sf_account` and `_validate_sf_name` added.

**Remaining concerns:**
- `_retry_with_backoff` uses `time.sleep` — blocks event loop (MED-C1).
- `LIMIT {max_rows}` f-string (line 459): low risk since `max_rows` is config-validated as `int >= 1`, but is still f-string interpolation.
- `hours_since` in `DATEADD(hour, -{hours_since}, ...)` (line 455): computed as `max(1, int(...) + 1)` — safe, but still f-string.
- `extract_traffic` v2 discards observation cursor (MED-C7).
- MED-6 from prior audit (column mapping `strict=False`): status unverified in this pass — the lineage section was not re-read in full; carry forward until confirmed fixed.

### `adapters/bigquery.py` (~1,413 lines)
**Status:** Well-hardened. Input validation added. `quote_bq_identifier` used for identifiers.

**Fixed since prior audit:**
- `_effective_since` now correctly computes the high-water mark for incremental scans.
- `_validate_bq_project_id` and `_validate_bq_location` added.
- `region_prefix` in INFORMATION_SCHEMA queries uses `quote_bq_identifier`.

**Remaining concerns:**
- `_retry_with_backoff` uses `time.sleep` — blocks event loop (MED-C1).
- `quote_bq_identifier` backslash escape omission (MED-C6).

### `adapters/dbt.py` (740 lines)
**Status:** The best-written adapter in the package. Clean dual v1/v2 implementation.

**Concern:** Uses `kind = SourceAdapterKind.DBT` (v1 enum) despite extending `BaseAdapterV2` — type confusion (LOW-C6).

### `adapters/airflow.py` (491 lines)
**Status:** Correct async code. CRIT-1 resolved.

**Remaining concerns:**
- Per-call `AsyncClient` creation (MED-C2).
- No observation cursor in `extract_traffic` (MED-C7).

### `adapters/looker.py` (537 lines)
**Status:** CRIT-1 resolved. Now uses persistent `self._client` and `asyncio.Lock` for token refresh.

**Remaining concerns:**
- No `__aenter__`/`__aexit__` (LOW-C1).
- 3× redundant `lookml_models` calls (LOW-C2).
- `close()` never called automatically.

### `adapters/fivetran.py` (420 lines)
**Status:** CRIT-1 resolved. Now uses persistent `self._client`.

**Remaining concerns:**
- `probe()` missing error message (LOW-C3).
- No `__aenter__`/`__aexit__` (LOW-C1).

### `adapters/metabase.py` (463 lines)
**Status:** CRIT-1 resolved. HIGH-5 (session token refresh) resolved — `_api_get` now
clears `_session_token` and retries once on 401.

**Remaining concerns:**
- `resp.json()["id"]` KeyError on unexpected session response shape (MED-C3).
- Enterprise traffic fallback swallows all exceptions (MED-C4).
- OSS `since` filter works against a 1,000-row ceiling (MED-C5).
- No `__aenter__`/`__aexit__` (LOW-C1).

### `alma_ports/sql_safety.py` (50 lines)
**Status:** Correct for all real-world BigQuery and Snowflake identifiers.

**Issue:** `quote_bq_identifier` does not escape backslashes before backticks (MED-C6).
For actual BigQuery identifiers (which cannot contain `\`), this is safe. The Snowflake
quoting (`""` doubling) is correct per SQL standard.

---

## Test Quality Assessment

### `test_airflow_adapter.py` (~755 lines) — **Good**
Tests constructor validation, auth headers, pagination termination, probe (healthy/unhealthy/
error/subset), test_connection, discover, extract_traffic (with events, with `since`, skipped
tasks, fallback SQL), extract_lineage, extract_orchestration (with upstream derivation, last-run
error tolerance), and NotImplementedError stubs.

**Gaps:**
- No test that a failing DAG-runs paginator propagates the error (error tolerance during fan-out).
- No test for the per-call-client issue — each `_api_get` call in tests uses `patch.object`, bypassing the real `AsyncClient` creation entirely.

### `test_looker_adapter.py` (~752 lines) — **Good**
Tests OAuth token flow, caching, expiry refresh, 401-token-retry path (including correct new-token usage on second call), test_connection, probe, discover (project deduplication, label fallback), extract_schema, extract_definitions (SQL expression inclusion, fallback when no sql), extract_lineage (primary/join edges, empty sql_table_name), and NotImplementedError stubs.

**Gaps:**
- No test for `close()` actually closing the client.
- No test for the concurrent `_get_access_token` lock (would require `asyncio.gather()`).

### `test_fivetran_adapter.py` (~estimated 400+ lines from header) — **Pattern: Good**
Based on visible structure: constructor validation, async `_api_get`, paginated `_get_all_connectors`, test_connection, probe, discover, extract_lineage (enabled/disabled schema/table filtering), extract_orchestration (schedule mapping, last-run datetime), NotImplementedError stubs.

**Gap:** `probe()` failure message not tested (`message=None` on failure).

### `test_metabase_adapter.py` (~estimated 400+ lines from header) — **Pattern: Good**
Tests API-key and username/password auth paths, `_get_auth_headers`, `_api_get` 401-retry, discover, extract_schema, extract_traffic (enterprise path, OSS fallback, `since` filtering).

**Critical gap:** The 401-retry test in `_api_get` must verify that `_session_token` is correctly cleared AND the retry uses fresh headers. Without this, the fix for HIGH-5 could regress.

### `test_community_stubs.py` (420 lines) — **Smoke only, limited value**
Tests that adapters import and instantiate, capabilities are declared, constructor rejects empty credentials, and implemented methods return non-None values.

**Pattern concern:** Several tests bypass `_get_auth_headers` by pre-seeding `adapter._session_token = "test-session"` (Metabase) or `adapter._access_token = "test-token"` (Looker). This means the actual authentication code path is never exercised by these tests.

### `test_sql_safety.py` (117 lines) — **Good but missing one case**
Tests both quoting functions thoroughly: normal names, hyphens, unicode, emojis, injection attempts (semicolons, `--`, `/* */`), null bytes, newlines, empty-raises.

**Missing:** No test for `quote_bq_identifier` with a backslash-then-backtick input — the
bug in MED-C6 is not caught.

### `test_postgres_adapter.py`, `test_postgres_v2.py`, `test_snowflake_adapter.py`,
`test_snowflake_v2.py`, `test_bigquery_adapter.py`, `test_bigquery_v2.py` — **Not fully read**
These are unit tests for the database adapters that require mocked connections. Based on the
hardening commit messages, edge-case tests were added. Specific gaps:

- **Snowflake timezone parsing:** Postgres TZ fallback (warning + UTC) — verify this is tested.
- **Snowflake `since` non-None:** The hours_since calculation `max(1, int(...) + 1)` should be tested at the boundary.
- **BigQuery `_effective_since` clamping:** Does a test verify that a very old `since` is clamped to 180 days?

### `test_dbt_adapter.py`, `test_dbt_v2.py` — **Assumed adequate** (not re-read)
dbt is file-based and easily unit-tested. The adapter is the most complete in the package.

### `test_v2_types.py`, `test_edge_model.py`, `test_smoke_imports.py` — **Structural only**
These verify type construction and import health, not adapter behavior.

---

## Remaining SQL Injection Surface

The hardening commits significantly reduced the SQL injection surface by replacing bare
f-strings with proper identifier quoting. The remaining surface is:

| Location | Code | Risk |
|---|---|---|
| `snowflake.py:455` | `DATEADD(hour, -{hours_since}, ...)` | **Low** — `hours_since` is `max(1, int(...)+1)` or config-validated int |
| `snowflake.py:459` | `LIMIT {max_rows}` | **Low** — config-validated `int >= 1` |
| `bigquery.py` (region_prefix) | All INFORMATION_SCHEMA queries use `quote_bq_identifier` | **Addressed** |
| `postgres.py:335` | `WHERE {where_clause}` in test_connection | **None** — where_clause is hardcoded string fragments; values are parameterized |
| `sql_safety.py` | `quote_bq_identifier` missing backslash escape | **Theoretical** — not exploitable with real BQ identifiers |

No f-strings inserting unvalidated user-controlled strings were found in the current codebase
after the hardening commits.

---

## Recommendations (Priority Order)

### 1. Fix `SourceAdapterService` registry (HIGH-C1) — immediate

Add `SourceAdapterKind.SNOWFLAKE` and `SourceAdapterKind.DBT` to the registry in
`source_adapter_service.py`. Add explicit branches in `serialize_definition` and
`row_to_adapter`. Without this, the service layer can only serve Postgres and BigQuery.
This is likely a root cause of silent failures if Snowflake or dbt adapters have been
configured but never successfully loaded.

### 2. Fix `apply_probe_routing_override` for dbt (HIGH-C2) — 1-line fix

Guard the non-Postgres branch with a check for `DbtAdapterConfig` and raise `ValueError`.

### 3. Wrap Snowflake/BigQuery sync SDK calls in `asyncio.to_thread` (MED-C1)

The retry helpers and SDK calls are synchronous. Wrap with `asyncio.to_thread()` so the
event loop is not blocked during connection establishment and query execution.

### 4. Fix `AirflowAdapter._api_get` persistent client (MED-C2)

Add `self._client: httpx.AsyncClient | None = None` and `_get_client()` helper. This
matches the pattern already used by Looker, Fivetran, and Metabase.

### 5. Fix `MetabaseAdapter` remaining issues (MED-C3, MED-C4)

- Use `resp.json().get("id")` with an explicit error for missing id.
- Narrow the enterprise endpoint exception catch to `httpx.HTTPStatusError` 403/404 only.

### 6. Add `__aenter__`/`__aexit__` to Looker, Fivetran, Metabase (LOW-C1)

Prevents `httpx.AsyncClient` leaks when adapters are not used as context managers.

### 7. Fix `quote_bq_identifier` backslash handling (MED-C6) — 1-line fix

```python
escaped = value.replace("\\", "\\\\").replace("`", "\\`")
```

Even though unexploitable today, this makes the function safe for all inputs.
