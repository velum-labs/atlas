# Historical Audit Note

This document is retained for context only.

Foundation-layer behavior has changed since this audit. Use the repaired runtime, current tests, and current docs as the source of truth.

# Foundations Audit: alma-ports · alma-sqlkit · alma-atlas-store

**Audited:** 2026-03-24
**Scope:** Every `.py` and `.sql` file under `packages/*/src/`
**Packages:**
- `alma-ports` (18 files)
- `alma-sqlkit` (14 files)
- `alma-atlas-store` (12 files including 3 SQL migrations)

---

## Summary

These three packages are the substrate everything else runs on. Overall code quality is high—clean protocols, proper sqlglot usage, and consistent parameterized queries throughout. However, there are **3 Critical** bugs that will cause silent data corruption or hard import-time failures, **5 High** bugs including a thread-safety façade and a NOT IN logic inversion, and a cluster of Medium issues mostly in the SQLite migration system and binder scoping. No SQL injection risks were found in the persistence layer.

---

## Critical Issues

### C-1 · `Dialect.ANSI` does not exist — import-time `AttributeError`

**Files:**
- `alma-sqlkit/src/alma_sqlkit/normalize.py:17`
- `alma-sqlkit/src/alma_sqlkit/parse.py:16,23`

Both modules use `Dialect.ANSI` as a default parameter value:

```python
# normalize.py:17
def normalize_sql(sql: str, dialect: Dialect | str = Dialect.ANSI) -> str:

# parse.py:16, 23
def parse_sql(sql: str, dialect: Dialect | str = Dialect.ANSI) -> list[exp.Expression]:
def extract_tables(sql: str, dialect: Dialect | str = Dialect.ANSI) -> list[str]:
```

`Dialect` is a frozen `@dataclass`. The only class-level names defined in `dialect.py` are the classmethods (`postgres`, `duckdb`, `snowflake`, `bigquery`, `from_name`). There is no `ANSI` attribute anywhere on the class. Default parameter values are evaluated at **module import time**, so importing either `alma_sqlkit.normalize` or `alma_sqlkit.parse` immediately raises:

```
AttributeError: type object 'Dialect' has no attribute 'ANSI'
```

These modules are not re-exported from `alma_sqlkit/__init__.py`, so the package import survives—but any consumer that imports `normalize_sql`, `parse_sql`, or `extract_tables` directly will fail at import time.

**Fix:** Change default to a string `"ansi"` (sqlglot recognises it) or `Dialect.postgres()`, whichever matches intended semantics.

---

### C-2 · Migration TOCTOU: crash between `executescript` and `_migrations` insert makes `003_enforcement.sql` permanently non-recoverable

**File:** `alma-atlas-store/src/alma_atlas_store/db.py:42–46`

```python
sql = migration_file.read_text()
self._conn.executescript(sql)                               # DDL committed here
self._conn.execute("INSERT INTO _migrations (name) VALUES (?)", (name,))
self._conn.commit()
```

`sqlite3.executescript()` issues an implicit `COMMIT` before running the script. The DDL is permanently applied to the database at that point. The subsequent `INSERT INTO _migrations` and `commit()` are in a separate implicit transaction. If the process crashes (OOM, SIGKILL, power loss) between the two, the migration is applied but not recorded.

On the next startup `_migrate()` will attempt to re-apply the migration:

- **001/002** (`CREATE TABLE IF NOT EXISTS`): benign—idempotent.
- **003_enforcement.sql** (`ALTER TABLE contracts ADD COLUMN mode`): **fatal**—SQLite has no `ALTER TABLE ADD COLUMN IF NOT EXISTS`. This raises `OperationalError: duplicate column name: mode`, making the database permanently unbootable without manual intervention.

**Fix:** Wrap the entire migration (script + _migrations insert) in an explicit `BEGIN`/`COMMIT` using `executemany` or `execute` with a transaction, not `executescript`. Alternatively, make individual DDL statements idempotent where feasible, and check column existence before `ALTER TABLE`.

---

### C-3 · `NOT IN` predicate silently inverted to `IN` in SQLBinder

**File:** `alma-sqlkit/src/alma_sqlkit/binder/sql_binder.py:534–549`

```python
if isinstance(expr, exp.In):
    bound_expr = self._bind_expression(expr.this, scope)
    values = []
    for v in expr.expressions:
        values.append(self._bind_expression(v, scope))
    ...
    return BoundIn(
        expression=bound_expr,
        values=values,
        negated=False,          # ← always False
        upstream_columns=upstream,
    )
```

sqlglot represents `x NOT IN (1, 2, 3)` as `exp.In` with `expr.args.get("not") == True`. The binder never reads this flag, so `NOT IN` is bound as `IN`. Any query using `NOT IN` will silently produce the logically opposite filter without error.

**Fix:**
```python
return BoundIn(
    expression=bound_expr,
    values=values,
    negated=bool(expr.args.get("not", False)),
    upstream_columns=upstream,
)
```

---

## High Issues

### H-1 · `ConnectionError` and `TimeoutError` shadow Python builtins

**File:** `alma-ports/src/alma_ports/errors.py:16,24`

```python
class ConnectionError(AtlasError): ...   # shadows builtins.ConnectionError
class TimeoutError(AtlasError):    ...   # shadows builtins.TimeoutError
```

Both names are in `__all__` and re-exported. In any module that does `from alma_ports.errors import *` (or uses star imports transitively), bare `except ConnectionError:` or `except TimeoutError:` will silently catch the Atlas class instead of the Python built-in—or vice versa depending on which import wins. The OS/networking layer routinely raises the built-in `ConnectionError`; swallowing it as an Atlas error is a subtle correctness bug.

**Fix:** Rename to `AdapterConnectionError` / `AdapterTimeoutError` (or add a module prefix). The `__all__` export names must also change.

---

### H-2 · `check_same_thread=False` with no mutex — thread-safety illusion

**File:** `alma-atlas-store/src/alma_atlas_store/db.py:26`

```python
self._conn = sqlite3.connect(str(path), check_same_thread=False)
```

`check_same_thread=False` suppresses the sqlite3 module's single-thread assertion, but it provides **zero actual serialisation**. SQLite in WAL mode supports concurrent readers but only one writer at a time; the Python `sqlite3` module's `Connection` object itself is not thread-safe—concurrent calls can corrupt the connection state. Any caller that passes a single `Database` to multiple threads (e.g., background sync + HTTP handler) will experience intermittent `ProgrammingError` or silent data corruption.

**Fix:** Either (a) add a `threading.Lock` around all `self._conn` access, (b) use `connection_per_thread` pattern (one connection per thread via `threading.local`), or (c) explicitly document that `Database` is not thread-safe and each thread must own its own instance.

---

### H-3 · `_emit_with` uses inconsistent sqlglot attribute names

**File:** `alma-sqlkit/src/alma_sqlkit/emitter.py:519,528`

```python
# Line 519 — main path
main_ast.set("with_", with_clause)

# Line 528 — fallback path (non-SELECT main query)
select.set("with", with_clause)
```

In sqlglot, `exp.Select.args` uses the key `"with"` (not `"with_"`). The `with_` suffix is only used as a Python *property* accessor to avoid the reserved keyword—`set()` takes the raw `arg_types` key, which is `"with"`. The main path (line 519) silently sets a non-existent key, so CTEs are dropped from the emitted SQL for any `WithExpression` whose main query is a `SELECT`. The fallback (line 528) is correct.

**Fix:** Change line 519 from `set("with_", ...)` to `set("with", ...)`.

---

### H-4 · `IN (SELECT …)` subquery silently replaced with a string placeholder

**File:** `alma-sqlkit/src/alma_sqlkit/parser/sql_parser.py:397–402`

```python
if subquery_expr is not None:
    # IN (SELECT ...) - subquery case
    right = Literal(value="__subquery__", data_type="subquery")
    op = ComparisonOp.NOT_IN if is_negated else ComparisonOp.IN
    return AtomicPredicate(left=left, op=op, right=right)
```

Any query containing `col IN (SELECT …)` or `col NOT IN (SELECT …)` is silently converted to a predicate that compares `col` against the literal string `"__subquery__"`. Downstream consumers get a structurally valid `AtomicPredicate` with meaningless content. There is no warning, no log, and no exception. The comment says "full subquery support is complex" but the workaround produces wrong results rather than a parse error.

**Fix:** Raise `ValueError("IN (SELECT …) subqueries are not yet supported")` so callers know to handle this case explicitly instead of receiving corrupted data.

---

### H-5 · N+1 query in `ConsumerRepository._row_to_consumer`

**File:** `alma-atlas-store/src/alma_atlas_store/consumer_repository.py:87–90`

```python
def _row_to_consumer(self, row: sqlite3.Row) -> Consumer:
    asset_rows = self._db.conn.execute(
        "SELECT asset_id FROM consumer_assets WHERE consumer_id = ?", (row["id"],)
    ).fetchall()
```

`_row_to_consumer` issues a separate SQL query per consumer. `list_all()` therefore executes `1 + N` queries for N consumers. `list_for_asset()` has the same problem. For a system with 500 consumers this is 501 round-trips.

**Fix:** Join `consumer_assets` directly in the main query and aggregate `asset_id` values, or load all associations in a single bulk query and join in Python:

```sql
SELECT c.*, GROUP_CONCAT(ca.asset_id) as asset_ids
FROM consumers c
LEFT JOIN consumer_assets ca ON ca.consumer_id = c.id
GROUP BY c.id
```

---

## Medium Issues

### M-1 · `ConnectionT = Any` defeats type-checking across the proposal protocol

**File:** `alma-ports/src/alma_ports/connection.py:12`

```python
ConnectionT = Any
```

This type alias is used in `proposal.py` as the parameter type for `get_proposal_for_update`, `update_proposal_in_tx`, and `execute_sql_in_tx`. Every concrete implementation of `ProposalTransactor` accepts `Any` for the connection argument with no type enforcement. The alias provides no more safety than writing `Any` directly, but looks like a typed abstraction.

**Fix:** Use `TypeVar` or a proper `Protocol` with the actual connection interface (e.g., `psycopg.AsyncConnection`, `asyncpg.Connection`) so callers and implementations are type-checked.

---

### M-2 · `@contextmanager` on a Protocol method stub creates a real generator

**File:** `alma-ports/src/alma_ports/connection.py:18–21`

```python
@runtime_checkable
class ConnectionProvider(Protocol):
    @contextmanager
    def get_connection(self) -> ConnectionT:
        ...
```

Protocol method bodies are stubs and should not carry real decorators that produce behaviour. The `@contextmanager` decorator transforms `get_connection` into a generator function that yields `None` (the `...` body). If anyone were to call `ConnectionProvider().get_connection()` (possible since Protocol classes are instantiable), they'd get a context manager that yields `None`. More practically, `runtime_checkable` checks only for attribute presence—not signature—so this doesn't affect isinstance checks, but the decorator adds confusion and could surprise a reader trying to understand the interface.

**Fix:** Remove `@contextmanager` from the Protocol stub. Document the expected semantics with a type comment only.

---

### M-3 · Condition parse failure silently emits `WHERE 'bad condition'`

**File:** `alma-sqlkit/src/alma_sqlkit/builder.py:280–283`

```python
except Exception:
    parsed_conditions.append(exp.Literal.string(condition))
```

When `SQLBuilder.where("some condition")` calls `_parse_conditions`, a parse failure causes the condition to be wrapped as a string literal. The resulting SQL becomes `WHERE 'some condition'`—a truthy string constant that is always `TRUE` in Postgres. The query silently returns all rows instead of raising an error.

**Fix:** Re-raise the exception or raise `ValueError(f"Cannot parse WHERE condition: {condition!r}")`.

---

### M-4 · `join_type` string accepted without validation

**File:** `alma-sqlkit/src/alma_sqlkit/builder.py:108`

```python
def join(self, table: str, on: str, alias: str | None = None,
         join_type: str = "INNER") -> Self:
```

Any string is accepted. `.join("t", on="...", join_type="BANANA")` silently stores `"BANANA"` in the `JoinSpec`, which then gets passed to sqlglot's `exp.Join(kind="BANANA")`. Depending on dialect, sqlglot may emit it verbatim or silently drop it.

**Fix:** Validate against `{"INNER", "LEFT", "RIGHT", "FULL", "CROSS"}` and raise `ValueError` for unknown values.

---

### M-5 · `db.py:20` — both branches of the ternary are identical

**File:** `alma-atlas-store/src/alma_atlas_store/db.py:20`

```python
self.path = Path(path) if path != ":memory:" else Path(path)
```

Both the `if` and `else` branches evaluate to `Path(path)`. The intent was presumably `None` (or some sentinel) for in-memory databases. As written, `self.path` is always a `Path` object, so checking `self.path` to detect in-memory mode elsewhere would silently fail.

**Fix:** `self.path = Path(path) if path != ":memory:" else None`

---

### M-6 · Invalid contract `mode` silently becomes `"shadow"` — hides bad data

**File:** `alma-atlas-store/src/alma_atlas_store/contract_repository.py:80`

```python
mode=row["mode"] if row["mode"] in ("shadow", "warn", "enforce") else "shadow",
```

An unrecognised `mode` value in the database (e.g., from a future migration or a manual edit) is silently coerced to `"shadow"`. This hides schema drift and data integrity problems. The contract appears valid to the caller while carrying the wrong enforcement mode.

**Fix:** Raise `ValueError(f"Unknown contract mode: {row['mode']!r}")` or use `typing.cast` with an assertion.

---

### M-7 · `list_for_asset` LIKE pattern breaks for asset IDs containing `%` or `_`

**File:** `alma-atlas-store/src/alma_atlas_store/query_repository.py:47–50`

```python
rows = self._db.conn.execute(
    "SELECT * FROM queries WHERE tables LIKE ?",
    (f'%"{asset_id}"%',),
).fetchall()
return [self._row_to_query(r) for r in rows if asset_id in json.loads(r["tables"])]
```

The LIKE pattern uses `asset_id` directly without escaping SQLite LIKE metacharacters (`%`, `_`). An asset ID like `project_name` matches any single character where `_` appears. The Python-side JSON filter compensates for false positives, but false positives still hit the database and add unnecessary load.

**Fix:** Escape LIKE wildcards: `asset_id.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")` with `ESCAPE '\\'`, or use `INSTR` / JSON extraction instead.

---

### M-8 · CTE column schemas not propagated into scope

**File:** `alma-sqlkit/src/alma_sqlkit/binder/sql_binder.py:238–250`

```python
def _bind_ctes(self, with_clause: exp.With, scope: Scope) -> None:
    for cte in with_clause.expressions:
        ...
        self._bind_select(cte_query)   # return value discarded
        cte_instance = RelationInstance(
            table_id=generate_cte_id(cte_name),
            alias=cte_name,
            is_cte=True,
        )
        scope.add_cte(cte_name, cte_instance)
```

`_bind_select(cte_query)` returns a `BoundQuery` whose column schema is discarded. The `RelationInstance` created for the CTE has no `schema` attribute. When the main query references a CTE column (e.g., `SELECT cte.some_col FROM cte`), `resolve()` will find the CTE relation but fail to resolve `some_col` against it, raising `UnresolvedColumnError`.

**Fix:** Pass the `BoundQuery`'s output columns to the `RelationInstance` schema so CTE columns are resolvable.

---

### M-9 · Subquery child scope discarded; inner binding uses parent scope

**File:** `alma-sqlkit/src/alma_sqlkit/binder/sql_binder.py:280–296`

```python
elif isinstance(table_expr, exp.Subquery):
    alias = ...
    inner_select = table_expr.this
    if isinstance(inner_select, exp.Select):
        scope.create_child_scope()       # return value discarded
        self._bind_select(inner_select)  # uses self._current_scope (parent!)
```

`create_child_scope()` returns the child scope but it is not stored. `_bind_select` overwrites `self._current_scope` with a new empty `Scope()` (line 148), not the child scope. The subquery is bound against a fresh top-level scope, not a proper child scope. Correlated column references from the outer query cannot be resolved inside the subquery, and any columns defined in the subquery leak into the parent scope.

**Fix:** Capture the child scope and pass it to `_bind_select`, or refactor `_bind_select` to accept an optional pre-existing scope.

---

### M-10 · `Dialect.from_name` silent fallback for MySQL and SQLite

**File:** `alma-sqlkit/src/alma_sqlkit/dialect.py:77–84`

```python
factories = {
    "postgres": cls.postgres,
    "postgresql": cls.postgres,
    "duckdb": cls.duckdb,
    "snowflake": cls.snowflake,
    "bigquery": cls.bigquery,
}
factory = factories.get(name.lower())
if factory:
    return factory(pretty=pretty)
# Default dialect for unknown names
return cls(name=name, pretty=pretty)
```

`DialectName` includes `MYSQL` and `SQLITE` but neither appears in the factory map. Callers using `Dialect.from_name("mysql")` or `Dialect.from_name("sqlite")` silently receive a `Dialect` with `identifier_quote='"'` (PostgreSQL-style double quotes), while MySQL uses backticks and SQLite uses either. This produces incorrectly-quoted SQL without any warning.

**Fix:** Add mysql/sqlite factories or raise `ValueError` for unrecognised names instead of silently falling back.

---

## Low Issues

### L-1 · `get_shadow_results` default `limit=1000` could return massive datasets silently

**File:** `alma-ports/src/alma_ports/contract.py:41`

```python
def get_shadow_results(
    self, contract_id: str | UUID, *, limit: int = 1000, since: datetime | None = None
) -> list[dict[str, Any]]: ...
```

The default limit of 1000 is far higher than other protocol methods (typically 100–200). A caller that omits `limit` on a busy contract could silently materialise a large in-memory list.

**Fix:** Lower default to `200` and document that callers are expected to paginate.

---

### L-2 · `AssetRepository.search` LIKE metacharacters not escaped

**File:** `alma-atlas-store/src/alma_atlas_store/asset_repository.py:64`

```python
pattern = f"%{query}%"
rows = self._db.conn.execute(
    "SELECT * FROM assets WHERE id LIKE ? OR name LIKE ? OR description LIKE ? ORDER BY id",
    (pattern, pattern, pattern),
).fetchall()
```

User-supplied `query` strings containing `%` or `_` silently alter match semantics. This is a data integrity / surprising-results issue, not an injection issue (parameterised query prevents injection).

**Fix:** Escape wildcards or use SQLite FTS5 for text search.

---

### L-3 · `ViolationRepository.list_recent` always filters to unresolved — misleading name

**File:** `alma-atlas-store/src/alma_atlas_store/violation_repository.py:37–41`

```python
def list_recent(self, limit: int = 50) -> list[Violation]:
    """Return the most recent unresolved violations across all assets."""
    rows = self._db.conn.execute(
        "SELECT * FROM violations WHERE resolved_at IS NULL ORDER BY detected_at DESC LIMIT ?",
```

`list_for_asset` has an `include_resolved` parameter; `list_recent` does not. Callers who want "the 50 most recent violations regardless of resolution state" (a common monitoring query) have no way to get this from the API. The name "recent" implies recency, not status filter.

**Fix:** Add `include_resolved: bool = False` parameter to `list_recent` to match `list_for_asset` API.

---

### L-4 · `SQLEmitter.emit` typed as `Any` — no static protection

**File:** `alma-sqlkit/src/alma_sqlkit/emitter.py:125`

```python
def emit(self, expr: Any) -> str:
```

The public API accepts any object and dispatches via duck-typing and class-name string matching. Static type checkers cannot warn when callers pass the wrong type. The return `str` is also not guaranteed to be valid SQL if dispatch fails (it would raise `ValueError` at runtime instead).

**Fix:** Type as `def emit(self, expr: RAExpression) -> str:`.

---

### L-5 · `ProposalTransactor.execute_sql_in_tx` accepts arbitrary SQL with an `allow_unsafe` bypass flag

**File:** `alma-ports/src/alma_ports/proposal.py:44`

```python
def execute_sql_in_tx(
    self, sql: str, conn: ConnectionT, *, allow_unsafe: bool = False
) -> None: ...
```

The protocol itself is fine (it's just a port), but the `allow_unsafe` flag signals that implementations are expected to execute arbitrary SQL strings. If `sql` is ever sourced from user input (e.g., `migration_sql` from a proposal object that users create), this is an injection vector. The flag name suggests it's intended for trusted contexts, but the protocol doesn't enforce any validation.

**Note for review:** Audit all callers of `execute_sql_in_tx` to verify `sql` is always system-generated, not user-controlled.

---

## File-by-File Notes

### alma-ports

| File | Notes |
|---|---|
| `__init__.py` | Empty aside from version. Fine. |
| `connection.py` | `ConnectionT = Any` (M-1). `@contextmanager` on stub (M-2). |
| `errors.py` | `ConnectionError`/`TimeoutError` shadow builtins (H-1). |
| `edge.py` | Clean. `updates: dict[str, Any]` in `update_*_fields` methods are footguns but acceptable given dict-based storage pattern. |
| `query.py` | `get_query_event` accepts `str | UUID` while some other methods use only `str` — minor inconsistency. |
| `asset.py` | `list_asset_physical_names_batch` takes `list[str]` positionally (not keyword-only) — inconsistent with sibling methods. |
| `contract.py` | L-1 (shadow result limit). |
| `proposal.py` | L-5 (unsafe SQL execution flag). |
| `consumer.py` | `list_consumers` and `list_consumer_dependencies` have positional `target_id` / `limit` args instead of keyword-only; inconsistent with rest of API. |
| `cluster.py` | `insert_cluster` takes `query_ids: list[str]` by position—should be keyword-only. |
| `sql_safety.py` | Correct. `quote_bq_identifier` backtick escaping matches BigQuery docs. |
| All others | Clean. |

### alma-sqlkit

| File | Notes |
|---|---|
| `__init__.py` | Re-exports look correct. |
| `dialect.py` | M-10 (mysql/sqlite fallback). |
| `normalize.py` | **C-1** (`Dialect.ANSI` missing). |
| `parse.py` | **C-1** (`Dialect.ANSI` missing). Also: `normalize_sql` in `sql_parser.py` catches `(ParseError, Exception)` — `ParseError` is already a subclass of `Exception`; the redundant clause is harmless but noisy. |
| `builder.py` | M-3 (silent condition fallback), M-4 (unvalidated join_type). `copy()` works correctly. |
| `emitter.py` | **H-3** (`_emit_with` key inconsistency). L-4 (`Any` type). `_frame_bound_to_ast` returns raw strings used as sqlglot frame bounds—this may work because sqlglot accepts strings for `start`/`end` on `WindowSpec`, but it is fragile and dialect-specific. `_binary_expr_to_ast` fallback uses f-string SQL interpolation (line 609); safe because both operands came through sqlglot AST first, but still worth noting. |
| `parser/sql_parser.py` | **H-4** (`IN (SELECT …)` placeholder). C-3 context: binder issue, not parser. `_convert_table_expr` raises `ValueError` for `VALUES` clauses, `TABLESAMPLE`, function-in-FROM — acceptable limitation but should be documented. |
| `binder/sql_binder.py` | **C-3** (NOT IN). M-8 (CTE schemas), M-9 (subquery scope). |
| `extensions/window.py` | `WindowSpec.order_by` typed as `list[tuple[Expression, str]]` while `sql_parser.py` stores `list[tuple[Expression, SortDirection]]` — minor type inconsistency. |
| `extensions/cte.py` | `CTEDefinition.is_recursive` vs `alma_algebrakit.CTEDefinition.recursive` — these are separate classes at separate layers; no conflict. `WithClause.referenced_tables()` correctly removes CTE names. |
| `extensions/postgres.py` | `ExtendedJoin.type` is `Literal["join"]` matching the base `Join.type`. The duplicate annotation is harmless but redundant. |

### alma-atlas-store

| File | Notes |
|---|---|
| `db.py` | **C-2** (migration TOCTOU). **H-2** (thread safety). M-5 (path ternary). No index hint or `PRAGMA synchronous` setting—defaults are fine for WAL but worth noting. |
| `asset_repository.py` | L-2 (LIKE metacharacters). `upsert` calls `commit()` per operation — no batch write path. |
| `query_repository.py` | M-7 (LIKE on JSON). |
| `contract_repository.py` | M-6 (silent mode fallback). |
| `consumer_repository.py` | **H-5** (N+1 query). |
| `schema_repository.py` | `SchemaSnapshot.id` property uses `:` as separator — if `asset_id` ever contains `:`, the ID is non-round-trippable. Fingerprint is 16 hex chars so it's safe. `ColumnInfo(**c)` in `_row_to_snapshot` will raise `TypeError` if the stored JSON has extra or missing keys from a future schema change. |
| `edge_repository.py` | Clean. `upsert` uses `ON CONFLICT(upstream_id, downstream_id, kind)` matching the UNIQUE constraint in migration 001. |
| `violation_repository.py` | L-3 (`list_recent` naming). |
| `migrations/001_initial.sql` | `edges` table has `REFERENCES assets(id) ON DELETE CASCADE` for both sides, but `edge_repository.py` doesn't enforce that edges only reference registered assets at the Python layer — FK enforcement via `PRAGMA foreign_keys=ON` will catch this at runtime. |
| `migrations/002_v2_extractions.sql` | Clean. Both tables use `IF NOT EXISTS`. |
| `migrations/003_enforcement.sql` | `ALTER TABLE contracts ADD COLUMN mode` is not idempotent — feeds into **C-2**. `violations` table has no FK to `assets` (inconsistent with other tables), though this may be intentional for violations referencing ephemeral objects. |

---

## Recommendations

1. **Fix C-1 immediately** — `normalize.py` and `parse.py` cannot be imported. If they are ever added to `__init__.py` or used by tests, the entire package will fail to import.

2. **Fix C-2 with a transaction wrapper** — The simplest fix is to execute each migration file inside a single SQLite transaction using `BEGIN`/`COMMIT`/`ROLLBACK` via explicit `execute()` calls (not `executescript()`). Alternatively add `IF NOT EXISTS` guards to DDL in 003.

3. **Fix C-3 one-liner** — Change `negated=False` to `negated=bool(expr.args.get("not", False))` in `sql_binder.py:546`.

4. **Fix H-1 before any broad `except ConnectionError:` pattern spreads** — The shadow will become harder to eliminate as more code is written.

5. **Fix H-3 one-character change** — `set("with_", ...)` → `set("with", ...)` in `emitter.py:519`. CTE emission is currently broken for the common case.

6. **Address H-4 with an explicit error** — Replacing silent placeholder with a `ValueError` is a one-line change that prevents subtle data bugs downstream.

7. **Address H-2 (thread safety) with documentation at minimum** — If `Database` is single-threaded by design, document it clearly in the class docstring and add an assertion. If multi-threaded use is intended, add a `threading.Lock`.
