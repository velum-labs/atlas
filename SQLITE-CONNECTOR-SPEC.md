# SQLite Connector Spec

## Overview

A new `alma-atlas connect sqlite` command and `SQLiteAdapter` that enables Atlas to scan SQLite databases — discovering tables, columns, types, foreign keys, row counts, and value distributions.

Primary motivation: BIRD benchmark (11 SQLite databases), but also useful for any SQLite-backed system (local analytics, embedded apps, mobile databases, test fixtures).

## CLI

```bash
# Single database
alma-atlas connect sqlite --path /path/to/db.sqlite

# With a custom source ID
alma-atlas connect sqlite --path /path/to/db.sqlite --id my-db

# Multiple databases in a directory
alma-atlas connect sqlite --dir /path/to/databases/ --glob "*.sqlite"
```

### `connect sqlite` Parameters

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `--path` | yes (or `--dir`) | — | Path to a single `.sqlite` / `.db` file |
| `--dir` | yes (or `--path`) | — | Directory containing multiple SQLite files |
| `--glob` | no | `*.sqlite` | Glob pattern when using `--dir` |
| `--id` | no | `sqlite:{filename_stem}` | Custom source ID |

### SourceConfig

```python
SourceConfig(
    id="sqlite:debit_card_specializing",
    kind="sqlite",
    params={
        "path": "/opt/velum/repos/bird-atlas/data/dev_databases/debit_card_specializing/debit_card_specializing.sqlite",
    },
)
```

For `--dir` mode, one `SourceConfig` per database file.

## Adapter: `SQLiteAdapter`

### Location

`packages/alma-connectors/src/alma_connectors/adapters/sqlite.py`

### Class Hierarchy

```python
class SQLiteAdapter(BaseAdapterV2):
    kind = SourceAdapterKindV2.SQLITE  # new enum value
    declared_capabilities = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.SCHEMA,
    })
```

Extends `BaseAdapterV2`. Implements two capabilities: DISCOVER and SCHEMA. No TRAFFIC, LINEAGE, DEFINITIONS, or ORCHESTRATION (SQLite has no query logs, no orchestration, and definitions/lineage come from `learn`).

### Constructor

```python
def __init__(self, db_path: str | Path) -> None:
    self._db_path = Path(db_path)
```

No secrets, no env vars, no auth. SQLite is a file.

### `probe()`

```python
async def probe(self, adapter: PersistedSourceAdapter) -> tuple[CapabilityProbeResult, ...]:
    """Check that the file exists and is a valid SQLite database."""
    if not self._db_path.exists():
        return self._make_probe_results(
            self.declared_capabilities, available=False,
            scope_ctx=..., message=f"File not found: {self._db_path}"
        )
    # Try opening it
    try:
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("SELECT 1")
        conn.close()
    except sqlite3.Error as e:
        return self._make_probe_results(
            self.declared_capabilities, available=False,
            scope_ctx=..., message=f"Not a valid SQLite database: {e}"
        )
    return self._make_probe_results(
        self.declared_capabilities, available=True, scope_ctx=...
    )
```

### `discover()`

```python
async def discover(self, adapter: PersistedSourceAdapter) -> DiscoverySnapshot:
    """Discover all user tables (excluding sqlite_* system tables)."""
```

Returns a `DiscoverySnapshot` with one `DiscoveredContainer` per table. Each container has:
- `name`: table name
- `kind`: "table" or "view" (from `sqlite_master.type`)

### `extract_schema()`

```python
async def extract_schema(self, adapter: PersistedSourceAdapter) -> SchemaSnapshotV2:
    """Extract full schema: tables, columns, types, FKs, row counts."""
```

For each table (from `sqlite_master WHERE type IN ('table', 'view')`):

1. **Columns** via `PRAGMA table_info("{table}")`:
   ```python
   ColumnSchemaV2(
       name=col_name,
       data_type=col_type or "ANY",  # SQLite has dynamic typing; type may be empty
       nullable=not col_notnull,
       is_primary_key=bool(col_pk),
   )
   ```

2. **Foreign keys** via `PRAGMA foreign_key_list("{table}")`:
   Collected as `ObjectDependencyV2` entries.

3. **Row count** via `SELECT COUNT(*) FROM "{table}"`.

4. **SchemaObject** assembled per table:
   ```python
   SchemaObject(
       schema_name="_default",  # SQLite has no schemas; use sentinel
       object_name=table_name,
       kind=SchemaObjectKindV2.TABLE,  # or VIEW
       columns=[...],
       row_count=count,
   )
   ```

Dependencies from foreign keys:
```python
ObjectDependencyV2(
    source_schema="_default",
    source_object=table_name,         # the table with the FK
    target_schema="_default",
    target_object=referenced_table,   # the table being referenced
)
```

Returns `SchemaSnapshotV2(meta=..., objects=[...], dependencies=[...])`.

### `execute_query()` (optional but useful)

```python
async def execute_query(
    self, adapter: PersistedSourceAdapter, sql: str,
    *, max_rows: int | None = None, **kwargs
) -> QueryResult:
    """Execute arbitrary SQL against the database (read-only)."""
```

Opens with `sqlite3.connect(str(self._db_path), uri=True)` in read-only mode (`?mode=ro`). Useful for `learn --assets` when the annotator agent wants to inspect data.

## SQLite-Specific Considerations

### Dynamic Typing

SQLite columns don't have fixed types. `PRAGMA table_info` returns the declared type affinity (e.g., `"INTEGER"`, `"TEXT"`, `"REAL"`, `"BLOB"`, `""`), but actual values can be anything. The adapter should:

1. Report the declared type from PRAGMA as `data_type`
2. If the declared type is empty (`""`), report `"ANY"` (SQLite's actual behavior)
3. NOT try to infer types from data — that's the learning agent's job

### No Schemas

SQLite has no schema/namespace concept. Use `"_default"` as the sentinel schema name, consistent with how dbt adapter handles single-schema sources.

### System Tables

Skip tables where `name` starts with `sqlite_` or matches `_SQLITE_SYSTEM_TABLES`:
- `sqlite_sequence`
- `sqlite_stat1` through `sqlite_stat4`

### Read-Only Access

Always connect read-only. The adapter must never modify the target database:
```python
sqlite3.connect(f"file:{path}?mode=ro", uri=True)
```

### Large Databases

Some SQLite databases are multi-GB. The adapter should:
- NOT load all rows into memory
- Use `COUNT(*)` for row counts (fast in SQLite — it's a full scan, but unavoidable)
- Stream column profiling through cursors

## Registration Changes

### 1. `SourceAdapterKindV2` enum

Add `SQLITE = "sqlite"` to `packages/alma-connectors/src/alma_connectors/source_adapter_v2.py`:

```python
class SourceAdapterKindV2(StrEnum):
    # existing...
    SQLITE = "sqlite"
```

### 2. `adapters/__init__.py`

```python
from alma_connectors.adapters.sqlite import SQLiteAdapter
```

### 3. `cli/connect.py`

Add a new `connect_sqlite` command:

```python
@app.command("sqlite")
def connect_sqlite(
    path: Annotated[str | None, typer.Option("--path", help="Path to SQLite database file.")] = None,
    dir: Annotated[str | None, typer.Option("--dir", help="Directory with SQLite files.")] = None,
    glob: Annotated[str, typer.Option("--glob", help="Glob pattern for --dir.")] = "*.sqlite",
    id: Annotated[str | None, typer.Option("--id", help="Custom source ID.")] = None,
) -> None:
    """Register a SQLite data source."""
```

### 4. `pipeline/scan.py` — `_build_adapter()`

Add a `kind == "sqlite"` branch:

```python
if kind == "sqlite":
    from alma_connectors.adapters.sqlite import SQLiteAdapter

    db_path = source.params["path"]
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = PersistedSourceAdapter(
        id=adapter_id,
        key=adapter_key,
        display_name=source.id,
        kind=SourceAdapterKind.SQLITE,  # need to add this too
        target_id=source.id,
        status=SourceAdapterStatus.READY,
        config=None,  # SQLite needs no config object
    )
    return adapter, persisted
```

Note: `SourceAdapterKind` (v1 enum) also needs a `SQLITE = "sqlite"` entry, or use a generic `OTHER` kind if available. Check what the persisted adapter requires.

## Testing

### Unit Tests

`packages/alma-connectors/tests/test_sqlite_adapter.py`:

1. `test_probe_valid_db` — probe returns available=True for a valid SQLite file
2. `test_probe_missing_file` — probe returns available=False with message
3. `test_probe_invalid_file` — probe returns available=False for a non-SQLite file
4. `test_discover` — returns correct table and view count
5. `test_extract_schema_tables` — correct columns, types, row counts
6. `test_extract_schema_foreign_keys` — dependencies match PRAGMA FK list
7. `test_extract_schema_system_tables_excluded` — sqlite_* tables not in output
8. `test_extract_schema_empty_type` — columns with no declared type get "ANY"
9. `test_extract_schema_views` — views discovered with correct kind
10. `test_execute_query_readonly` — query works, write rejected

### Integration Test

`packages/alma-atlas/tests/test_sqlite_e2e.py`:

```python
def test_sqlite_scan_e2e(tmp_path):
    """Full scan pipeline: connect → scan → check assets in store."""
    # Create a test SQLite database
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), amount REAL)")
    conn.executemany("INSERT INTO users VALUES (?, ?, ?)", [(1, "Alice", 30), (2, "Bob", 25)])
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?)", [(1, 1, 99.9), (2, 2, 49.5)])
    conn.commit()
    conn.close()

    # Connect + scan
    cfg = get_config()
    source = SourceConfig(id="sqlite:test", kind="sqlite", params={"path": str(db_path)})
    cfg.add_source(source)
    result = run_scan_v2(source, cfg)

    assert result.asset_count == 2
    assert result.edge_count >= 1  # FK from orders → users
```

### BIRD Integration Test

```bash
# After building the connector:
for db in data/dev_databases/*/; do
    db_name=$(basename "$db")
    alma-atlas connect sqlite --path "$db/${db_name}.sqlite" --id "sqlite:${db_name}"
done
alma-atlas scan
alma-atlas status  # should show all 11 databases, ~80 tables
alma-atlas learn --assets --dry-run  # should list unannotated assets
```

## File Manifest

| File | Action |
|------|--------|
| `packages/alma-connectors/src/alma_connectors/adapters/sqlite.py` | **NEW** — SQLiteAdapter |
| `packages/alma-connectors/src/alma_connectors/adapters/__init__.py` | Add SQLiteAdapter export |
| `packages/alma-connectors/src/alma_connectors/source_adapter_v2.py` | Add `SQLITE` to `SourceAdapterKindV2` |
| `packages/alma-connectors/src/alma_connectors/source_adapter.py` | Add `SQLITE` to `SourceAdapterKind` (if needed) |
| `packages/alma-atlas/src/alma_atlas/cli/connect.py` | Add `connect_sqlite` command |
| `packages/alma-atlas/src/alma_atlas/pipeline/scan.py` | Add `kind == "sqlite"` to `_build_adapter()` |
| `packages/alma-connectors/tests/test_sqlite_adapter.py` | **NEW** — unit tests |
| `packages/alma-atlas/tests/test_sqlite_e2e.py` | **NEW** — integration test |
