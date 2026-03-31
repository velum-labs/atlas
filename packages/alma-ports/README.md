# alma-ports

`alma-ports` contains the shared record types, repository protocols, error types, and safety helpers that define Atlas' infrastructure boundary.

## What lives here

- protocol contracts for assets, queries, schemas, contracts, consumers, and pipeline storage
- shared record/dataclass shapes aligned with the concrete SQLite store
- cross-package error types in `src/alma_ports/errors.py`
- SQL identifier safety helpers in `src/alma_ports/sql_safety.py`

## Architecture notes

- These contracts are now aligned with the concrete `alma-atlas-store` repository API instead of describing a separate speculative storage model.
- `alma-ports` is intentionally lightweight and dependency-minimal so higher-level packages can depend on it safely.
