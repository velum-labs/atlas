# alma-sqlkit

`alma-sqlkit` is the SQL-facing adapter layer around the canonical `alma-algebrakit` semantic model.

## What lives here

- `SQLParser` for SQL -> relational algebra conversion
- `SQLBinder` for SQL -> bound query conversion
- `SQLEmitter` for relational algebra -> SQL emission
- `SQLBuilder` for fluent SQL construction
- dialect helpers and SQL-specific extension types

## Architecture notes

- `alma-sqlkit` does not own a separate semantic model for CTEs or bound queries; it reuses the canonical `alma-algebrakit` types.
- The parser, binder, and emitter are the main entry points. All of them should preserve or adapt into the same underlying algebraic model rather than invent parallel structures.
