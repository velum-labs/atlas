# alma-algebrakit

`alma-algebrakit` is the canonical semantic core for Atlas query reasoning.

## What lives here

- relational algebra models
- schema, catalog, and constraint types
- scope and bound-query resolution primitives
- normalization, rewriting, and proof helpers
- folding and learning components built on top of the algebraic model

## Architecture notes

- This package is the semantic spine for SQL reasoning across Atlas.
- Higher-level packages such as `alma-sqlkit` should adapt to these types rather than define competing semantic models.
- Correctness-sensitive transforms live here, so fail-closed behavior and semantic preservation matter more than convenience fallbacks.
