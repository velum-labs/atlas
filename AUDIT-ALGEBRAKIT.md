# Historical Audit Note

This document is retained for context only.

Some findings remain useful background, but this file is not the current source of truth for the repaired repo state.

# Technical Audit: alma-algebrakit

**Audited package:** `packages/alma-algebrakit/src/alma_algebrakit/`
**Audit date:** 2026-03-24
**Auditor:** Automated deep audit (all source files read)

---

## Summary

The `alma-algebrakit` package is the mathematical and algorithmic core of the Atlas query engine. It implements relational algebra ASTs, conjunctive query containment, Fourier-Motzkin elimination, Z3-based SMT solving, view folding/rewriting, workload learning, and hierarchical view derivation.

The audit covered all 55+ Python source files. Three **critical** bugs were found: one inverts the mathematical definition of a lattice operation (LUB), one causes a silent `AttributeError` at runtime, and one configuration option has had zero effect since it was introduced. Eight **high** severity issues were found, including an ON-predicate corruption bug during join canonicalization, a SQL injection vector, and silent swallowing of validation failures. Numerous medium and low severity issues round out the findings.

**Issue counts by severity:**

| Severity | Count |
|----------|-------|
| Critical | 3 |
| High | 8 |
| Medium | 14 |
| Low | 8 |

---

## Critical Issues

### CRIT-1: `compute_lub` has inverted lattice semantics

**File:** `proof/containment.py`, lines 524–607

**Description:**
The `compute_lub` function is documented as computing the *least upper bound* (LUB) in the CQ containment order, where Q1 ⊆ Q2 means every answer to Q1 is also an answer to Q2. A query Q2 is *more general* than Q1 (higher in the lattice) if it produces a superset of answers.

The implementation takes the **union** of relation atoms across all input queries and the **conjunction** of predicates. Adding more relations to a conjunctive query produces *fewer* tuples (more restrictive), not more. This is the definition of **greatest lower bound** (GLB/meet), not LUB.

```python
# Current (incorrect):
# Returns UNION of atoms + AND of predicates → most restrictive (GLB)
all_atoms = set()
for cq in cqs:
    all_atoms.update(cq.atoms)  # union of atoms = more joins = fewer rows

combined_predicates = []
for cq in cqs:
    combined_predicates.extend(cq.predicates)  # conjunction = more filters = fewer rows
```

**Correct fix:**
For a proper LUB (least general query containing all inputs): take the **intersection** of relation atom sets and the **intersection** of predicates (drop predicates not shared by all). This matches the homomorphism definition: a CQ H is a homomorphic image of CQ Q iff H ⊆ Q.

```python
# Correct LUB: intersection of atoms, intersection of shared predicates
all_atoms = cqs[0].atoms
for cq in cqs[1:]:
    all_atoms = all_atoms & cq.atoms  # intersection

shared_predicates = [p for p in cqs[0].predicates
                     if all(p in cq.predicates for cq in cqs[1:])]
```

**Impact:** Any caller using `compute_lub` for view generalization or lattice operations receives a maximally-restrictive query when a maximally-general one is expected. View learning and GCS computation produce incorrect results.

---

### CRIT-2: `_flatten_inner_joins` is a complete no-op

**File:** `normalizer/core.py`, lines 385–411

**Description:**
The function is supposed to flatten nested inner joins into a left-deep or flat tree. The implementation recurses into children and then reconstructs *identical* `Join` nodes—no flattening occurs. The `flatten_joins` configuration option has had zero effect since it was introduced.

```python
# Current (no-op):
def _flatten_inner_joins(expr: Join, config: NormalizationConfig) -> Join:
    left = _normalize_recursive(expr.left, config)   # recurse
    right = _normalize_recursive(expr.right, config)  # recurse
    # BUG: just rebuilds the same structure
    return Join(left=left, right=right, join_type=expr.join_type,
                condition=expr.condition)
    # Comment says "for now, just return with flattened children"
    # but there is no flattening
```

**Correct fix:**
Collect leaf nodes from a right-deep tree and rebuild left-deep:

```python
def _collect_inner_join_leaves(expr: RAExpression) -> list[RAExpression]:
    if isinstance(expr, Join) and expr.join_type == JoinType.INNER:
        return _collect_inner_join_leaves(expr.left) + _collect_inner_join_leaves(expr.right)
    return [expr]

def _flatten_inner_joins(expr: Join, config: NormalizationConfig) -> Join:
    leaves = _collect_inner_join_leaves(expr)
    result = leaves[0]
    for leaf in leaves[1:]:
        result = Join(left=result, right=leaf, join_type=JoinType.INNER, condition=None)
    return result
```

**Impact:** Downstream normalization steps that rely on canonical join shape (join-order canonicalization, topology extraction) operate on un-normalized trees. The `flatten_joins=True` option in `NormalizationConfig` is silently ignored.

---

### CRIT-3: `extract_common_structure` calls a non-existent method

**File:** `learning/gcs.py`, lines 518–526

**Description:**
`extract_common_structure` calls `QueryGeneralizer.compute_common_structure(cqs)`, but `QueryGeneralizer` has no such method. This raises `AttributeError` at runtime whenever this function is called.

```python
# Current (crashes at runtime):
def extract_common_structure(queries, ...):
    generalizer = QueryGeneralizer(...)
    result = generalizer.compute_common_structure(cqs)  # AttributeError!
```

**Impact:** Any workload learning or view derivation path that calls `extract_common_structure` will crash. This is a latent bug that would surface as soon as the learning pipeline is exercised on a real workload.

**Fix:** Implement `compute_common_structure` on `QueryGeneralizer`, or replace the call with the correct existing method name.

---

## High Issues

### HIGH-1: Join canonicalization corrupts ON predicate column references

**File:** `normalizer/core.py`, lines 487–517

**Description:**
`_canonicalize_join_order` swaps `left` and `right` subtrees when `left_name > right_name`, but does not rewrite the ON predicate's column references to reflect the new side assignment. After a swap, a predicate `a.key = b.key` remains literally but `a` is now in the right subtree. For SQL emitters that use left/right position semantics, this produces invalid SQL.

```python
# Current (incorrect after swap):
if left_name > right_name:
    return Join(
        left=right,   # swapped
        right=left,   # swapped
        join_type=expr.join_type,
        condition=expr.condition,  # NOT updated — still references original aliases
    )
```

**Fix:** After swapping, traverse the condition and swap column aliases if they belong to the swapped subtrees. Alternatively, canonicalize by sorting relation *aliases* in the condition rather than swapping the subtree positions.

---

### HIGH-2: `_build_join_plan` silently accepts unreachable single remaining relation

**File:** `folding/boundary.py`, lines 291–307

**Description:**
The unreachability check has a dead-code `pass` that silently marks the analysis as valid when there is exactly one remaining relation and it was never joined to anything.

```python
if truly_unreachable and len(remaining) > 0:
    if len(remaining) == 1 and not joined_remaining:
        pass  # BUG: silently accepts—join plan is incomplete
    elif truly_unreachable:
        analysis.is_valid = False
        analysis.rejection_reasons.append(...)
```

When `len(remaining) == 1` and `joined_remaining` is empty, the remaining relation has no join path to the view, yet `analysis.is_valid` stays `True`. The rewriter will produce a query with a dangling cross-join or missing FROM clause entry.

**Fix:** Remove the `pass` branch or make it explicit:

```python
if truly_unreachable:
    analysis.is_valid = False
    analysis.rejection_reasons.append(
        f"Remaining relations have no join path: {[str(r) for r in truly_unreachable]}"
    )
```

---

### HIGH-3: Full-coverage fold ignores `check_fold_condition` failures

**File:** `folding/folder.py`, lines 148–152

**Description:**
`fold_query` calls `check_fold_condition` and examines its result, but when `coverage == FULL` and `can_fold=False`, the code has a bare `pass` and continues to `_fold_full_coverage` regardless.

```python
result = check_fold_condition(query_ra, view, config)
if result.coverage == CoverageType.FULL:
    if not result.can_fold:
        pass  # BUG: validation failure is silently ignored
    return _fold_full_coverage(query_ra, view, config, view_alias)
```

**Fix:**

```python
if result.coverage == CoverageType.FULL:
    if not result.can_fold:
        return FoldResult(success=False, reasons=result.rejection_reasons)
    return _fold_full_coverage(query_ra, view, config, view_alias)
```

---

### HIGH-4: `_build_full_coverage_column_map` uses invalid prefix matching

**File:** `folding/folder.py`, lines 540–550

**Description:**
The function builds a column rewrite map by matching keys of the form `f"{alias}."` (a prefix string) against entries in `column_rewrite_map`. A key like `"orders."` will never match a full column key like `"orders.id"` in a dict lookup. The column map is silently empty, causing the rewriter to emit unqualified column references or miss rewrites.

```python
# Current (broken—prefix never matches a dict key):
for alias in covered_aliases:
    prefix = f"{alias}."
    for key, val in column_rewrite_map.items():
        if key == prefix:  # this is never True
            result[key] = val
```

**Fix:** Match by prefix substring:

```python
for key, val in column_rewrite_map.items():
    if any(key.startswith(f"{alias}.") for alias in covered_aliases):
        result[key] = val
```

---

### HIGH-5: `ra_to_cq` uses a single wildcard variable per relation

**File:** `learning/gcs.py`, lines 208–222

**Description:**
`ra_to_cq` creates exactly one CQ variable per relation, named `f"{alias}_*"`. This means the CQ representation cannot express column-level bindings. The homomorphism search (`_find_homomorphism`) checks that each atom maps to an atom in the target—but because all columns of a relation share a single variable, any predicate involving `orders.id` and `orders.amount` are treated identically. Column-level containment checks are unsound.

**Fix:** Create one variable per `(alias, column)` pair:

```python
for col in relation.columns:
    var_name = f"{alias}_{col}"
    cq.add_variable(var_name)
    cq.add_column_binding(alias, col, var_name)
```

---

### HIGH-6: SQL injection in `DuckDBExecutor._values_clause`

**File:** `proof/empirical.py`, lines 447–462

**Description:**
String values are interpolated directly into SQL without escaping:

```python
# Current (SQL injection):
elif isinstance(val, str):
    parts.append(f"'{val}'")  # apostrophes in val break the query
```

A string value containing a single quote (e.g., `"O'Brien"`) will produce malformed SQL. A malicious value like `"'; DROP TABLE foo; --"` could execute arbitrary SQL against the DuckDB in-process database.

**Fix:** Use parameterized queries (DuckDB supports `?` or `$1` placeholders):

```python
params.append(val)
parts.append("?")
```

Or use DuckDB's proper quoting: `val.replace("'", "''")`

---

### HIGH-7: `_normalize_join_edge` discards the join predicate

**File:** `rewriting/equivalence.py`, line 188

**Description:**
When normalizing a join edge for equivalence comparison:

```python
t1, t2, _ = edge  # predicate (condition) is discarded
```

Two joins on the same pair of tables but on *different columns* (e.g., `orders.customer_id = customers.id` vs `orders.billing_id = customers.id`) are treated as equivalent. This produces false positives in equivalence checks.

**Fix:** Include the predicate in the normalized edge:

```python
t1, t2, predicate = edge
normalized = (min(t1, t2), max(t1, t2), predicate)
```

---

### HIGH-8: `_rewrite_predicate` can insert `None` into `CompoundPredicate.operands`

**File:** `normalizer/core.py`, lines 990–1000

**Description:**
`_rewrite_predicate` calls itself recursively on each operand and appends the result to `new_operands`. If the recursive call returns `None` (the documented "no rewrite needed" sentinel), `None` is appended:

```python
new_operands = []
for op in pred.operands:
    new_operands.append(_rewrite_predicate(op, ...))  # may be None
return CompoundPredicate(operands=new_operands, ...)  # None in operands
```

Downstream code that iterates over `CompoundPredicate.operands` will encounter `None` elements and crash with `AttributeError`.

**Fix:**

```python
new_operands = []
changed = False
for op in pred.operands:
    rewritten = _rewrite_predicate(op, ...)
    if rewritten is not None:
        new_operands.append(rewritten)
        changed = True
    else:
        new_operands.append(op)
if not changed:
    return None
return CompoundPredicate(operands=new_operands, ...)
```

---

## Medium Issues

### MED-1: `LRUCache` is not thread-safe

**File:** `proof/implication.py`

`LRUCache` uses a plain `dict` and `OrderedDict` with no locking. If `PredicateImplicationChecker` is shared across threads (e.g., in an async request handler), concurrent `get`/`put` calls will corrupt the cache structure. Fix: add a `threading.Lock` around all cache mutations.

---

### MED-2: Float precision loss in range containment

**File:** `proof/implication.py`, line 664

```python
val = float(value)  # loses precision for large integers / Decimal
```

Columns with `BIGINT` or `NUMERIC(38,10)` values are converted to 64-bit float, silently losing precision. A value of `9999999999999999` becomes `10000000000000000.0` after float conversion. Fix: keep values as `Decimal` or `int` when the column type is known to be integer/decimal.

---

### MED-3: `Attribute.__hash__` uses only name, ignores type

**File:** `schema/types.py`, line 140

```python
def __hash__(self):
    return hash(self.name)  # two Attributes with same name, different types hash equal
```

`Attribute("id", DataType.INTEGER)` and `Attribute("id", DataType.TEXT)` have the same hash. In a set, the second one silently replaces the first. Fix: `return hash((self.name, self.type))`.

---

### MED-4: `AttributeRef.id` UUID is dead state

**File:** `bound/types.py`

`AttributeRef` has a `id: UUID = Field(default_factory=uuid4)` field that is never used in `__hash__` or `__eq__`. Every instance gets a unique UUID that has no semantic meaning and adds 16 bytes of memory per attribute reference for no benefit. Remove the field or document its intended purpose.

---

### MED-5: `DataType.is_comparable_to` treats UNKNOWN as comparable to everything

**File:** `schema/types.py`, line 121

`UNKNOWN` type passes all type-compatibility checks. This allows predicates like `integer_col > text_col` to be accepted if either operand has `UNKNOWN` type. These silent type mismatches will produce runtime errors or incorrect results in DuckDB. Fix: treat `UNKNOWN` comparisons as an explicit warning/error in strict mode.

---

### MED-6: `check_containment` returns `equivalent=True` with `confidence=0.8`

**File:** `rewriting/equivalence.py`, lines 126–129

The function checks only table names and attribute counts, not predicates or join conditions. Yet it returns `equivalent=True`. Callers that act on `equivalent=True` without checking `confidence` will treat structurally different queries as equivalent. Fix: return `equivalent=False` when `confidence < 1.0`, and rename the result field to `structurally_similar` to be less misleading.

---

### MED-7: `_decorrelate_recursive` does not recurse into Aggregation/Union/etc.

**File:** `transforms/decorrelate.py`, line 279

When a subquery contains `Aggregation`, `Union`, `Difference`, `Intersect`, `Sort`, or `Limit` nodes, `_decorrelate_recursive` returns the node unchanged without visiting children. Correlated subqueries nested inside aggregations or set operations are silently left correlated.

```python
# Current:
else:
    return expr  # no recursion into children

# Fix: recurse into children generically
else:
    return _transform_children(expr, _decorrelate_recursive, ...)
```

---

### MED-8: `_check_linear_arithmetic` may run on non-numeric columns

**File:** `proof/implication.py`

Layer 3 (Fourier-Motzkin) is attempted even when no `type_env` is provided. FM elimination assumes all variables are numeric. If a column is of type `TEXT` or `DATE`, the system will produce nonsensical inequality constraints and may return incorrect implication results.

Fix: skip FM elimination when `type_env` is unavailable or when any variable in the constraint has a non-numeric type.

---

### MED-9: `_find_homomorphism` has no timeout or depth bound

**File:** `proof/containment.py`, lines 189–239

The backtracking homomorphism search is exponential in the worst case: O(|target_atoms|^|source_atoms|). For large conjunctive queries with many relations, this may run indefinitely. No timeout, iteration count, or depth bound is enforced.

Fix: add a `max_iterations` parameter (defaulting to, say, 100,000) and return `None` (unknown) when the bound is exceeded.

---

### MED-10: `_check_predicate_implied` uses string literal instead of enum

**File:** `proof/containment.py`, line 400

```python
return ImplicationResult(method="unknown", ...)  # string literal
# Should be:
return ImplicationResult(method=ImplicationMethod.UNKNOWN, ...)
```

This bypasses the enum type system and will cause comparison failures (`result.method == ImplicationMethod.UNKNOWN` will be `False`).

---

### MED-11: O(n) linear scan in `RelationBinding.get_column`

**File:** `bound/query.py`

`get_column` does a case-insensitive linear scan over all columns on every lookup. In queries with wide tables (100+ columns), this is called O(predicates × columns) times. Fix: build a lowercased `dict` index at construction time.

---

### MED-12: `ORDER BY` columns excluded from `all_upstream_columns`

**File:** `bound/query.py`

`BoundQuery.all_upstream_columns()` iterates over `select`, `where`, and `group_by` but not `order_by`. Columns referenced only in `ORDER BY` are not tracked as upstream dependencies. A query optimizer that relies on this method to determine which columns must be available will silently drop ORDER BY columns.

---

### MED-13: `cq_variable_name` collision when alias contains underscore

**File:** `proof/containment.py`

Variable names are formed as `f"{rel.alias}_{col_name}"`. If `alias="order_item"` and `col_name="id"`, the variable is `"order_item_id"`. But `alias="order"` and `col_name="item_id"` also produces `"order_item_id"`. These are different columns that hash to the same variable, causing silent predicate misattribution.

Fix: use a separator that cannot appear in SQL identifiers, e.g., `f"{rel.alias}::{col_name}"`.

---

### MED-14: `SchemaConstraints` is mutable and has no index on FK lookups

**File:** `schema/constraints.py`

`SchemaConstraints` is a mutable dataclass. Both `has_fk` and `get_fk` do O(n) linear scans over all FKs on every call. During boundary analysis, these are called once per join edge per query. For schemas with 100+ FKs, this is a performance bottleneck. Fix: build a dict index `{(from_table, from_col): FK}` at construction time, or use `@cached_property`.

---

## Low Issues

### LOW-1: `has_subquery` field is always `False` in `PatternSignature`

**File:** `learning/patterns.py`, line 375

```python
has_subquery = False  # TODO: detect subqueries
```

This field is never set to `True`. Any clustering or similarity logic that uses `has_subquery` for differentiation has no effect. Either implement detection or remove the field.

---

### LOW-2: `use_linear_arithmetic` missing from `ProofConfig`

**File:** `config.py` / `proof/implication.py`

`PredicateImplicationChecker.__init__` accesses `getattr(config, 'use_linear_arithmetic', True)`. The `ProofConfig` Pydantic model does not define this field. The `getattr` fallback masks the missing field. Fix: add `use_linear_arithmetic: bool = True` to `ProofConfig`.

---

### LOW-3: BFS in `Topology.is_connected` uses `list.pop(0)` (O(n) per step)

**File:** `learning/topology.py`

```python
queue = [start]
while queue:
    node = queue.pop(0)  # O(n) — should be deque.popleft()
```

For topologies with many relations, this is O(n²) instead of O(n). Fix: `from collections import deque; queue = deque([start])` and use `queue.popleft()`.

---

### LOW-4: `HierarchicalViewDeriver` assumes hub table has `id` column

**File:** `learning/derivation.py`, line 590

```python
candidate_keys = [[f"{hub_table}.id"]]  # hardcoded assumption
```

Tables that use composite keys or non-`id` primary key names will produce incorrect key specifications. Fix: look up the actual primary key from `SchemaConstraints`.

---

### LOW-5: `_outer.` is an undocumented magic string protocol

**File:** `scope/resolution.py`, line 128

The `_outer.` prefix on aliases is a convention for LATERAL query scope resolution but is not documented anywhere in the module. Any code that constructs alias names must know to use this prefix. Fix: define a named constant `OUTER_SCOPE_PREFIX = "_outer."` and reference it from both the producer and consumer.

---

### LOW-6: `clustering._find_cluster` is recursive (potential stack overflow)

**File:** `learning/clustering.py`

The union-find `_find_cluster` uses recursive path compression. Python's default recursion limit (~1000) means inputs with more than ~500 merges in a chain will raise `RecursionError`. Fix: implement iterative path compression.

---

### LOW-7: `compute_similarity` returns 0.0 for two empty signatures

**File:** `learning/clustering.py`, line 171

Two queries with empty feature signatures (no tables, no predicates) have Jaccard similarity of 0.0 (0/0 → 0). Semantically they are identical. While this is mathematically conventional (0/0 = 0 by definition), it means two trivially identical queries are placed in separate clusters. Fix: return `1.0` when both signatures are empty.

---

### LOW-8: `ViewCandidate.to_view_specification` inverts column lineage direction

**File:** `learning/derivation.py`

`column_lineage` is built with keys as stripped column names and values as `"table.col"`. The expected format in the rest of the codebase (e.g., `boundary.py`, `folder.py`) is `{(table, col): view_col_name}` or `{qualified_col: view_col_name}`. This mismatch will cause column rewrite to silently fail when views derived from `HierarchicalViewDeriver` are used for folding.

---

## File-by-File Notes

### `__init__.py`
No issues. Clean re-export of 55+ symbols.

### `config.py`
- LOW-2: `use_linear_arithmetic` missing from `ProofConfig`

### `visitor.py`
- Duck-typing fallback for `type == "with_clause"` at line 368–370 is fragile. New expression types with a `type` attribute could be silently misrouted. Consider a stricter isinstance check or an explicit registry.
- `TransformationVisitor` identity-check optimization is sound.

### `exceptions.py`
No issues. Well-structured exception hierarchy.

### `models/algebra.py`
- `RelationRef` frozen dataclass is correctly designed for use as set/dict key.
- `SubqueryExpression`, `ExistsExpression`, `InSubqueryExpression` are modeled but `_decorrelate_recursive` only handles `ExistsExpression` (see MED-7).

### `models/capabilities.py`
- `QueryCapabilities.is_spj()` returns `True` for queries with `DISTINCT` because `DISTINCT` is a separate boolean field not included in the `sql_features` check. A `SELECT DISTINCT` query changes bag→set semantics and is not a pure SPJ query. The `is_spj()` method should also check `not self.has_distinct`.

### `bound/types.py`
- MED-4: `AttributeRef.id` UUID is dead state.
- `BoundLogical` for NOT is not validated to have exactly 1 operand.

### `bound/query.py`
- MED-11: O(n) column scan.
- MED-12: ORDER BY excluded from upstream column tracking.

### `bound/fingerprint.py`
- Duck-typing dispatch ordering for predicate type detection is correct but fragile. A comment explaining the ordering constraint would prevent future regressions.

### `normalizer/core.py`
- CRIT-2: `_flatten_inner_joins` is a no-op.
- HIGH-1: `_canonicalize_join_order` corrupts ON predicates.
- HIGH-8: `_rewrite_predicate` can insert `None` into compound operands.
- `extract_join_graph()` uses "first table" as representative for multi-table subtrees—only correct for leaf `Relation` nodes. Would be incorrect for subquery-producing nodes.
- `predicate_implies` recursion has no depth limit (line 737).

### `proof/implication.py`
- MED-1: Non-thread-safe LRU cache.
- MED-2: Float precision loss in range containment.
- MED-8: FM elimination attempted without type environment.
- LOW-2: `use_linear_arithmetic` accessed via `getattr` fallback.

### `proof/containment.py`
- CRIT-1: `compute_lub` has inverted LUB semantics.
- HIGH-5 (also in `gcs.py`): CQ variable representation is per-relation not per-column.
- MED-9: `_find_homomorphism` has no timeout.
- MED-10: `ImplicationMethod.UNKNOWN` used as string literal.
- MED-13: CQ variable name collision when alias contains underscore.

### `proof/linear_arithmetic.py`
- Strict inequality handling in `_combine_inequalities` is correct.
- `NOT BETWEEN` returns `None` (non-linear) is correct—OR is not linear.
- `is_linear_predicate` variable naming: loop variable `op` shadows `pred` parameter name (cosmetic).

### `proof/empirical.py`
- HIGH-6: SQL injection via unescaped string values.
- `_normalize_value` 10-decimal-place float rounding is arbitrary; may produce different canonical forms for logically equal floats across platforms.

### `folding/folder.py`
- HIGH-3: Full-coverage fold ignores `check_fold_condition` failures.
- HIGH-4: `_build_full_coverage_column_map` uses prefix string as dict key (never matches).
- `_compute_attribute_coverage` returns `0.0` when `query_attrs` is empty rather than `1.0` (vacuous coverage).

### `folding/boundary.py`
- HIGH-2: Silently accepts unreachable single remaining relation.
- `normalize_table_name` is correct for unqualified comparison but callers must ensure consistent normalization throughout the pipeline.

### `folding/outer_join_inference.py`
- Mathematical foundation (FK + NOT NULL → INNER JOIN) is correct.
- `_extract_join_columns` only inspects the first operand of compound ON predicates.
- `_left_has_remaining` / `_right_has_remaining` computed but never used (lines 226–227).

### `rewriting/equivalence.py`
- MED-6: `check_containment` returns `True` with `confidence=0.8`.
- HIGH-7: `_normalize_join_edge` discards the join predicate.

### `rewriting/columns.py`, `rewriting/joins.py`, `rewriting/predicates.py`
- No critical issues observed. Predicate rewriting handles most expression types.

### `scope/resolution.py`
- LOW-5: `_outer.` undocumented magic string prefix.

### `schema/types.py`
- MED-3: `Attribute.__hash__` uses only name.
- MED-5: `DataType.is_comparable_to` treats UNKNOWN as comparable to everything.

### `schema/constraints.py`
- MED-14: Mutable dataclass with O(n) FK lookup.

### `transforms/decorrelate.py`
- MED-7: `_decorrelate_recursive` does not recurse into Aggregation/Union/Difference/Intersect/Sort/Limit.

### `learning/clustering.py`
- MED (performance): O(n³) agglomerative clustering.
- LOW-6: Recursive union-find can stack overflow.
- LOW-7: Empty signature similarity returns 0.0.

### `learning/patterns.py`
- LOW-1: `has_subquery` always `False`.
- `PatternExtractionVisitor` correctly saves/restores state around CTE internals.

### `learning/gcs.py`
- CRIT-3: `extract_common_structure` calls non-existent `compute_common_structure` method.
- HIGH-5: `ra_to_cq` one variable per relation instead of per column.
- `cq_to_ra` uses CROSS JOINs for all relations—algebraically correct but extremely inefficient.

### `learning/topology.py`
- LOW-3: BFS uses `list.pop(0)` instead of `deque.popleft()`.
- `topology_to_ra` greedy construction is O(n³).
- Self-join canonicalization in `EdgeBasedCanonicalizer` is complex but appears correct.

### `learning/derivation.py`
- LOW-4: Hardcoded `hub_table.id` primary key assumption.
- LOW-8: `ViewCandidate.to_view_specification` inverts column lineage direction.

### `learning/workload.py`
- `Workload.get_pattern` is O(n) linear scan (no dict index by ID). For workloads with thousands of patterns this is a bottleneck.

### `learning/evidence.py`
- Large file; evidence scoring and propagation logic not fully audited. Spot-check showed no critical issues, but the numeric scoring functions should be reviewed for numeric stability (division by zero guards appear to be present).

---

## Performance Hotspots

| Location | Complexity | Notes |
|----------|-----------|-------|
| `learning/clustering.py:_agglomerative_cluster` | O(n³) | Pairwise similarity precomputed then O(n) merge scans |
| `learning/topology.py:topology_to_ra` | O(n³) | Greedy join construction |
| `learning/topology.py:Topology.is_connected` | O(n²) | `list.pop(0)` in BFS instead of deque |
| `proof/containment.py:_find_homomorphism` | O(k^n) | Unbounded backtracking, no timeout |
| `schema/constraints.py:has_fk / get_fk` | O(n) per call | No index; called in tight loops during boundary analysis |
| `bound/query.py:RelationBinding.get_column` | O(n) per call | Case-insensitive scan; no dict index |
| `learning/workload.py:Workload.get_pattern` | O(n) per call | Linear scan; should be a dict |

---

## Recommendations

1. **Fix CRIT-1 first.** The inverted `compute_lub` semantics affects all view learning and GCS paths. Any downstream test that does not compare against known-correct LUB outputs will silently pass with wrong results.

2. **Fix CRIT-3 immediately.** `extract_common_structure` will `AttributeError` at runtime on any real workload. Add a regression test that calls this function.

3. **Add a test for `flatten_joins` (CRIT-2).** Write a test that passes `NormalizationConfig(flatten_joins=True)`, normalizes a right-deep join tree, and asserts the result is left-deep. This test would have caught the no-op immediately.

4. **Fix HIGH-1 (`_canonicalize_join_order`) before enabling join reordering.** Swapping join sides without rewriting ON predicates produces syntactically valid but semantically incorrect SQL. Add a round-trip test: normalize → emit SQL → parse → check predicate column references.

5. **Parameterize the DuckDB SQL (HIGH-6).** SQL injection via empirical validation is a critical security surface in multi-tenant deployments. Switch to prepared statements immediately.

6. **Add thread safety to `LRUCache` (MED-1).** Even without explicit threading, async frameworks (asyncio with thread executors) can cause concurrent access. A `threading.RLock` has negligible overhead.

7. **Index FK lookups in `SchemaConstraints` (MED-14).** Build a `dict[tuple[str,str], FK]` at construction. This is a zero-risk performance improvement.

8. **Add `ORDER BY` columns to `all_upstream_columns` (MED-12).** This is a correctness issue for query optimization passes that rely on dependency tracking.

9. **Fix `Attribute.__hash__` to include type (MED-3).** Schema-wide attribute sets may silently deduplicate attributes with the same name but different types.

10. **Replace duck-typing dispatch with a formal expression visitor registry.** Multiple files (`normalizer/core.py`, `bound/fingerprint.py`, `visitor.py`) use ad-hoc attribute inspection to dispatch on expression type. A centralized visitor registry would eliminate entire classes of silent misidentification bugs.

11. **Add a missing-method test for `QueryGeneralizer` (CRIT-3).** Python's duck typing means these method-not-found bugs are only caught at runtime. A simple instantiation+call test in CI would catch this class of error.

12. **Document or remove `_outer.` prefix convention (LOW-5).** Magic string protocols in scope resolution are a maintenance hazard. Define a module-level constant and reference it bidirectionally.
