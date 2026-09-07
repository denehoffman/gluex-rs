# Recorded runs and read-only SQL

Open a session with an RCDB SQLite file to discover conditions and resolve numeric
scope. Keep the file unchanged while the session or its queries are in use.

```python
import gluex

gx = gluex.open(rcdb='rcdb.sqlite', ccdb=gluex.DISABLED)
conditions = gx.conditions
print(conditions.keys())
print(conditions['event_count'].value_type)  # 'int'
for name, definition in conditions.items():
    print(name, definition.description)

scope = gluex.RunSelection.runs([50697, 50685, 50697])
query = gx.runs(scope)
print(query.selection, query.provenance)  # No run retrieval.
runs = query.collect()
print(runs.numbers, runs.provenance.source)
```

The catalog includes dynamic names defined in the database. Its definitions
expose `id`, `name`, `value_type`, `description`, and native `created` text.
Descriptions and creation text are empty when unavailable. Iteration yields names
in lexical order; unknown indexed names raise `KeyError`. Catalogs and definitions
are immutable.

`RunSelection.runs(numbers)` sorts and deduplicates explicit numbers.
`RunSelection.range(start, end)` uses **inclusive** bounds and never expands the
range at construction. Reversed bounds and an empty number sequence resolve to an
empty collection. `RunSelection.period(gluex.RunPeriod.RP2018_08)` selects the
period's numeric bounds. These constructors need no database. Building a Run
Query requires RCDB, and a missing capability raises `RuntimeError` with
configuration guidance.

A Run Query resolves recorded membership only: it applies no production or
approval criterion. `collect()` explicitly materializes sorted, unique recorded
numbers, releases the GIL during Rust execution, and raises `RuntimeError` for
database failures. Query inspection does not retrieve runs. Queries are reusable
and retain their reader after the root is dropped. Results support integer
indexing (including negative Python indices), iteration, membership and length;
`numbers` is an immutable tuple. Provenance identifies the source path and
requested scope. It does not archive the file or promise historical contents.
Query provenance describes inputs; collection produces the completed Run Set.

The corresponding Rust operations are:

```rust,no_run
use gluex_rs::{GlueX, SourceConfig, RunSelection};
let gx = GlueX::open(SourceConfig::sqlite("rcdb.sqlite"), SourceConfig::Disabled)?;
let conditions = gx.conditions()?;
assert_eq!(conditions["event_count"].name(), "event_count");
let query = gx.runs(RunSelection::range(50000, 59999))?;
let runs = query.collect()?;
println!("{:?}: {:?}", runs.provenance(), runs.numbers());
# Ok::<(), Box<dyn std::error::Error>>(())
```

Rust catalog `get` returns `Option`; indexing a missing name panics like a standard
map. `keys` and `items` borrow ordered definitions. `RunSet::numbers` borrows the
immutable collection. The existing `rcdb::RunSelection` path reexports the shared
type for compatibility. Rust additionally retains the explicit `RunSelection::All`
variant for existing callers.

## Raw source reads

Both `gx.sources.rcdb.raw` and `gx.sources.ccdb.raw` execute one parameterized
SQLite read. They work independently when their respective source is configured.

```python
result = gx.sources.rcdb.raw(
    'SELECT number AS run FROM runs WHERE number BETWEEN ? AND ? ORDER BY number',
    parameters=[50000, 59999],
)
print([(column.name, column.declared_type) for column in result.columns])
for row in result.rows:
    print(row.values)
```

Parameters are keyword-only in Python, default to an empty sequence, and accept
`None`, signed 64-bit integers, floats, strings, and bytes. Integer overflow raises
`OverflowError`. SQL NULL, INTEGER, REAL, TEXT and BLOB return those same Python
value types. Rows, values, and column collections are immutable tuples or frozen
Rust-owned objects. Column metadata is available even for empty results;
expressions may have no declared type. Duplicate column names are preserved, so
values use positional access. SQL determines row order.

Rust uses `reader.raw(sql, &[RawValue::Integer(50000), ...])`, with
`RawValue::{Null, Integer, Real, Text, Blob}`. Access `result.columns()`,
`result.rows()` and `row.values()` by shared reference.

Ordinary SELECTs, CTEs and reads of `sqlite_schema` are supported. Supported PRAGMA
inspection includes `table_info`, `table_xinfo`, `index_list`, `index_info`,
`index_xinfo`, `foreign_key_list`, `query_only`, `schema_version`, `user_version`,
`database_list`, `table_list`, and `compile_options`. Other PRAGMAs are rejected.
SQLite read-only opening, query-only mode and a persistent backend authorizer
reject mutation, schema changes, attachments, transaction control, extension
loading and attempts to disable restrictions. Multiple statements are rejected,
even when both are reads. Authorization applies during preparation and execution.
SQL, binding and decoding failures raise `RawError` in Rust and `RuntimeError` in
Python. Invalid UTF-8 text is an error, while BLOB bytes remain unchanged.

**Rust migration:** unrestricted `RCDB::connection()` and `CCDB::connection()`
handles are no longer public. Replace direct prepared-statement reads with
`raw`; this keeps the reading interface enforceably read-only. The existing fetch
interfaces remain available. Raw queries materialize their results; cancellation
and streaming are not part of this surface yet.

A runnable installed-package example is
[`examples/database_reads.py`](../examples/database_reads.py).
