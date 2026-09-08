# Database discovery, run predicates, and calibration reads

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

A Run Query resolves recorded membership and any explicitly supplied predicates.
It applies no automatic production or approval criterion. `collect()` explicitly materializes sorted, unique recorded
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


## Typed run predicates

Condition Definitions select the operand type from their database metadata:

```python
current = gx.conditions["beam_current"]
query = gx.runs(gluex.RunSelection.range(50000, 59999))
selected = query.where(current > 10).collect()
print(selected.numbers, selected.report.unknown_runs)

# Explicit approval is a separate scientific choice.
approved = query.where(gluex.approved_production(gluex.RunPeriod.RP2018_01))
print(approved.collect().numbers)
```

Comparisons support `.eq(value)`, `.ne(value)`, `<`, `<=`, `>`, and `>=`.
Python `==` and `!=` raise `TypeError` directing you to `.eq` and `.ne`; the named
methods return a statically typed predicate without conflicting with Python
object equality typing. Integers require signed
64-bit integer operands; floats accept finite numeric operands; booleans support
only equality and inequality with booleans. Text types accept strings. Time
conditions accept timezone-aware `datetime` values. Incompatible operands raise
`ValueError` or `TypeError`; out-of-range integers raise `OverflowError`.

Compose predicates with parenthesized `&`, `|`, and `~`. Python `and`, `or`, `not`,
chained comparisons and truth conversion are rejected because they would evaluate
the predicate before database execution. Missing comparisons remain **unknown**,
including after negation. Only a final true value includes a run. False AND unknown
is false; true OR unknown is true. `definition.is_missing()` and
`definition.is_present()` always produce true or false. `report.unknown_runs`
lists recorded runs excluded by a final unknown result, not every run with any
missing input. Unrecorded numeric requests are not predicate exclusions.

`where` returns a new query; the original remains reusable. Repeated calls combine
with AND. Provenance retains the explicit predicates, and inspection retrieves no
run values. Predicate operands are bound parameters, including strings with SQL
syntax. Named `approved_production(period)` predicates use the supported existing
period-specific approval definitions; unsupported periods raise an error.

Rust uses `definition.gt(10.0)?`, `eq(true)?`, and the other comparison methods,
with `query.filter(predicate)` and `&`, `|`, `!` composition. Rust float operands
are explicitly floating point. Time operands are `chrono::DateTime<Utc>`.
`gluex_rs::approved_production(period)?` returns an explicit `RunPredicate`.
`RunSet::report().unknown_runs()` borrows the completed exclusion list.

## CCDB-only calibration reads

```python
gx = gluex.open(rcdb=gluex.DISABLED, ccdb="ccdb.sqlite")
catalog = gx.calibrations
print(catalog.keys())
print(catalog.directories["/TARGET"].tables)
table = catalog["/TARGET/density"]
print(table.path, table.description, [(c.name, c.value_type) for c in table.columns])
query = table.for_runs(gluex.RunSelection.runs([50685, 50697]))
print(query.provenance)  # Captured source, table, scope, variation and time.
series = query.collect()
for run, entry in series.items():
    print(run, entry.assignment_id, entry.constant_set_id, entry.payload.column("density"))
print(series.report.missing_runs)
```

The catalog maps exact absolute table paths to definitions and iterates in lexical
order. Directory definitions expose child names mapped to full paths, plus local
table names mapped to definitions. Python directory mappings are independent
copies; editing them cannot change the catalog. Metadata and column discovery
never retrieve assignments or constants. Unknown catalog paths and payload column
names raise `KeyError` in Python; Rust catalog `get` returns `None`.

`for_runs` consumes the same numeric Run Selection used for RCDB queries, but
performs no recorded-membership check. A range stays compact until `collect()`;
collection currently materializes the requested numbers and results. Use bounded
numeric scopes for collection. Rust `RunSelection::All` is rejected because there
is no implicit all-runs calibration request. Empty and reversed scopes collect
empty results. Each query captures the `default` variation and its source's
opening timestamp; inspect these through `query.provenance.variation` and
`query.provenance.as_of`. Historical overrides are not exposed by this query yet.

A Calibration Series contains resolved entries in ascending run order. Python
`series.runs` is an immutable tuple of numeric runs, not an RCDB-resolved Run Set.
`series[run]` returns an immutable entry with the effective assignment identifier,
constant-set identifier, creation timestamp, variation and inclusive run range.
Its payload has named immutable tuple columns; runs with the same constant set
share decoded Rust storage. The series provenance identifies the source and query
inputs. Absent assignments appear in `report.missing_runs`; malformed payloads and
execution failures raise errors. Collection releases the GIL. Queries and results
remain usable after the original root is dropped, with source files kept unchanged.

Rust starts with `gx.calibrations()?`, then
`catalog.get("/TARGET/density").unwrap().for_runs(selection)?.collect()?`.
Use `series.items()`, `entry.payload().named_column("density")` and
`series.report().missing_runs()` for borrowed access. Table `metadata()` and
`columns()` expose immutable metadata values. Existing lower-level RCDB and CCDB
fetch APIs remain available.

The installed-package example [`examples/database_queries.py`](../examples/database_queries.py)
accepts independent RCDB and CCDB paths and exercises both query surfaces.
