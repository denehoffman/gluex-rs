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
interfaces remain available. Raw queries materialize their results rather than
streaming. Rust callers can pass `ExecutionOptions` to `raw_with_options`; Python
callers use the keyword-only `timeout` argument and may interrupt an in-flight
read with `KeyboardInterrupt`.

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
with `query.where_predicate(predicate)` and `&`, `|`, `!` composition. Rust float operands
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
`query.provenance.as_of`. Use `with_variation(name)` and `as_of(timestamp)` for explicit historical requests (see below).

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
Use `series.items()`, `entry.payload().column("density")` and
`series.report().missing_runs()` for borrowed access. Table `metadata()` and
`columns()` expose immutable metadata values. Existing lower-level RCDB and CCDB
fetch APIs remain available.

The installed-package example [`examples/database_queries.py`](../examples/database_queries.py)
accepts independent RCDB and CCDB paths and exercises both query surfaces.


## Condition projections

```python
query = gx.runs(gluex.RunSelection.range(50000, 59999))
projected = query.select(["beam_current", "polarization_direction"])
print(projected.provenance)  # No value retrieval.
result = projected.collect()
print(result.runs.numbers)
print(result.column("beam_current"))  # Same order as result.runs.numbers.
for run in result.runs:
    print(run, result[run, "polarization_direction"])
print(result.report.missing_values)
```

`RunQuery.select(names)` returns a distinct `ConditionQuery`; its `collect()`
returns `ConditionResults`, while the original Run Query still collects a
`RunSet`. Names are validated without reading values; empty projections and
unknown names raise `ValueError`. Duplicate names are deduplicated in request
order. Non-string fields raise `TypeError`.

Values are native `int`, `float`, `bool`, `str`, timezone-aware UTC `datetime`, or
`None`. JSON and RCDB blob text remain strings. A missing row or SQL NULL becomes
`None`, and `report.missing_values` lists `(run, name)` pairs in run/name order.
An unknown result run or unprojected name raises `KeyError`, distinguishing an
invalid lookup from an absent value. Columns are immutable tuples. Results,
provenance and reports are immutable Rust-owned objects. Predicate exclusions
remain available through `result.runs.report.unknown_runs`.

`result.provenance.runs` retains source, numeric scope and predicates;
`result.provenance.fields` retains projected names. Malformed encoded values
(including invalid timestamps, JSON, boolean values and numeric types) raise
`RuntimeError` with run/name context; they are never converted to missing values.
Collection releases the GIL. `strict()` rejects any missing projected value.
`fill(name, value=...)` applies a caller-supplied, type-checked substitution for
that condition and records every substituted `(run, name)` cell in the report
and provenance; malformed values and execution failures remain errors.

Rust uses `query.select(["beam_current", "polarization_direction"])?`,
`result.get(run, name)?` for `Option<&ConditionValue>`, and `result.column(name)?`
for an aligned slice of optional typed values. Invalid lookups return `DatabaseError`.

## Streaming and terminal operations

Run, condition-projection, and calibration queries support bounded chunk
iteration. The chunk size is keyword-only in Python and must be positive:

```python
for chunk in query.stream(chunk_size=256):
    print(chunk.numbers, chunk.report.evaluated_runs, chunk.report.complete)

for chunk in projected.stream(chunk_size=256):
    print(chunk.runs.numbers, chunk.report.missing_values)
```

Run chunks contain matching runs; `chunk.report.evaluated_runs` identifies the
recorded candidates actually examined. `complete` is false until the final
chunk. A condition chunk exposes the same progress through
`chunk.runs.report`. Dropping an iterator abandons the remaining work and holds
no live SQLite cursor. Ranges and sparse selections are paged without expanding
the complete numeric request. `collect()` drains the same evaluator and marks
its report complete, so streamed and collected values agree.

Every query also provides `first()`, `one()`, and `count()`. `first()` stops after
the first matching or requested result, `one()` rejects zero or multiple
results, and `count()` evaluates the whole query without retaining a complete
collection. For projected and calibration queries, `first()` and `one()` return
the corresponding one-row result object. Rust uses `stream(chunk_size)?` and
the same terminal method names; stream items are `Result` values.

## Composed calibration requests

A calibration table accepts a numeric Run Selection, a resolved Run Set, or a
lazy Run Query. A composed lazy query resolves RCDB membership only when it is
evaluated and retains the numeric scope and predicates in calibration
provenance:

```python
runs = gx.runs(gluex.RunSelection.period(gluex.RunPeriod.RP2018_08))
reconstruction = gluex.ReconstructionSelection.periods({
    gluex.RunPeriod.RP2018_08:
        gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2018_08, 2),
})
series = (
    gx.calibrations['/TARGET/density']
    .for_runs(runs)
    .with_reconstruction(reconstruction)
    .collect()
)
print(series.provenance.runs)
print(series.provenance.run_report.unknown_runs)
print(series.provenance.resolved_reconstruction)
```

Each requested period must have an entry. Use
`ReconstructionSelection.latest()` to explicitly select the source-opening
defaults for all represented periods. Direct `with_variation()` or `as_of()`
selectors conflict with a reconstruction selector and fail instead of silently
taking precedence. Numeric-only requests remain CCDB-only. Rust uses
`table.for_query(&query)?`, `table.for_run_set(&runs)?`, and
`ReconstructionSelection::{latest, periods}`.

## Missing Data Policies

Interactive retrieval remains in report mode by default. Condition reports keep
every missing `(run, name)` cell, and calibration reports keep every run without
an assignment. Strict mode rejects the first chunk containing an omission:

```python
complete_conditions = projected.strict().collect()
complete_calibrations = calibration_query.strict().collect()
```

Fallbacks must be supplied explicitly. `projected.fill(name, value=...)` validates
the value against that Condition Definition. `calibration_query.fallback_to(run)`
resolves an assignment for the same table and selectors, then uses it for each
missing requested run. Original omissions remain visible, while
`report.substitutions` records every replacement. A missing calibration fallback
is itself an error. Malformed stored values, payloads, selectors, schemas, and
database failures always raise under every policy; fallback behavior applies
only to genuine absence. Result provenance records `missing_policy`, plus
`fallback_fields` or `fallback_run` as applicable. Predicate unknown semantics
are unchanged.

## Historical calibration requests

```python
from datetime import datetime, timezone

query = gx.calibrations["/TARGET/density"].for_runs(
    gluex.RunSelection.runs([50685, 50697])
)
historical = query.with_variation("mc").as_of(
    datetime(2019, 1, 1, tzinfo=timezone.utc)
)
series = historical.collect()
print(series.provenance)
for run, entry in series.items():
    print(run, entry.assignment_id, entry.created, entry.variation, entry.run_range)
```

Both transformations return new queries, preserve the original, and perform no
assignment lookup. Python requires a timezone-aware datetime; Rust uses
`query.with_variation("mc").as_of(timestamp)` with `chrono::DateTime<Utc>`.
Invalid variation names fail at collection, even for empty selections. Explicit
cutoffs are inclusive and always interpreted in UTC; the source opening time is
used only when no cutoff is supplied.

Resolution follows the requested variation first, then its parent for each
unresolved run, retaining the same time cutoff throughout the chain. Run bounds
are inclusive. Within each variation, the **highest eligible assignment ID** wins,
even when creation timestamps are equal or not ordered by ID. This matches
Jefferson Lab's [CCDB SQLite resolver](https://github.com/JeffersonLab/ccdb/blob/63525fb0065c7fd0ef8f2742c681f5d4683eeecd/cpp/src/CCDB/Providers/SQLiteDataProvider.cc#L286)
and [Python resolver](https://github.com/JeffersonLab/ccdb/blob/63525fb0065c7fd0ef8f2742c681f5d4683eeecd/python/ccdb/provider.py#L934).
The same rule is present in the [v1 SQLite resolver](https://github.com/JeffersonLab/ccdb/blob/5bc855b98f5e4cd332a3ff94b3ab7e24aa862830/src/Library/Providers/SQLiteDataProvider.cc#L999).
The legacy `goBackBehavior` and `goBackTime` metadata fields do not alter lookup
in these upstream readers, and do not alter this reader's parent fallback.

**Correctness change:** the prior Rust reader ranked eligible assignments by
creation timestamp. Requests where timestamp order differs from assignment-ID
order now follow CCDB's assignment-ID rule. Existing lower-level reads use the
same corrected resolver. The API treats stored timestamp text as UTC and does
not inherit the C++ reader's process-local timezone conversion.

Series preserve requested selectors separately from each entry's effective
assignment, variation, constant-set ID, creation time and run range. Repeated
constants share immutable decoded storage, including across different
assignments. Cyclic or missing variation parents, malformed timestamps, invalid
column metadata and payload decoding errors raise contextual errors containing
the table and selectors. Genuine absent assignments remain in `missing_runs`.

## Refreshing captured sources

```python
old_query = gx.calibrations["/TARGET/density"].for_runs(
    gluex.RunSelection.runs([50685])
)
old_time = old_query.provenance.as_of
gx.refresh()
new_query = gx.calibrations["/TARGET/density"].for_runs(
    gluex.RunSelection.runs([50685])
)
assert old_query.provenance.as_of == old_time
print(new_query.provenance.as_of)
```

`refresh()` reopens the captured source paths, rebuilds metadata caches, and
captures a new opening time. It does not re-read environment variables. To select
different paths or capabilities, open a new GlueX object. Existing queries,
catalogs, reader handles, results, and Rust clones retain their original bindings.
Python releases the GIL while reopening; Rust uses `gx.refresh()?` on a mutable
session. If either configured source fails, the entire refresh fails and leaves
the session unchanged (`ValueError` in Python; `GlueXError` in Rust).

Keep SQLite files unchanged while readers or queries use them. Finish all work
using an old file before replacing it and refreshing. Existing results remain
immutable, but old queries do not promise access to historical contents after a
file is modified or replaced. Refresh creates no snapshot copies or monitoring.

## Canonical luminosity workflow

Luminosity is evaluated from an already resolved `RunSet`; the workflow never
adds an approval or production predicate. Reconstruction is mandatory and may
be explicit per period or an explicit request for the latest captured defaults:

```python
runs = gx.runs(gluex.RunSelection.runs([50685])).collect()
reconstruction = gluex.ReconstructionSelection.periods({
    gluex.RunPeriod.RP2018_08:
        gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2018_08, 2),
})
result = gx.workflows.luminosity(
    runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0]
).collect()
print(result.histograms.tagged_luminosity.counts)  # inverse picobarns
print(result.report.selected_runs, result.report.used_runs)
print(result.provenance.runs, result.provenance.rcdb_source, result.provenance.ccdb_source)
print(result.provenance.requested_reconstruction)
print(result.provenance.resolved_reconstruction)
print(result.provenance.calibration_default_as_of)
```

The default missing-input policy is strict. `report_missing()` instead excludes
only runs with genuinely absent scientific inputs and records the reason; malformed
payloads, schema failures, invalid selectors, and database errors always raise.
`fallback_to(run)` instead supplies the complete, valid luminosity inputs of one
explicitly chosen run whenever a selected run lacks required inputs. Each
`(selected_run, fallback_run)` replacement is retained in `report.substitutions`;
the result still identifies the selected run as used. A missing or malformed
fallback remains an error.
Missing or zero livetime is a missing scientific input, never an implicit 1.0
scale. `ReconstructionSelection.latest()` resolves at the captured CCDB opening
time, so delaying collection cannot change that default. Non-REST calibration
lookups use that same captured cutoff. Provenance retains both the requested
reconstruction selector and its resolved period mapping, distinguishing an
explicit `latest()` request from a period-specific override.
The result records procedure version `gluex-luminosity-v1`, its provisional
scientific-review status, the pair-production reference, and the explicit
RP2019-11 endpoint-constant exception. It also retains both source identities,
Run Set provenance, and the coherent-peak/polarized settings. Multi-period requests resolve each period
separately and aggregate per-run luminosity, including per-run target density.
The `gluex lumi` command uses this same workflow.

Rust uses `gx.workflows().luminosity(&runs, reconstruction, edges).collect()?`.

## Cancellation, timeouts, and caches

Raw readers accept `timeout=` in seconds. Run, condition, and calibration queries
provide immutable `.timeout(seconds)` transformations; Rust uses
`with_timeout(Duration)`. Database execution releases the Python GIL and polls
Python signals, so `KeyboardInterrupt` stops eligible SQLite work. Interrupted
operations return no completed report and release their cursor; the same reader
can be used again. Rust callers may also attach a shareable `CancellationToken`.

`gx.cache_info` exposes the per-stream decoded calibration payload capacity and
current disposable CCDB metadata occupancy. Use
`gx.set_calibration_payload_cache_capacity(n)` to bound shared-payload streaming
(the minimum is one) and `gx.clear_caches()` to release variation, inheritance,
and decoded column-layout caches. Catalog definitions and collected immutable
results remain valid. Rust exposes the corresponding methods on `GlueX`.

For callers moving from the pre-workflow API, see the
[database and luminosity migration guide](database-migration.md).

Maintainers investigating database or luminosity performance can use the
[fixture-backed local workload matrix](database-workloads.md). Its generated
measurements remain ignored local artifacts and are not ordinary CI gates.
