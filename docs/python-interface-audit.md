# Python interface audit

The supported high-level entry point is the session returned by `gluex.connect()`.
Routine analysis should stay on that object:

```python
gx.runs.select(*scopes).where(predicate).columns(*names)
gx.calibrations.select(*scopes).tables(*paths)
gx.luminosity(runs, reconstruction=selection, edges=edges)
```

The two database readers under `gx.sources` and the `gluex.rcdb` and
`gluex.ccdb` modules are the advanced, database-native layer. They intentionally
expose database records and raw reads; their types should not be mixed into the
ordinary session examples.

## Findings and decisions

| Area | Confusing alternatives | Canonical interface | Disposition |
| --- | --- | --- | --- |
| Session creation | `connect`, `open`, direct `GlueX()` | `gluex.connect()` | `open` and direct construction are compatibility-only and omitted from new examples |
| Run reads | facade `select`, `between`, and `RunSelection` constructors | `gx.runs.select(*scopes)` | value constructors remain for reusable/programmatic scopes; `between` is explicitly inclusive |
| Condition projection | historical query `select` and `columns` | `.columns(*names)` | only `columns` is public |
| Calibration reads | catalog table `for_runs` and selector-first reads | `gx.calibrations.select(*scopes).tables(*paths)` | table-first reads are legacy; catalog indexing remains useful for metadata discovery |
| Calibration context | calibrated scopes, query keywords, and later selector mutators | `RunPeriod.rest(...)` or `RunPeriod.at(...)` in the selected scope | the selector-first query has no variation/date mutators, so a scope cannot be overwritten later |
| Missing calibrations | constructor policy and fluent methods | `.strict()` or `.fallback_to(run)` | the selector-first query configures policy only through these methods |
| Aliases | root exports, high-level aliases, low-level RCDB aliases | `gx.runs.aliases` | `gluex.rcdb.aliases` remains only for low-level `rcdb.Expr` users; both adapt the same native definitions |
| Luminosity | `gx.luminosity` and `gx.workflows.luminosity` | `gx.luminosity(...)` | the root method is the documented common path; workflows remains an advanced discovery namespace |

Calibration context has one owner. A `CalibratedRunPeriod` carries an effective
timestamp and variation. A plain period or numeric scope uses the defaults
captured when the session opened. Supplying the same period twice with different
calibrated contexts fails before database execution. The selector-first query
does not expose `with_variation`, `as_of`, `with_reconstruction`, or
`latest_reconstruction`, so call order cannot change scientific meaning.

Exceptional runs do not require a new vocabulary. `RunSelection.at(...)` and
`RunSelection.rest(...)` return `CalibratedRunSelection`, while
`CalibratedRunPeriod.excluding(...)` carves explicit overrides out of a period.
Normalization assigns each run exactly one context: identical overlaps
deduplicate, conflicting overlaps fail with the corrective syntax in the error.

Multiple calibration tables return `CalibrationResults`. Exact absolute paths
are the keys and Polars column names, avoiding basename and payload-column
collisions. The Polars representation has one row per evaluated run and one
struct column per table. Each payload field is a list because a CCDB assignment
may contain more than one table row. A flat single-table view remains available
through `results[path].to_polars()`.

## Deliberate distinctions

Some similar-looking APIs represent real seams rather than duplication:

- `gx.runs.aliases` returns high-level `RunPredicate` values, while
  `gluex.rcdb.aliases` returns database-native expressions.
- `gx.sources.rcdb` and `gx.sources.ccdb` expose advanced source operations;
  `gx.runs` and `gx.calibrations` own experiment-level selection and results.
- `RunSelection` is a reusable, database-independent value. `gx.runs.select`
  binds that value to a session and creates a lazy query.
- Condition results are scalar columns; calibration results contain tabular
  payloads. Their query grammar is parallel, but their result shapes remain
  distinct.

## Remaining removal boundary

The old `CalibrationTable.for_runs` surface is retained temporarily for source
compatibility and the flat `CalibrationSeries` view. New documentation and
examples do not use it. Its selector keyword arguments and the matching
`CalibrationQuery` selector mutators are the principal remaining duplicate and
should be removed together at the next intentional breaking-version boundary,
after downstream code has moved to selector-first queries. Keeping only one of
those halves would preserve the same precedence ambiguity in a different form.
