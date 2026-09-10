# Database and luminosity migration

The unified API intentionally replaces the path-owning luminosity calculator and
ad-hoc database contexts with one `GlueX` session, immutable queries, and retained
provenance.

## Python

Replace `gluex.lumi.Luminosity(...).fetch(edges, runs=..., rest_version=...)` with:

```python
gx = gluex.open(rcdb="rcdb.sqlite", ccdb="ccdb.sqlite")
runs = gx.runs.select(50685).collect()
reconstruction = gluex.ReconstructionSelection.periods({
    gluex.RunPeriod.RP2018_08:
        gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2018_08, 2),
})
result = gx.workflows.luminosity(
    runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0]
).collect()
histograms = result.histograms
```

The behavioral changes are deliberate:

- `gx.runs` is the discoverable run-domain facade; use `gx.runs.select(scope)`,
  `gx.runs.between(start, end)`, and `gx.runs.conditions`;
- project conditions with `query.columns("name", ...)`; the callable runs facade,
  root `gx.conditions`, and list-taking `query.select(...)` remain temporary
  migration forms;

- the supplied `RunSet` is authoritative; no approved-production cut is hidden;
- reconstruction is explicit and resolved per represented run period;
- `latest()` is fixed to the CCDB source-opening time;
- missing required inputs are strict by default, or reported with
  `.report_missing()`; `.fallback_to(run)` applies and records an explicit
  selected-run to fallback-run substitution;
- absent or zero livetime is not silently replaced by 1.0;
- generic Python runtime/value failures are replaced by public capability,
  configuration, query, decoding, missing-data, timeout, and cancellation classes;
- multi-run luminosity applies each run's own target density before aggregation;
- results retain selected/used/excluded runs, source-bound run provenance,
  both database source identities, the requested and resolved reconstruction,
  the source-opening calibration cutoff, luminosity settings, procedure version,
  canonical status, references, assumptions, exceptions, and explicit scientific
  validation gaps.

Raw reads remain available through `gx.sources.rcdb.raw(...)` and
`gx.sources.ccdb.raw(...)`. Run conditions and calibrations should use the typed
catalog/query APIs for routine analysis.

## Rust

Replace `lumi::Luminosity` and `LuminosityContext` with `GlueX::workflows`:

```rust
let gx = GlueX::open(SourceConfig::sqlite(rcdb), SourceConfig::sqlite(ccdb))?;
let runs = gx.runs(RunSelection::runs([50_685]))?.collect()?;
let reconstruction = ReconstructionSelection::periods([(
    RunPeriod::RP2018_08,
    RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2)?,
)]);
let result = gx
    .workflows()
    .luminosity(&runs, reconstruction, [8.0, 8.5, 9.0])
    .collect()?;
```

The former path-owning calculator and context are no longer public. Backend-native
readers remain available under `gx.sources()` for advanced read-only access.
