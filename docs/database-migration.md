# Database and luminosity migration

The unified API intentionally replaces the path-owning luminosity calculator and
ad-hoc database contexts with one `GlueX` session, immutable queries, and retained
provenance.

## Python

Replace `gluex.lumi.Luminosity(...).fetch(edges, runs=..., rest_version=...)` with:

```python
gx = gluex.connect(rcdb="rcdb.sqlite", ccdb="ccdb.sqlite")
runs = gx.runs.select(50685).collect()
f18 = gluex.RunPeriod("f18")
result = gx.luminosity(
    runs, reconstruction=f18.rest(2), edges=[8.0, 8.5, 9.0]
).compute()
histograms = result.histograms
```

The behavioral changes are deliberate:

- `gx.runs` is the discoverable run-domain facade; use `gx.runs.select(scope)`,
  `gx.runs.between(start, end)`, and `gx.runs.conditions`;
- construct run-period value objects from familiar case-insensitive names, such
  as `s17 = RunPeriod("s17")`, then use `s17.rest(5)` for reconstruction;
- named predicates now have one preferred home under `gx.runs.aliases`; replace
  root `gluex.approved_production(period)` with
  `gx.runs.aliases.approved_production(period)`;
- project conditions with `query.columns("name", ...)`; the former callable
  runs facade, root `gx.conditions`, and list-taking `query.select(...)` forms
  have been removed;

- `gx.luminosity(...)` is the concise path for the canonical derived quantity;
  `gx.workflows.luminosity(...)` remains the full discovery path;
- use `.compute()` for derived workflows; the earlier `.collect()` terminal has
  been removed so database retrieval and derived-quantity execution read differently;
- `gx.operations` reserves the discoverable home for future experiment artifact
  operations; it intentionally provides no conversion machinery yet;
- replace inclusive `RunSelection.range(start, end)` with
  `RunSelection.between(start, end)`; ordinary Python `range` inputs retain
  Python's exclusive stop semantics;

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

### RF factors, polarization conditions, and four-period luminosity

The direct replacement for the common S17/S18/F18/S20 setup is:

```python
import polars as pl

PERIODS = tuple(gluex.RunPeriod(name) for name in ("s17", "s18", "f18", "s20"))
# Keep this analysis-owned choice explicit; these are examples, not defaults.
REST_VERSIONS = {"S17": 5, "S18": 2, "F18": 2, "S20": 1}
calibrated_periods = tuple(
    period.rest(REST_VERSIONS[period.short_name]) for period in PERIODS
)
reconstruction = gluex.ReconstructionSelection.periods(*calibrated_periods)

# This confirms the important non-default REST-v05 context before doing I/O.
assert reconstruction.resolve("S17")[0] == "recon_2017_01_ver05"

calibrations = gx.calibrations.select(*calibrated_periods).tables(
    "/ANALYSIS/accidental_scaling_factor",
).collect()
rf = calibrations["/ANALYSIS/accidental_scaling_factor"].to_polars().rename({
    "HODOSCOPE_HI_FACTOR": "hodoscope_hi",
    "HODOSCOPE_LO_FACTOR": "hodoscope_lo",
    "MICROSCOPE_FACTOR": "microscope",
    "MICROSCOPE_ENERGY_HI": "microscope_energy_hi",
    "MICROSCOPE_ENERGY_LO": "microscope_energy_lo",
})

polarization_rows = []
for period in PERIODS:
    selected = gx.runs.select(period).where(
        gx.runs.aliases.approved_production(period)
        & gx.runs.aliases.is_coherent_beam
    )
    values = selected.columns("polarization_angle").collect().to_polars()
    polarization_rows.append(values.with_columns(
        run_period=pl.lit(period.short_name),
    ))

runs = gx.runs.select(run_numbers).collect()
result = gx.luminosity(
    runs, reconstruction=reconstruction, edges=[8.0, 9.0]
).coherent_peak().polarized().compute()
value = float(result.histograms.tagged_luminosity.counts[0])
error = float(result.histograms.tagged_luminosity.errors[0])
```

The reconstruction mapping is resolved per run period. In particular, S17 REST
v05 supplies both its catalog timestamp and the `recon_2017_01_ver05` CCDB
variation; callers must not separately guess or duplicate that variation.
When a deliberate override is required, use
`gluex.RunPeriod("s17").rest(5, variation="name")`; it retains the REST
timestamp and replaces only the resolved CCDB variation.
For calibration work not tied to a REST production, use
`gluex.RunPeriod("s17").at(timestamp, variation="name")`.
Calibration queries use numeric period bounds and therefore do not require RCDB.
Polarization selection does require RCDB because approval, coherent-beam state,
and `polarization_angle` are recorded conditions. The polarization-magnitude
histograms in the original example are analysis input files rather than RCDB
conditions; join those JSON bins to `polarization_rows` as before.

Coherent-peak energy bounds are still represented by the project run-period
table. Luminosity provenance records this as a validation gap because an
authoritative database field has not yet been identified. REST metadata remains
the deliberately maintained external catalog. Moving peak bounds behind a data
source should happen only after identifying the authoritative RCDB/CCDB record;
silently deriving them from unrelated endpoint constants would change the
scientific meaning.

`RunPeriod` and `CalibratedRunPeriod` are intentionally separate types. The
former is only a data-taking/run scope. Calling `.rest(...)` validates the REST
version and returns the latter, which is accepted wherever reconstruction is
required. A configured period passed to `gx.calibrations.select(...)` supplies
both scope and calibration context. For a uniform custom context over numeric
scopes, use `select(runs, variation="name", as_of=timestamp)`. These keywords
cannot be combined with a calibrated period, so there is no precedence rule to
memorize.

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
