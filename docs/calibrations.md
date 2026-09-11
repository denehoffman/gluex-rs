# CCDB calibrations

CCDB tables are addressed by canonical absolute paths. Numeric `typeTables.id` values are database metadata, not stable application identifiers.

## Discover tables

```python
table = gx.calibrations["/TARGET/density"]
print(table.path)
print([(column.name, column.value_type) for column in table.columns])
```

Catalog discovery loads metadata, not constant payloads.

## Read one or more tables

```python
results = (
    gx.calibrations.select([50685, 50697])
    .tables("/TARGET/density", "/PHOTON_BEAM/endpoint_energy")
    .collect()
)

density = results["/TARGET/density"]
for run, entry in density.items():
    print(run, entry.assignment_id, entry.payload.column("density"))
```

The resolver applies CCDB run ranges, variation inheritance, and assignment timestamps. A broad assignment can supply a period that has no period-specific row. When a run falls after a calibration's range and before its next replacement, the latest preceding assignment carries forward; a result is null only when no eligible earlier assignment exists.

## Select variation and time

```python
from datetime import datetime, timezone

historical = (
    gx.calibrations.select(
        [50685, 50697],
        variation="default",
        as_of=datetime(2022, 1, 1, tzinfo=timezone.utc),
    )
    .tables("/TARGET/density")
    .collect()
)
```

When `as_of` is omitted, the session opening time is used. Reconstruction selections instead resolve the variation and calibration timestamp recorded for a REST version.

## Reconstruction-aware selection

```python
period = gluex.RunPeriod("2018-08")
reconstruction = period.rest(2)
```

Calibration contexts survive RCDB filtering. This allows a collected `RunSet` to carry different REST versions or variations for different runs, including runs in the same period:

```python
old = gx.runs.select([50685]).rest(1)
new = gx.runs.select([50697]).rest(2, variation="default")

runs = (
    gx.runs.select([old, new])
    .where(gx.runs.aliases.approved_production)
    .collect()
)
results = gx.calibrations.select(runs).tables("/TARGET/density").collect()
```

Only runs that survive the RCDB predicate retain a context. Overlapping scopes with different contexts are rejected when the `RunSet` is used for calibration retrieval. Runs with no explicit context use the session default. Passing a global `variation` or `as_of` together with inherited contexts is rejected rather than silently overriding them.

The bundled REST catalog maps `(run period, REST revision)` to the recorded CCDB variation and timestamp. Refresh it from the Hall-D data-version service with:

```console
python scripts/sync_rest_versions.py
```

The refresh is deterministic: duplicate revisions keep the newest calibration timestamp, and the generated TSV is sorted.

## Coherent-energy convenience query

```python
window = gx.sources.ccdb.coherent_peak(140000)
assert window == (1.0, 1.2)
```

This resolves `/PHOTON_BEAM/coherent_energy` by path and applies normal CCDB assignment rules. See [run periods](run-periods.md) for the distinction from the beam-asymmetry table.

## Convert several tables to Polars

```python
frame = results.to_polars()
```

The frame has one row per run and one struct column per CCDB path. A table whose payload has exactly one row is flattened to nullable scalar struct fields. Tables with genuinely repeated rows retain list-valued fields, preserving their row structure without guessing.
