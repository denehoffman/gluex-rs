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
selection = gluex.RunSelection.runs([50685, 50697])
results = (
    gx.calibrations.select(selection)
    .tables("/TARGET/density", "/PHOTON_BEAM/endpoint_energy")
    .collect()
)

density = results["/TARGET/density"]
for run, entry in density.items():
    print(run, entry.assignment_id, entry.payload.column("density"))
```

The resolver applies CCDB run ranges, variation inheritance, and assignment timestamps. A broad assignment can supply a period that has no period-specific row.

## Select variation and time

```python
from datetime import datetime, timezone

historical = (
    gx.calibrations.select(
        selection,
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
reconstruction = gluex.ReconstructionSelection.periods({period: period.rest(2)})
```

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
