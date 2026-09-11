# Runs and RCDB conditions

Run selection has two stages: define a numeric scope, then optionally evaluate RCDB predicates inside that scope.

## Select a period or explicit runs

```python
period_runs = gx.runs.select("2018-08")
explicit_runs = gx.runs.select([50685, 50697])
several_periods = gx.runs.select(
    [gluex.RunPeriod("2018-01"), gluex.RunPeriod("2018_08")]
)

print(period_runs)          # no database read yet
result = period_runs.collect()
print(result.numbers)
print(result.report)
```

A `RunSet` contains only runs recorded in RCDB. The report accounts for requested numbers that were not recorded.

See [run periods](run-periods.md) for accepted spellings and current numeric ranges.

## Filter with typed conditions

```python
conditions = gx.runs.conditions
current = conditions["beam_current"]
direction = conditions["polarization_direction"]

query = (
    gx.runs.select("2018-08")
    .where(
        gx.runs.aliases.is_coherent_beam
        & (current > 10.0)
        & direction.isin(["PARA", "PERP"])
    )
)
runs = query.collect()
```

Condition definitions come from the opened RCDB rather than a fixed list in the package. Operators validate against the condition type. Missing condition values are neither true nor false; the result report records them as unknown unless the query's missing-data policy says otherwise.

`gx.runs.aliases` is the discoverable home for RCDB's named run cuts. Its members produce the same predicate type as condition comparisons, so aliases and custom cuts compose directly inside `.where(...)`. `approved_production` inspects each run number and applies the definition for that run's period, allowing one query to span periods; runs in periods without a defined approved-production cut do not match. The lower-level `gluex.rcdb` namespace remains available for advanced source-specific work, but ordinary selection does not require it.

## Project condition values

```python
values = (
    gx.runs.select("2018-08")
    .columns("event_count", "polarization_angle")
    .collect()
)

print(values.runs.numbers)
print(values.column("event_count"))
frame = values.to_polars()
```

Projection preserves run order and reports missing cells instead of inventing defaults.

## Preserve calibration choices

Attach a REST version or direct calibration timestamp to a run query before collecting it:

```python
special = gx.runs.select([50685]).rest(1, variation="default")
standard = gx.runs.select([50697]).rest(2)
runs = gx.runs.select([special, standard]).collect()
```

The collected `RunSet` retains these per-run contexts. Passing it to `gx.calibrations.select(runs)` applies the correct context to each surviving run automatically.

## Stream large selections

```python
for chunk in query.stream(chunk_size=256):
    process(chunk.numbers)
```

Streaming bounds result memory. Each chunk carries its own report, and the final chunk indicates completion.

## Raw RCDB reads

Use typed queries for analysis logic. For schema inspection or a query the typed layer does not express:

```python
rows = gx.sources.rcdb.raw(
    "SELECT number FROM runs WHERE number BETWEEN ? AND ? ORDER BY number",
    parameters=[50000, 50010],
)
```

Raw access is read-only, accepts positional parameters, and rejects multiple or mutating statements.
