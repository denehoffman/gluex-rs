# gluex-rcdb (Python)

Python bindings for the `gluex-rcdb` crate. This library provides a simple interface for loading run conditions from RCDB which match the given context (run numbers and filters). It also provides common aliases used to determine production data.

## Installation

```bash
uv pip install gluex-rcdb
```

## Example

```python
import gluex_rcdb as rcdb

client = rcdb.RCDB("/data/rcdb.sqlite")
filters = rcdb.all(
    rcdb.float_cond("polarization_angle").gt(90.0),
    rcdb.aliases.is_production,
)
run_list = client.fetch_runs(run_min=55_000, run_max=55_020, filters=filters)
values = client.fetch(["polarization_direction", "polarization_angle"], runs=run_list)
for run, payload in values.items():
    print(run, float(payload["polarization_direction"]))
```

## License

Dual-licensed under Apache-2.0 or MIT.
