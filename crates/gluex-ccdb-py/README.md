# gluex-ccdb (Python)

Python bindings around the `gluex-ccdb` Rust crate. They expose lightweight wrappers for CCDB
directories, tables, and columnar payloads without compromising type information.

## Installation

```bash
uv pip install gluex-ccdb
```

## Example

```python
import gluex_ccdb as ccdb

client = ccdb.CCDB("/data/ccdb.sqlite")
tables = client.fetch("/PHOTON_BEAM/pair_spectrometer/lumi/trig_live", runs=[55_000, 55_005])

for run, dataset in tables.items():
    print(f"columns: {dataset.column_names()}")
    livetime = float(dataset.column(1).row(0))
    print(f"run {run}: livetime = {livetime:.3f}")
```

## License

Dual-licensed under Apache-2.0 or MIT.
