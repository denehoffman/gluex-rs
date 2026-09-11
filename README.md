# gluex-rs

`gluex-rs` is a read-only toolkit for working with GlueX run metadata, calibration constants, photon flux, luminosity, and standalone event generation. It provides the same core model through Rust, Python, and the `gluex` command-line program.

If you already know RCDB and CCDB, the central idea is straightforward: open both SQLite snapshots once, build lazy typed queries, and collect immutable results that retain their run selection, calibration time, variation, omissions, and source paths.

## Start here

- [Install and open the databases](docs/getting-started.md)
- [Select runs and conditions](docs/runs.md)
- [Read CCDB calibrations](docs/calibrations.md)
- [Calculate photon flux and luminosity](docs/luminosity.md)
- [Understand run periods and coherent-energy tables](docs/run-periods.md)
- [Use the command line](docs/cli.md)
- [Generate standalone HDDM events](docs/generation.md)

## Ten-line Python example

```python
import gluex

gx = gluex.connect(rcdb="rcdb.sqlite", ccdb="ccdb.sqlite")
beam_current = gx.runs.conditions["beam_current"]
runs = (
    gx.runs.select("2018-08")
    .where(beam_current > 10.0)
    .collect()
)
print(runs.numbers)
print(runs.provenance)
```

Database files are opened read-only. `RCDB_CONNECTION` and `CCDB_CONNECTION` may be used instead of explicit paths.

## Ten-line Rust example

```rust,no_run
use gluex_rs::{GlueX, RunPeriod, RunSelection, SourceConfig};

let gx = GlueX::open(
    SourceConfig::sqlite("rcdb.sqlite"),
    SourceConfig::sqlite("ccdb.sqlite"),
)?;
let runs = gx
    .runs(RunSelection::period(RunPeriod::RP2018_08))?
    .collect()?;
println!("{:?}", runs.numbers());
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Scope

The library does not modify RCDB or CCDB. It currently supports local SQLite snapshots, captures the database opening time as the default CCDB cutoff, and makes network refresh an explicit operation outside query execution.

Rust API reference is generated with `cargo doc --open`. The crate requires Rust 1.97.1 or newer; the Python package requires Python 3.11 or newer.

Licensed under Apache-2.0 or MIT.
