# gluex-ccdb

Typed Rust bindings for the GlueX Calibration and Conditions Database (CCDB). The library performs
read-only queries against SQLite snapshots, caches table metadata, and exposes ergonomic accessors
for run-dependent payloads.

## Installation

```bash
cargo add gluex-ccdb
```

## Example

```rust
use gluex_ccdb::prelude::*;

fn main() -> CCDBResult<()> {
    let ccdb = CCDB::open("/path/to/ccdb.sqlite")?;
    let ctx = Context::default().with_run_range(55_000..=55_010);
    let tables = ccdb.fetch("/PHOTON_BEAM/pair_spectrometer/lumi/trig_live", &ctx)?;

    for (run, dataset) in tables {
        if let Some(livetime) = dataset.double(1, 0) {
            println!("run {run}: livetime = {livetime:.3}");
        }
    }
    Ok(())
}
```

## License

Dual-licensed under Apache-2.0 or MIT.
