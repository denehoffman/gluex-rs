# gluex-rcdb

Rust bindings for the GlueX Run Condition Database (RCDB). This crate provides a simple interface for loading run conditions from RCDB which match the given context (run numbers and filters). It also provides common aliases used to determine production data.

## Installation

```bash
cargo add gluex-rcdb
```

## Example

```rust
use gluex_core::run_periods::RunPeriod;
use gluex_rcdb::{
    conditions,
    prelude::{Context, RCDB},
};

fn main() -> gluex_rcdb::RCDBResult<()> {
    let rcdb = RCDB::open("/path/to/rcdb.sqlite")?;
    let filters = conditions::aliases::approved_production(RunPeriod::RP2018_08);
    let ctx = Context::default().with_run_range(55_000..=55_050).filter(filters);
    let rows = rcdb.fetch(["polarization_angle", "polarization_direction"], &ctx)?;

    for (run, values) in rows {
        if let Some(angle) = values
            .get("polarization_angle")
            .and_then(|v| v.as_float())
        {
            println!("run {run}: angle = {angle:.2}");
        }
    }
    Ok(())
}
```

## License

Dual-licensed under Apache-2.0 or MIT.
