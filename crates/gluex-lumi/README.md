# gluex-lumi

Luminosity calculators for GlueX analyses. This crate can take a set of runs (optionally selecting a REST version for each run period) and produce histogram distributions of luminosity and flux in the hodoscope/microscope. It ships with a CLI that has similar inputs but prints JSON data for the histograms to stdout to be read by other tools (plotters, etc.).

## Installation

Add to an existing Rust project:

```bash
cargo add gluex-lumi
```

or install as a CLI tool:

```bash
cargo install gluex-lumi
```

## Example

```rust
use gluex_core::run_periods::RunPeriod;
use gluex_lumi::{get_flux_histograms, RestSelection};
use std::collections::HashMap;

fn main() -> Result<(), gluex_lumi::GlueXLumiError> {
    let mut selection = HashMap::new();
    selection.insert(RunPeriod::RP2018_08, RestSelection::Current); // uses current timestamp rather than REST version
    let edges: Vec<f64> = (0..=20).map(|i| 7.5 + 0.05 * i as f64).collect();
    let flux = get_flux_histograms(
        selection,
        &edges,
        true, // coherent peak only
        false, // false -> include AMO runs
        "/path/to/rcdb.sqlite",
        "/path/to/ccdb.sqlite",
        None,
    )?;
    println!("Tagged luminosity in pb^{-1}: {:?}", flux.tagged_luminosity.counts);
    Ok(())
}
```

## License

Dual-licensed under Apache-2.0 or MIT.
