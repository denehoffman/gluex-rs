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
use gluex_lumi::{Luminosity, LuminosityContext, RESTVersionSelection, RunPeriod};
use std::collections::HashMap;

fn main() -> Result<(), gluex_lumi::LuminosityError> {
    let mut rest_version = HashMap::new();
    rest_version.insert(RunPeriod::RP2018_08, RESTVersionSelection::Current); // uses current timestamp rather than REST version
    let runs: Vec<_> = RunPeriod::RP2018_08.iter_runs().collect();
    let edges: Vec<f64> = (0..=20).map(|i| 7.5 + 0.05 * i as f64).collect();
    let ctx = LuminosityContext::new(runs, rest_version)?.with_coherent_peak(true);
    let lumi = Luminosity::new("/path/to/rcdb.sqlite", "/path/to/ccdb.sqlite");
    let flux = lumi.fetch(&edges, &ctx)?;
    println!("Tagged luminosity in pb^{-1}: {:?}", flux.tagged_luminosity.counts);
    Ok(())
}
```

## License

Dual-licensed under Apache-2.0 or MIT.
