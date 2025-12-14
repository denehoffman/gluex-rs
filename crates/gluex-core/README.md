# gluex-core

Foundational types shared by the GlueX crate ecosystem, including histogram utilities, physics
constants, REST metadata, and helpers for parsing run-period information.

## Installation

```bash
cargo add gluex-core
```

## Example

```rust
use gluex_core::{
    histograms::Histogram,
    run_periods::{coherent_peak, RunPeriod},
};

fn main() {
    let edges = vec![8.0, 8.2, 8.4, 8.6];
    let hist = Histogram::empty(&edges);
    let (peak_low, peak_high) = coherent_peak(55_000);
    println!(
        "{} runs from {} to {} with a {:.1}-{:.1} GeV coherent peak range",
        RunPeriod::RP2018_08.short_name(),
        RunPeriod::RP2018_08.min_run(),
        RunPeriod::RP2018_08.max_run(),
        peak_low,
        peak_high
    );
    println!("Histogram bins: {}", hist.bins());
}
```

## License

Dual-licensed under Apache-2.0 or MIT.
