# gluex-rs

`gluex-rs` is one library for GlueX analysis utilities. The same Cargo package
provides:

- the `gluex_rs` Rust library;
- the `gluex` PyO3 extension module;
- the `gluex` command-line executable.

## Organization

| Module | Purpose |
| --- | --- |
| `core` | Particles, detector metadata, run periods, constants, and parsers |
| `ccdb` | Typed, read-only Calibration and Conditions Database access |
| `rcdb` | Run Conditions Database access and composable run predicates |
| `lumi` | Photon-flux and tagged-luminosity calculations |
| `generation` | laddu 0.20 channels, event sinks, and GlueX HDDM output |
| `cli` | Shared implementation of the native and Python console commands |

The most common core types are also re-exported from the crate root.

## Rust

```bash
cargo add gluex-rs
```

```rust
use gluex_rs::{
    RunPeriod,
    ccdb::{CCDB, CCDBContext},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ccdb = CCDB::new()?;
    let data = ccdb.fetch(
        "/PHOTON_BEAM/endpoint_energy",
        &CCDBContext::default().with_run(RunPeriod::RP2018_08.min_run()),
    )?;
    println!("{data:#?}");
    Ok(())
}
```

## Python

Maturin builds the Python module directly from the unified crate:

```bash
uvx maturin develop --uv --generate-stubs
```

```python
import gluex

period = gluex.RunPeriod("f18")
print(period.min_run, period.max_run)
```

No Python API wrappers or handwritten `.pyi` files are maintained. Builds use
PyO3 introspection and Maturin stub generation, so the installed package
contains type information generated from the Rust API.

laddu datasets can be streamed to HDDM through `gluex.generation`:

```python
from gluex import generation

config = generation.GlueXHddmConfig(
    channel,
    beam="beam",
    target="target",
    run_number=90_000,
)
generation.GlueXHddmWriter(config).write(dataset, "events.hddm")
```

## Command line

The native executable and Python console script share the same implementation:

```bash
gluex lumi --run f18=2 --rcdb rcdb.sqlite --ccdb ccdb.sqlite
gluex info rest f18
gluex info runs f18
```

## Development

```bash
just test
just lint
```

GitHub workflows are generated exclusively from `.yamloom.py`.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT).
