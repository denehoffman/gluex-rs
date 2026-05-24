# gluex-rs



## Workspace members

| Package | Language | Summary |
| --- | --- | --- |
| [`gluex-core`](crates/gluex-core) | Rust | Shared physics constants, run-period metadata, histogram helpers, and serialization primitives. |
| [`gluex-ccdb`](crates/gluex-ccdb) | Rust | Read-only CCDB client with typed column accessors and caching. |
| [`gluex-rcdb`](crates/gluex-rcdb) | Rust | RCDB query layer with expression builders for run selection. |
| [`gluex-lumi`](crates/gluex-lumi) | Rust | Luminosity calculators that combine CCDB and RCDB payloads. |
| [`gluex-rs`](crates/gluex-rs) | Rust | Main crate re-exporting the GlueX APIs, HDDM generation support, and the `gluex` CLI. |
| [`gluex-rs` (python)](crates/gluex-rs-py) | Python (PyO3) | Unified `gluex` package and console command exposing core, CCDB, RCDB, and luminosity APIs. |

## Command Line

The `gluex` executable is owned by the facade crate and by the unified Python
distribution. Luminosity output is JSON for downstream analysis:

```bash
gluex lumi --run f18=2 --rcdb rcdb.sqlite --ccdb ccdb.sqlite > luminosity.json
```

REST-version and run-period metadata are available independently of the
luminosity calculator:

```bash
gluex info rest f18
gluex info runs f18
```

## License

Unless noted otherwise, every crate and Python package in this repository is available under a dual
[Apache-2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT) license.
option.
