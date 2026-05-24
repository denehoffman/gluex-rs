# gluex-rs



## Workspace members

| Package | Language | Summary |
| --- | --- | --- |
| [`gluex-core`](crates/gluex-core) | Rust | Shared physics constants, run-period metadata, histogram helpers, and serialization primitives. |
| [`gluex-ccdb`](crates/gluex-ccdb) | Rust | Read-only CCDB client with typed column accessors and caching. |
| [`gluex-rcdb`](crates/gluex-rcdb) | Rust | RCDB query layer with expression builders for run selection. |
| [`gluex-lumi`](crates/gluex-lumi) | Rust | Luminosity calculators that combine CCDB and RCDB payloads and expose a CLI. |
| [`gluex-rs`](crates/gluex-rs) | Rust | Main crate re-exporting the GlueX APIs and HDDM generation support. |
| [`gluex-rs` (python)](crates/gluex-rs-py) | Python (PyO3) | Unified `gluex` package exposing core, CCDB, RCDB, and luminosity APIs. |

## License

Unless noted otherwise, every crate and Python package in this repository is available under a dual
[Apache-2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT) license.
option.
