# gluex-rs



## Workspace members

| Package | Language | Summary |
| --- | --- | --- |
| [`gluex-core`](crates/gluex-core) | Rust | Shared physics constants, run-period metadata, histogram helpers, and serialization primitives. |
| [`gluex-ccdb`](crates/gluex-ccdb) | Rust | Read-only CCDB client with typed column accessors and caching. |
| [`gluex-rcdb`](crates/gluex-rcdb) | Rust | RCDB query layer with expression builders for run selection. |
| [`gluex-lumi`](crates/gluex-lumi) | Rust | Luminosity calculators that combine CCDB and RCDB payloads and expose a CLI. |
| [`gluex-ccdb` (python)](crates/gluex-ccdb-py) | Python (PyO3) | Python bindings for the CCDB client |
| [`gluex-rcdb` (python)](crates/gluex-rcdb-py) | Python (PyO3) | Python bindings for RCDB condition queries |
| [`gluex-lumi` (python)](crates/gluex-lumi-py) | Python (PyO3) | Python wrappers and CLI shim for the luminosity tools. |

## License

Unless noted otherwise, every crate and Python package in this repository is available under a dual
[Apache-2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT) license.
option.
