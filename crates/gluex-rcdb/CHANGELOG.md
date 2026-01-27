# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.6](https://github.com/denehoffman/gluex-rs/compare/gluex-rcdb-v0.1.5...gluex-rcdb-v0.1.6) (2026-01-27)

## [0.1.5](https://github.com/denehoffman/gluex-rs/compare/gluex-rcdb-v0.1.4...gluex-rcdb-v0.1.5) (2026-01-22)


### Features

* Add run period arguments to fetch and fix aliases type hinting ([91e128b](https://github.com/denehoffman/gluex-rs/commit/91e128b059c385f3d6f2e7951ae1107144b141e1))
* First draft of RCDB function, move some constants into gluex-core ([dfda19d](https://github.com/denehoffman/gluex-rs/commit/dfda19d5c7a747562d931f73663d858799bf7c87))
* First full impl of gluex-lumi, but it's slow due to RCDB, and gluex-ccdb-py won't build ([7b07372](https://github.com/denehoffman/gluex-rs/commit/7b07372d9c43537baf51cb71691fabb973bea21f))
* **rcdb:** First draft of RCDB python interface ([a3da761](https://github.com/denehoffman/gluex-rs/commit/a3da761250b62b71221e9f2493f734e172391ed9))
* Release-ready I hope ([aebbf2d](https://github.com/denehoffman/gluex-rs/commit/aebbf2d481f273caaf8987efb55aab72706131a4))
* Restructure crates a bit and add RCDB skeleton crate ([8f1ba69](https://github.com/denehoffman/gluex-rs/commit/8f1ba698b240ac20b2a624d905d8bb820b6a76a6))
* Separate Python crates, add lots of clippy lints, add precommit, and a few other small API changes ([d4de1b6](https://github.com/denehoffman/gluex-rs/commit/d4de1b6a39571d0bc58c769af6514a7c63f49c30))


### Performance Improvements

* **gluex-rcdb:** Benchmark and force run-number index ([a439456](https://github.com/denehoffman/gluex-rs/commit/a43945639bf158c29819eeefe240e0d42df3681f))

## [Unreleased]

## [0.1.3](https://github.com/denehoffman/gluex-rs/compare/gluex-rcdb-v0.1.2...gluex-rcdb-v0.1.3) - 2026-01-21

### Other

- update Cargo.toml dependencies

## [0.1.2](https://github.com/denehoffman/gluex-rs/compare/gluex-rcdb-v0.1.1...gluex-rcdb-v0.1.2) - 2025-12-18

### Added

- add run period arguments to fetch and fix aliases type hinting

## [0.1.1](https://github.com/denehoffman/gluex-rs/compare/gluex-rcdb-v0.1.0...gluex-rcdb-v0.1.1) - 2025-12-15

### Other

- release v0.1.0 ([#1](https://github.com/denehoffman/gluex-rs/pull/1))

## [0.1.0](https://github.com/denehoffman/gluex-rs/releases/tag/gluex-rcdb-v0.1.0) - 2025-12-14

### Added

- release-ready I hope
- first full impl of gluex-lumi, but it's slow due to RCDB, and gluex-ccdb-py won't build
- separate Python crates, add lots of clippy lints, add precommit, and a few other small API changes
- *(rcdb)* first draft of RCDB python interface
- first draft of RCDB function, move some constants into gluex-core
- restructure crates a bit and add RCDB skeleton crate

### Other

- *(gluex-rcdb)* benchmark and force run-number index
- *(gluex-rcdb)* add rcdb fetch benchmark
