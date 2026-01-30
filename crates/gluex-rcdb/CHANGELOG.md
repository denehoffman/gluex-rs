# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.8](https://github.com/denehoffman/gluex-rs/compare/gluex-rcdb-v0.1.7...gluex-rcdb-v0.1.8) (2026-01-30)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * gluex-core bumped from 0.1.7 to 0.1.8

## [0.1.7](https://github.com/denehoffman/gluex-rs/compare/gluex-rcdb-v0.1.6...gluex-rcdb-v0.1.7) (2026-01-28)


### Bug Fixes

* Change cargo layout to hopefully force dependency syncing ([ebb8163](https://github.com/denehoffman/gluex-rs/commit/ebb8163dc40962aea84b88924de0c09e78938d8c))
* **gluex-ccdb-py:** Release gluex-ccdb-py-v0.1.5 ([a0cfa95](https://github.com/denehoffman/gluex-rs/commit/a0cfa95edf2e6e5c43496f9e424783238799c5ec))
* **gluex-ccdb:** Release gluex-ccdb-v0.1.5 ([c7f1245](https://github.com/denehoffman/gluex-rs/commit/c7f124526ee1ccc5cd8b99f8c847e72ed7a42852))
* **gluex-lumi-py:** Release gluex-lumi-py-v0.1.7 ([c74ab0b](https://github.com/denehoffman/gluex-rs/commit/c74ab0be7254e558bae21149ec025338cfecba6b))
* **gluex-lumi:** Release gluex-lumi-v0.1.7 ([c61aac8](https://github.com/denehoffman/gluex-rs/commit/c61aac898ed66d068dd13ba7c258c2ae0cedae5a))
* **gluex-rcdb-py:** Release gluex-rcdb-py-v0.1.7 ([6649f15](https://github.com/denehoffman/gluex-rs/commit/6649f15ded268475e8822f303f3319f5802c16de))
* **gluex-rcdb:** Release gluex-rcdb-v0.1.7 ([a4acfb0](https://github.com/denehoffman/gluex-rs/commit/a4acfb026faf8faa6d5b85f4377322a19e5602e8))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * gluex-core bumped from 0.1.3 to 0.1.7

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
