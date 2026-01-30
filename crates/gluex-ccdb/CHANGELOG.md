# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.8](https://github.com/denehoffman/gluex-rs/compare/gluex-ccdb-v0.1.7...gluex-ccdb-v0.1.8) (2026-01-30)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * gluex-core bumped from 0.1.7 to 0.1.8

## [0.1.7](https://github.com/denehoffman/gluex-rs/compare/gluex-ccdb-v0.1.4...gluex-ccdb-v0.1.7) (2026-01-28)


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

## [0.1.4](https://github.com/denehoffman/gluex-rs/compare/gluex-ccdb-v0.1.3...gluex-ccdb-v0.1.4) (2026-01-27)

## [0.1.3](https://github.com/denehoffman/gluex-rs/compare/gluex-ccdb-v0.1.2...gluex-ccdb-v0.1.3) (2026-01-22)

## [Unreleased]

## [0.1.2](https://github.com/denehoffman/gluex-rs/compare/gluex-ccdb-v0.1.1...gluex-ccdb-v0.1.2) - 2026-01-21

### Other

- update Cargo.toml dependencies

## [0.1.1](https://github.com/denehoffman/gluex-rs/compare/gluex-ccdb-v0.1.0...gluex-ccdb-v0.1.1) - 2025-12-15

### Other

- release v0.1.0 ([#1](https://github.com/denehoffman/gluex-rs/pull/1))

## [0.1.0](https://github.com/denehoffman/gluex-rs/releases/tag/gluex-ccdb-v0.1.0) - 2025-12-14

### Added

- release-ready I hope
- first full impl of gluex-lumi, but it's slow due to RCDB, and gluex-ccdb-py won't build
- separate Python crates, add lots of clippy lints, add precommit, and a few other small API changes
- *(rcdb)* first draft of RCDB python interface
- first draft of RCDB function, move some constants into gluex-core
- restructure crates a bit and add RCDB skeleton crate
- add python interface, rename Database to CCDB, and add a lot of helpers/alternate methods. rename subdir(s) to dir(s)
- add prelude, use CCDBResult alias, and add column_types to RowView and column iterators

### Fixed

- add tests and found flipped column/row arguments in python API
- change timestamp getter names and add comments/descriptions to python
- clear ty check
- add some helper methods to Data/RowView and change accessor function names

### Other

- bench(ccdb) add benchmark for parsing multiple data values
- *(ccdb)* speed up column layout reuse and vault parsing
- bench(ccdb) increase benchmark run range
- use test tables for benchmarks and add benchmark to test data parsing
- revert from using temp tables to just grabbing all the constant set data when we get assignments
- update documentation rules to pass checks
- add documentation
- reorganize into workspace crate
