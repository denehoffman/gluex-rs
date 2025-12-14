# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
