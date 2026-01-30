# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.8](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.7...gluex-lumi-v0.1.8) (2026-01-30)


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * gluex-core bumped from 0.1.7 to 0.1.8
    * gluex-rcdb bumped from 0.1.7 to 0.1.8
    * gluex-ccdb bumped from 0.1.7 to 0.1.8

## [0.1.7](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.6...gluex-lumi-v0.1.7) (2026-01-28)


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
    * gluex-rcdb bumped from 0.1.6 to 0.1.7
    * gluex-ccdb bumped from 0.1.4 to 0.1.7

## [0.1.6](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.5...gluex-lumi-v0.1.6) (2026-01-27)

## [0.1.5](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.4...gluex-lumi-v0.1.5) (2026-01-22)

## [Unreleased]

## [0.1.4](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.3...gluex-lumi-v0.1.4) - 2026-01-21

### Fixed

- correct --exclude-runs parsing in CLI

## [0.1.3](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.2...gluex-lumi-v0.1.3) - 2026-01-21

### Added

- add argument for skipping runs in gluex-lumi

## [0.1.2](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.1...gluex-lumi-v0.1.2) - 2025-12-18

### Other

- updated the following local packages: gluex-rcdb

## [0.1.1](https://github.com/denehoffman/gluex-rs/compare/gluex-lumi-v0.1.0...gluex-lumi-v0.1.1) - 2025-12-18

### Other

- update Cargo.lock dependencies

## [0.1.0](https://github.com/denehoffman/gluex-rs/releases/tag/gluex-lumi-v0.1.0) - 2025-12-14

### Added

- release-ready I hope
- update REST version selections, calibration times, and the overall CLI for gluex-lumi to be more informative
- full lints and precommits plus a Justfile to round it all out
- *(lumi-py)* add python bindings and plotting CLI
- first full impl of gluex-lumi, but it's slow due to RCDB, and gluex-ccdb-py won't build

### Fixed

- handle RP2019_11 calibration override

### Other

- *(gluex-rcdb)* benchmark and force run-number index
