# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.8](https://github.com/denehoffman/gluex-rs/compare/gluex-core-v0.1.7...gluex-core-v0.1.8) (2026-01-30)


### Features

* Add convenience methods for creating and filling histograms ([22b162e](https://github.com/denehoffman/gluex-rs/commit/22b162eb7e89be5c3caee7b15eac2a1ffed23380))

## [0.1.7](https://github.com/denehoffman/gluex-rs/compare/gluex-core-v0.1.3...gluex-core-v0.1.7) (2026-01-28)


### Bug Fixes

* **gluex-ccdb-py:** Release gluex-ccdb-py-v0.1.5 ([a0cfa95](https://github.com/denehoffman/gluex-rs/commit/a0cfa95edf2e6e5c43496f9e424783238799c5ec))
* **gluex-ccdb:** Release gluex-ccdb-v0.1.5 ([c7f1245](https://github.com/denehoffman/gluex-rs/commit/c7f124526ee1ccc5cd8b99f8c847e72ed7a42852))
* **gluex-lumi-py:** Release gluex-lumi-py-v0.1.7 ([c74ab0b](https://github.com/denehoffman/gluex-rs/commit/c74ab0be7254e558bae21149ec025338cfecba6b))
* **gluex-lumi:** Release gluex-lumi-v0.1.7 ([c61aac8](https://github.com/denehoffman/gluex-rs/commit/c61aac898ed66d068dd13ba7c258c2ae0cedae5a))
* **gluex-rcdb-py:** Release gluex-rcdb-py-v0.1.7 ([6649f15](https://github.com/denehoffman/gluex-rs/commit/6649f15ded268475e8822f303f3319f5802c16de))
* **gluex-rcdb:** Release gluex-rcdb-v0.1.7 ([a4acfb0](https://github.com/denehoffman/gluex-rs/commit/a4acfb026faf8faa6d5b85f4377322a19e5602e8))

## [0.1.3](https://github.com/denehoffman/gluex-rs/compare/gluex-core-v0.1.2...gluex-core-v0.1.3) (2026-01-27)


### Features

* **detectors.rs:** Add enums for dealing with GlueX detectors ([1e8f2b0](https://github.com/denehoffman/gluex-rs/commit/1e8f2b0cb59d03ab8bb243b3566d54893711ea5c))
* **histograms.rs:** Add some helper methods to the Histogram class ([ad391c7](https://github.com/denehoffman/gluex-rs/commit/ad391c757e390b7b5bc8160d3e07cf5c631740d3))

## [0.1.2](https://github.com/denehoffman/gluex-rs/compare/gluex-core-v0.1.1...gluex-core-v0.1.2) (2026-01-22)


### Features

* Add fetch method which incorporates REST version timestamps and run period selection ([ef0a6eb](https://github.com/denehoffman/gluex-rs/commit/ef0a6eb25acb77d743f790c5923bf940198a5470))
* Add run_range and contains methods to RunPeriod ([b949014](https://github.com/denehoffman/gluex-rs/commit/b9490143127543ade2a9d240a0ff614c05656863))
* **core:** Add equivalent of particleType.h ([f67ab2e](https://github.com/denehoffman/gluex-rs/commit/f67ab2eaf5dd0b7285aef95c28fdb9f2b6b02474))
* First draft of RCDB function, move some constants into gluex-core ([dfda19d](https://github.com/denehoffman/gluex-rs/commit/dfda19d5c7a747562d931f73663d858799bf7c87))
* First full impl of gluex-lumi, but it's slow due to RCDB, and gluex-ccdb-py won't build ([7b07372](https://github.com/denehoffman/gluex-rs/commit/7b07372d9c43537baf51cb71691fabb973bea21f))
* Release-ready I hope ([aebbf2d](https://github.com/denehoffman/gluex-rs/commit/aebbf2d481f273caaf8987efb55aab72706131a4))
* Restructure crates a bit and add RCDB skeleton crate ([8f1ba69](https://github.com/denehoffman/gluex-rs/commit/8f1ba698b240ac20b2a624d905d8bb820b6a76a6))
* Separate Python crates, add lots of clippy lints, add precommit, and a few other small API changes ([d4de1b6](https://github.com/denehoffman/gluex-rs/commit/d4de1b6a39571d0bc58c769af6514a7c63f49c30))
* Update lumi rest version handling ([15427e5](https://github.com/denehoffman/gluex-rs/commit/15427e523b35966caaecc280f976264f6c16d8a6))
* Update REST version selections, calibration times, and the overall CLI for gluex-lumi to be more informative ([1156773](https://github.com/denehoffman/gluex-rs/commit/1156773210c364ac09f98566a58895ee1b3391b5))


### Bug Fixes

* Handle RP2019_11 calibration override ([a330993](https://github.com/denehoffman/gluex-rs/commit/a330993282ee4ce7f3f58bbaae69609c37d1c99a))

## [Unreleased]

## [0.1.1](https://github.com/denehoffman/gluex-rs/compare/gluex-core-v0.1.0...gluex-core-v0.1.1) - 2025-12-15

### Added

- add run_range and contains methods to RunPeriod

### Other

- release v0.1.0 ([#1](https://github.com/denehoffman/gluex-rs/pull/1))

## [0.1.0](https://github.com/denehoffman/gluex-rs/releases/tag/gluex-core-v0.1.0) - 2025-12-14

### Added

- release-ready I hope
- update REST version selections, calibration times, and the overall CLI for gluex-lumi to be more informative
- update lumi rest version handling
- first full impl of gluex-lumi, but it's slow due to RCDB, and gluex-ccdb-py won't build
- *(core)* add equivalent of particleType.h
- separate Python crates, add lots of clippy lints, add precommit, and a few other small API changes
- first draft of RCDB function, move some constants into gluex-core
- restructure crates a bit and add RCDB skeleton crate

### Fixed

- handle RP2019_11 calibration override
