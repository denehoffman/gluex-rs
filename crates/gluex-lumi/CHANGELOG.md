# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
