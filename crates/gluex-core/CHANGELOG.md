# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
