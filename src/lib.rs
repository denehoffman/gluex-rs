//! Unified Rust, Python, and command-line utilities for the `GlueX` experiment.
//!
//! The crate is organized around four public API areas:
//! shared experiment metadata at the crate root, [`ccdb`] and [`rcdb`] database
//! access, [`lumi`] calculations, and [`generation`] support for laddu/HDDM.

/// Read-only Calibration and Conditions Database access.
pub mod ccdb;
/// Command-line entry points shared by the native and Python executables.
pub mod cli;
/// Shared experiment metadata, particle definitions, and parsing helpers.
///
/// Much of the particle table mirrors external experiment identifiers whose
/// variant names are their documentation.
#[allow(
    missing_docs,
    clippy::complexity,
    clippy::nursery,
    clippy::pedantic,
    clippy::perf,
    clippy::style,
    clippy::suspicious
)]
pub mod core;
/// Monte Carlo generation and HDDM writing utilities.
pub mod generation;
/// Photon-flux and tagged-luminosity calculations.
pub mod lumi;
/// Run Conditions Database access and predicate builders.
pub mod rcdb;

#[cfg(feature = "python")]
#[allow(
    clippy::complexity,
    clippy::nursery,
    clippy::pedantic,
    clippy::perf,
    clippy::style,
    clippy::suspicious
)]
mod python;

pub use core::{
    Charge, DetectorSystem, GlueXCoreError, Histogram, Id, Particle, Polarization, RESTVersion,
    RESTVersionSelection, RunNumber, RunPeriod, constants, enums, parsers, particles, run_periods,
    utils,
};
