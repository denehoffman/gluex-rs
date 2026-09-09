//! Unified Rust, Python, and command-line utilities for the `GlueX` experiment.
//!
//! The [`GlueX`] session provides typed run, calibration, raw-read, and canonical
//! [`workflows`] APIs. Shared experiment metadata lives at the crate root;
//! [`ccdb`] and [`rcdb`] remain available for advanced database-native reads,
//! [`lumi`] contains histogram result types, and [`generation`] supports laddu/HDDM.

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
/// Cancellation and timeout controls for synchronous evaluation.
pub mod execution;
/// Monte Carlo generation and HDDM writing utilities.
pub mod generation;
pub use execution::{CancellationToken, ExecutionOptions};
/// Photon-flux and tagged-luminosity calculations.
pub mod lumi;
/// Run Conditions Database access and predicate builders.
pub mod rcdb;

/// Shared numeric scope and recorded membership queries.
pub mod runs;
pub use runs::{
    ConditionCatalog, ConditionDefinition, ConditionProvenance, ConditionQuery, ConditionReport,
    ConditionResults, ConditionStream, MissingDataPolicy, RunProvenance, RunQuery, RunReport,
    RunSelection, RunSet, RunStream,
};

/// Enforced read-only raw rows and parameters.
pub mod raw;
pub use raw::{RawColumn, RawError, RawResults, RawRow, RawValue};

mod session;
pub use session::{
    CacheInfo, Capabilities, DatabaseKind, GlueX, GlueXError, SourceConfig, Sources,
};

/// Canonical GlueX Workflows and retained provenance.
pub mod workflows;
pub use workflows::{
    LUMINOSITY_PROCEDURE_VERSION, LuminosityProvenance, LuminosityQuery, LuminosityReport,
    LuminosityResult, WorkflowError, Workflows,
};

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
    RESTVersionContext, RESTVersionInfo, RESTVersionSelection, RunNumber, RunPeriod, constants,
    enums, parsers, particles, run_periods, utils,
};

/// Typed operands and composable run predicates.
pub use rcdb::conditions::{ConditionOperand, Expr as RunPredicate, aliases::approved_production};

/// Calibration catalogs and explicit numeric queries.
pub mod calibrations;
pub use calibrations::{
    CalibrationCatalog, CalibrationDirectory, CalibrationEntry, CalibrationProvenance,
    CalibrationQuery, CalibrationReport, CalibrationSeries, CalibrationStream, CalibrationTable,
    ReconstructionSelection,
};
