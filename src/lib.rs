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
/// Backend-neutral errors returned by unified database requests.
pub mod database;
/// Cancellation and timeout controls for synchronous evaluation.
pub mod execution;
/// Monte Carlo generation and HDDM writing utilities.
pub mod generation;
pub use database::{DatabaseError, DatabaseResult};
pub use execution::{CancellationToken, ExecutionError, ExecutionOptions};
/// Photon-flux and tagged-luminosity calculations.
pub mod lumi;
/// Run Conditions Database access and predicate builders.
pub mod rcdb;

/// Shared numeric scope and recorded membership queries.
pub mod runs;
pub use runs::{
    CalibrationPath, ConditionCatalog, ConditionDefinition, ConditionOmission, ConditionProvenance,
    ConditionQuery, ConditionReport, ConditionResults, ConditionStream, ConditionValue,
    ConditionValueType, MissingDataConfig, MissingDataPolicy, ProcedureStatus, RunAccounting,
    RunOmission, RunOmissionReason, RunPredicate, RunProvenance, RunQuery, RunReport, RunSelection,
    RunSet, RunStream, SourceIdentity, Variation,
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

/// Typed operands for Condition Predicate construction.
pub use rcdb::conditions::ConditionOperand;

/// Build the explicit named approved-production Condition Predicate for a run period.
///
/// # Errors
/// Rejects periods without a documented approved-production definition.
pub fn approved_production(period: RunPeriod) -> DatabaseResult<RunPredicate> {
    rcdb::conditions::aliases::approved_production(period)
        .map(RunPredicate)
        .map_err(Into::into)
}

/// Calibration catalogs and explicit numeric queries.
pub mod calibrations;
pub use calibrations::{
    CalibrationCatalog, CalibrationColumn, CalibrationColumnValues, CalibrationDirectory,
    CalibrationEntry, CalibrationPayload, CalibrationProvenance, CalibrationQuery,
    CalibrationReport, CalibrationSeries, CalibrationStream, CalibrationTable,
    CalibrationTableMetadata, CalibrationValueType, ReconstructionSelection,
};
