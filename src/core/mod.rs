pub mod constants;
pub mod enums;
pub mod parsers;
pub mod particles;
pub mod run_periods;
/// Filesystem and other shared utility helpers.
pub mod utils;

use thiserror::Error;

/// Primary integer identifier type used throughout CCDB and RCDB.
pub type Id = i64;

/// Run number type as stored in CCDB and RCDB.
pub type RunNumber = i64;

/// REST versions of analysis reconstructions.
pub type RESTVersion = usize;

/// Unified error type for all `gluex-core` fallible APIs.
#[derive(Error, Debug, Clone)]
pub enum GlueXCoreError {
    /// Input contained no digits from which to form a timestamp.
    #[error("timestamp \"{0}\" has no digits")]
    TimestampNoDigits(String),
    /// Parsed timestamp was invalid according to the [`chrono`] crate.
    #[error("invalid timestamp: {0}")]
    TimestampChrono(String),
    /// Histogram requires at least two edge values.
    #[error("histogram edges must contain at least two values (found {len})")]
    HistogramTooFewEdges { len: usize },
    /// Edge value was NaN or infinite.
    #[error("histogram edge at index {index} is not finite: {value}")]
    HistogramNonFiniteEdge { index: usize, value: f64 },
    /// Consecutive edge values were not strictly increasing.
    #[error(
        "histogram edges must be strictly increasing (edges[{index}]={left}, edges[{next_index}]={right})"
    )]
    HistogramNotStrictlyIncreasing {
        index: usize,
        next_index: usize,
        left: f64,
        right: f64,
    },
    /// Number of counts does not match number of bins.
    #[error("counts length mismatch: expected {expected}, found {found}")]
    HistogramCountLengthMismatch { expected: usize, found: usize },
    /// Number of errors does not match number of bins.
    #[error("errors length mismatch: expected {expected}, found {found}")]
    HistogramErrorLengthMismatch { expected: usize, found: usize },
    /// Number of weights does not match number of values.
    #[error("weights length mismatch: expected {expected}, found {found}")]
    HistogramWeightLengthMismatch { expected: usize, found: usize },
    /// Uniform histogram requested with zero bins.
    #[error("uniform histogram requires at least one bin")]
    HistogramEmptyBinCount,
    /// Uniform histogram limits were not finite and strictly increasing.
    #[error(
        "uniform histogram limits must be finite and strictly increasing (min={min}, max={max})"
    )]
    HistogramInvalidUniformLimits { min: f64, max: f64 },
    /// Histograms with different edges cannot be combined.
    #[error("histogram edges differ and cannot be combined")]
    HistogramEdgeMismatch,
    /// Could not parse a particle from its canonical enum name.
    #[error("unknown particle enum name: {0}")]
    ParticleParse(String),
    /// Run number does not belong to any known run period.
    #[error("Run number {0} not in range of any known run period")]
    UnknownRunPeriod(RunNumber),
    /// Could not parse run-period shorthand.
    #[error("Could not parse run period from string {0}")]
    RunPeriodParse(String),
    /// No REST metadata exists for the run period.
    #[error("Run period {0:?} is missing REST version metadata")]
    MissingRESTVersions(crate::core::run_periods::RunPeriod),
    /// Requested REST version is not defined for the run period.
    #[error("REST version {requested} is not defined for run period {run_period:?}")]
    UnknownRESTVersion {
        run_period: crate::core::run_periods::RunPeriod,
        requested: RESTVersion,
    },
    /// Error from [`laddu`]
    #[error(transparent)]
    Laddu(#[from] laddu::LadduError),
}

pub use self::enums::{DetectorSystem, Polarization};
pub use self::particles::{Charge, Particle};
pub use crate::core::run_periods::{RESTVersionSelection, RunPeriod};
pub use laddu::physics::histogram::Histogram;
