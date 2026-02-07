pub mod constants;
pub mod detectors;
pub mod enums;
pub mod errors;
pub mod histograms;
pub mod parsers;
pub mod particles;
pub mod run_periods;
/// Filesystem and other shared utility helpers.
pub mod utils;

/// Primary integer identifier type used throughout CCDB and RCDB.
pub type Id = i64;

/// Run number type as stored in CCDB and RCDB.
pub type RunNumber = i64;

/// REST versions of analysis reconstructions.
pub type RESTVersion = usize;

pub use crate::detectors::DetectorSystem;
pub use crate::enums::Polarization;
pub use crate::errors::{HistogramError, ParseTimestampError};
pub use crate::histograms::Histogram;
pub use crate::particles::{Charge, Particle};
pub use crate::run_periods::{RESTVersionError, RESTVersionSelection, RunPeriod, RunPeriodError};
