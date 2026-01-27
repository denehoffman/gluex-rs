pub mod constants;
pub mod detectors;
pub mod enums;
pub mod errors;
pub mod histograms;
pub mod parsers;
pub mod particles;
pub mod run_periods;

/// Primary integer identifier type used throughout CCDB and RCDB.
pub type Id = i64;

/// Run number type as stored in CCDB and RCDB.
pub type RunNumber = i64;

/// REST versions of analysis reconstructions.
pub type RestVersion = usize;
