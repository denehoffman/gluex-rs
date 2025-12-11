pub mod constants;
pub mod errors;
pub mod parsers;
pub mod particle;

/// Primary integer identifier type used throughout CCDB and RCDB.
pub type Id = i64;

/// Run number type as stored in CCDB and RCDB.
pub type RunNumber = i64;
