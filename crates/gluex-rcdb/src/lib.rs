//! GlueX RCDB access library with optional Python bindings.

pub mod context;
pub mod data;
pub mod database;
pub mod models;

pub use context::{Context, RunSelection};
pub use data::Value;
pub use database::RCDB;
pub use models::ValueType;

use gluex_core::errors::ParseTimestampError;
use gluex_core::RunNumber;
use thiserror::Error;

/// Convenience alias for results returned from RCDB operations.
pub type RCDBResult<T> = Result<T, RCDBError>;

/// Errors that can occur while interacting with RCDB metadata or payloads.
#[derive(Error, Debug)]
pub enum RCDBError {
    /// Wrapper around rusqlite errors.
    #[error("{0}")]
    SqliteError(#[from] rusqlite::Error),
    /// Requested condition name does not exist.
    #[error("condition type not found: {0}")]
    ConditionTypeNotFound(String),
    /// The SQLite file does not contain the expected schema version entry.
    #[error("schema_versions table does not contain version 2")]
    MissingSchemaVersion,
    /// Timestamp parsing failed while decoding a `time` condition.
    #[error("{0}")]
    ParseTimestampError(#[from] ParseTimestampError),
    /// Encountered a value type identifier we do not understand.
    #[error("unknown RCDB value type identifier: {0}")]
    UnknownValueType(String),
    /// `time` condition row was missing a `time_value` entry.
    #[error("missing time_value for condition {condition_name} at run {run_number}")]
    MissingTimeValue {
        condition_name: String,
        run_number: RunNumber,
    },
}

/// Re-exports for the most common types.
pub mod prelude {
    pub use crate::{
        context::{Context, RunSelection},
        data::Value,
        database::RCDB,
        models::ValueType,
        RCDBError, RCDBResult,
    };
    pub use gluex_core::RunNumber;
}
