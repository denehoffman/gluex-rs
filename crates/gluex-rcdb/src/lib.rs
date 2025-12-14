//! `GlueX` RCDB access library with optional Python bindings.

/// Condition expression builders and helpers.
pub mod conditions;
/// Run-selection context utilities.
pub mod context;
/// Value container utilities returned from queries.
pub mod data;
/// High-level database accessors.
pub mod database;
/// Lightweight structs that mirror RCDB tables.
pub mod models;

use gluex_core::errors::ParseTimestampError;
use gluex_core::RunNumber;
use thiserror::Error;

use crate::models::ValueType;

/// Convenience alias for results returned from RCDB operations.
pub type RCDBResult<T> = Result<T, RCDBError>;

/// Errors that can occur while interacting with RCDB metadata or payloads.
#[derive(Error, Debug)]
pub enum RCDBError {
    /// Wrapper around [`rusqlite::Error`].
    #[error("{0}")]
    SqliteError(#[from] rusqlite::Error),
    /// Requested condition name does not exist.
    #[error("condition type not found: {0}")]
    ConditionTypeNotFound(String),
    /// The `SQLite` file does not contain the expected schema version entry.
    #[error("schema_versions table does not contain version 2")]
    MissingSchemaVersion,
    /// Fetch API requires at least one condition name.
    #[error("fetch requires at least one condition name")]
    EmptyConditionList,
    /// Timestamp parsing failed while decoding a `time` condition.
    #[error("{0}")]
    ParseTimestampError(#[from] ParseTimestampError),
    /// Encountered a value type identifier we do not understand.
    #[error("unknown RCDB value type identifier: {0}")]
    UnknownValueType(String),
    /// Predicate requested a condition with a mismatched type.
    #[error("condition {condition_name} type mismatch: expected {expected:?}, actual {actual:?}")]
    ConditionTypeMismatch {
        /// Name of the offending condition.
        condition_name: String,
        /// Type requested by the predicate builder.
        expected: ValueType,
        /// Type stored in the database schema.
        actual: ValueType,
    },
    /// `time` condition row was missing a `time_value` entry.
    #[error("missing time_value for condition {condition_name} at run {run_number}")]
    MissingTimeValue {
        /// Name of the time-valued condition.
        condition_name: String,
        /// Run number missing the time value.
        run_number: RunNumber,
    },
}

/// Re-exports for the most common types.
pub mod prelude {
    pub use crate::{
        conditions,
        context::{Context, RunSelection},
        data::Value,
        database::RCDB,
        models::ValueType,
        RCDBError, RCDBResult,
    };
    pub use gluex_core::RunNumber;
}
