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

use thiserror::Error;

use crate::models::ValueType as LocalValueType;
use gluex_core::RunNumber as LocalRunNumber;

/// Convenience alias for results returned from RCDB operations.
pub type RCDBResult<T> = Result<T, RCDBError>;

/// Errors that can occur while interacting with RCDB metadata or payloads.
#[derive(Error, Debug)]
pub enum RCDBError {
    /// Wrapper around [`rusqlite::Error`].
    #[error(transparent)]
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
    /// Failed to resolve or canonicalize a filesystem path.
    #[error(transparent)]
    PathResolutionError(#[from] std::io::Error),
    /// Timestamp parsing failed while decoding a `time` condition.
    #[error(transparent)]
    GlueXCoreError(#[from] gluex_core::GlueXCoreError),
    /// Encountered a value type identifier we do not understand.
    #[error("unknown RCDB value type identifier: {0}")]
    UnknownValueType(String),
    /// Predicate requested a condition with a mismatched type.
    #[error("condition {condition_name} type mismatch: expected {expected:?}, actual {actual:?}")]
    ConditionTypeMismatch {
        /// Name of the offending condition.
        condition_name: String,
        /// Type requested by the predicate builder.
        expected: LocalValueType,
        /// Type stored in the database schema.
        actual: LocalValueType,
    },
    /// `time` condition row was missing a `time_value` entry.
    #[error("missing time_value for condition {condition_name} at run {run_number}")]
    MissingTimeValue {
        /// Name of the time-valued condition.
        condition_name: String,
        /// Run number missing the time value.
        run_number: LocalRunNumber,
    },
    /// Required environment variable is not set.
    #[error("missing {0} environment variable for RCDB connection")]
    MissingConnectionEnv(String),
    /// No approved-production alias has been defined for the run period.
    #[error("approved-production selection is not defined for run period {0:?}")]
    UnsupportedApprovedProductionRunPeriod(gluex_core::RunPeriod),
}

pub use crate::conditions::{BoolField, Expr, FloatField, IntField, StringField, TimeField};
pub use crate::context::{RCDBContext, RunSelection};
pub use crate::data::Value;
pub use crate::database::RCDB;
pub use crate::models::ValueType;
pub use gluex_core::RunNumber;
pub use gluex_core::run_periods::RunPeriod;
