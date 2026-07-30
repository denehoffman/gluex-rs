//! `GlueX` CCDB access library with optional Python bindings.
//!
//! This crate provides a read-only interface to the Jefferson Lab Calibration
//! and Conditions Database (CCDB).
use thiserror::Error;

/// Context handling for run-, variation-, and timestamp-aware requests.
pub mod context;
/// Column-oriented data structures returned from CCDB queries.
pub mod data;
/// High-level database entry points and handles to CCDB objects.
pub mod database;
/// Lightweight structs that mirror CCDB tables.
pub mod models;

/// Convenience alias for functions that can return a [`CCDBError`].
pub type CCDBResult<T> = Result<T, CCDBError>;

/// Errors that can occur while interacting with CCDB metadata or payloads.
#[derive(Error, Debug)]
pub enum CCDBError {
    /// Wrapper around [`rusqlite::Error`].
    #[error(transparent)]
    SqliteError(#[from] rusqlite::Error),
    /// Requested directory path could not be resolved.
    #[error("directory not found: {0}")]
    DirectoryNotFoundError(String),
    /// Requested table path could not be resolved.
    #[error("table not found: {0}")]
    TableNotFoundError(String),
    /// Failed to resolve or canonicalize a filesystem path.
    #[error(transparent)]
    PathResolutionError(#[from] std::io::Error),
    /// Path was malformed or missing a required component.
    #[error("invalid path: {0}")]
    InvalidPathError(String),
    /// Variation name does not exist in the database.
    #[error("variation not found: {0}")]
    VariationNotFoundError(String),
    /// Path did not begin with a forward slash.
    #[error("path \"{0}\" is not absolute (must start with '/')")]
    NotAbsolutePath(String),
    /// Path contained a character outside the allowed set.
    #[error("illegal character encountered in path \"{0}\"")]
    IllegalCharacter(String),
    /// Run number was not a valid integer.
    #[error("invalid run number: {0}")]
    InvalidRunNumberError(String),
    /// Failed to parse data because the number of cells was not divisible by the number of columns.
    #[error("column count mismatch (expected {expected}, found {found})")]
    ColumnCountMismatch {
        /// The total expected number of cells.
        expected: usize,
        /// The number of cells found while parsing.
        found: usize,
    },
    /// Failed to parse a cell to the given type.
    #[error("parse error at row {row}, column {column} ({column_type}): {text:?}")]
    ParseError {
        /// The column index of the cell.
        column: usize,
        /// The row index of the cell.
        row: usize,
        /// The expected column type for the cell.
        column_type: crate::ccdb::models::ColumnType,
        /// The unparsed contents of the cell.
        text: String,
    },
    /// Failed to retrieve a row due to an out-of-bounds index.
    #[error("row index {requested} out of bounds (n_rows={n_rows})")]
    RowOutOfBounds {
        /// The requested index.
        requested: usize,
        /// The available number of rows.
        n_rows: usize,
    },
    /// Timestamp string failed to parse.
    #[error(transparent)]
    GlueXCoreError(#[from] crate::core::GlueXCoreError),
    /// Required environment variable is not set.
    #[error("missing {0} environment variable for CCDB connection")]
    MissingConnectionEnv(String),
}

pub use self::context::{CCDBContext, NamePath, Request};
pub use self::data::{Column, ColumnDef, ColumnLayout, Data, RowView, Value};
pub use self::database::CCDB;
pub use crate::ccdb::models::{
    AssignmentMeta, AssignmentMetaLite, ColumnMeta, ColumnType, ConstantSetMeta, DirectoryMeta,
    EventRangeMeta, RunRangeMeta, TypeTableMeta, VariationMeta,
};
pub use crate::core::{GlueXCoreError, RESTVersion, RunNumber, run_periods::RunPeriod};
