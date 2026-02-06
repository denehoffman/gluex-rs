//! `GlueX` CCDB access library with optional Python bindings.
//!
//! This crate provides a read-only interface to the Jefferson Lab Calibration
//! and Conditions Database (CCDB).
use gluex_core::errors::ParseTimestampError;
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
    #[error("{0}")]
    SqliteError(#[from] rusqlite::Error),
    /// Wrapper around data parsing or shape errors when decoding payloads.
    #[error("{0}")]
    CCDBDataError(#[from] crate::data::CCDBDataError),
    /// Requested directory path could not be resolved.
    #[error("directory not found: {0}")]
    DirectoryNotFoundError(String),
    /// Requested table path could not be resolved.
    #[error("table not found: {0}")]
    TableNotFoundError(String),
    /// Path was malformed or missing a required component.
    #[error("invalid path: {0}")]
    InvalidPathError(String),
    /// Variation name does not exist in the database.
    #[error("variation not found: {0}")]
    VariationNotFoundError(String),
    /// Request string failed to parse.
    #[error("{0}")]
    ParseRequestError(#[from] context::ParseRequestError),
    /// CCDB path failed validation.
    #[error("{0}")]
    NamePathError(#[from] context::NamePathError),
    /// Timestamp string failed to parse.
    #[error("{0}")]
    ParseTimestampError(#[from] ParseTimestampError),
    /// Error finding the requested REST version.
    #[error("{0}")]
    RESTVersionError(#[from] gluex_core::run_periods::RESTVersionError),
    /// Error parsing the requested run period.
    #[error("{0}")]
    RunPeriodError(#[from] gluex_core::run_periods::RunPeriodError),
    /// Required environment variable is not set.
    #[error("missing {0} environment variable for CCDB connection")]
    MissingConnectionEnv(String),
}

pub use crate::context::{CCDBContext, NamePath, NamePathError, ParseRequestError};
pub use crate::data::{CCDBDataError, Column, ColumnDef, ColumnLayout, Data, RowView, Value};
pub use crate::database::CCDB;
pub use crate::models::{
    AssignmentMeta, AssignmentMetaLite, ColumnMeta, ColumnType, ConstantSetMeta, DirectoryMeta,
    EventRangeMeta, RunRangeMeta, TypeTableMeta, VariationMeta,
};
pub use gluex_core::{
    run_periods::{RESTVersionError, RunPeriod, RunPeriodError},
    RESTVersion, RunNumber,
};
