//! GlueX CCDB access library with optional Python bindings.
//!
//! This crate provides a read-only interface to the Jefferson Lab Calibration
//! and Conditions Database (CCDB). It exposes both a native Rust API and an
//! optional Python API (via PyO3) for exploring directory structure, querying
//! tables, and loading typed constant data.
#![deny(missing_docs)]
use thiserror::Error;

/// Context handling for run-, variation-, and timestamp-aware requests.
pub mod context;
/// Column-oriented data structures returned from CCDB queries.
pub mod data;
/// High-level database entry points and handles to CCDB objects.
pub mod database;
#[allow(missing_docs, clippy::missing_docs_in_private_items)]
pub mod models;
#[cfg(feature = "python")]
pub mod python;

/// Primary integer identifier type used throughout the CCDB.
pub type Id = i64;
/// Run number type as stored in CCDB assignments.
pub type RunNumber = u32;
/// Convenience alias for functions that can return a `CCDBError`.
pub type CCDBResult<T> = Result<T, CCDBError>;

/// Errors that can occur while interacting with CCDB metadata or payloads.
#[derive(Error, Debug)]
pub enum CCDBError {
    /// Wrapper around SQLite driver errors.
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
    /// Timestamp string failed to parse.
    #[error("{0}")]
    ParseTimestampError(#[from] context::ParseTimestampError),
}

/// Re-exports of the most commonly used types and constructors.
pub mod prelude {
    pub use crate::{context::Context, database::CCDB, CCDBError, CCDBResult, Id, RunNumber};
}
#[cfg(feature = "python")]
pub use python::*;
