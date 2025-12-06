use thiserror::Error;

pub mod context;
pub mod data;
pub mod database;
pub mod models;

pub type Id = i64;
pub type RunNumber = u32;
pub type CCDBResult<T> = Result<T, CCDBError>;

#[derive(Error, Debug)]
pub enum CCDBError {
    #[error("{0}")]
    SqliteError(#[from] rusqlite::Error),
    #[error("{0}")]
    CCDBDataError(#[from] crate::data::CCDBDataError),
    #[error("directory not found: {0}")]
    DirectoryNotFoundError(String),
    #[error("table not found: {0}")]
    TableNotFoundError(String),
    #[error("invalid path: {0}")]
    InvalidPathError(String),
    #[error("variation not found: {0}")]
    VariationNotFoundError(String),
    #[error("{0}")]
    ParseRequestError(#[from] context::ParseRequestError),
    #[error("{0}")]
    ParseTimestampError(#[from] context::ParseTimestampError),
}

pub mod prelude {
    pub use crate::{context::Context, database::Database, CCDBError, CCDBResult, Id, RunNumber};
}
