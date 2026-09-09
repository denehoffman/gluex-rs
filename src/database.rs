//! Backend-neutral errors for source-bound database queries.

use std::{error::Error, fmt};

/// Error raised while constructing or evaluating a unified database request.
#[derive(Debug)]
pub struct DatabaseError {
    source: Box<dyn Error + Send + Sync>,
}

impl DatabaseError {
    pub(crate) fn new(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Box::new(source),
        }
    }

    pub(crate) fn with_context(context: impl Into<String>, source: Self) -> Self {
        Self::new(ContextError {
            context: context.into(),
            source,
        })
    }
}

#[derive(Debug)]
struct ContextError {
    context: String,
    source: DatabaseError,
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.context, self.source)
    }
}

impl Error for ContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<crate::rcdb::RCDBError> for DatabaseError {
    fn from(source: crate::rcdb::RCDBError) -> Self {
        Self::new(source)
    }
}

impl From<crate::ccdb::CCDBError> for DatabaseError {
    fn from(source: crate::ccdb::CCDBError) -> Self {
        Self::new(source)
    }
}

impl From<crate::GlueXCoreError> for DatabaseError {
    fn from(source: crate::GlueXCoreError) -> Self {
        Self::new(source)
    }
}

/// Result returned by unified run and calibration requests.
pub type DatabaseResult<T> = Result<T, DatabaseError>;
