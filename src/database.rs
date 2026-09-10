//! Backend-neutral errors for source-bound database queries.

use std::{error::Error, fmt};

/// Error raised while constructing or evaluating a unified database request.
#[derive(Debug)]
pub struct DatabaseError {
    source: Box<dyn Error + Send + Sync>,
    execution: Option<crate::ExecutionError>,
}

impl DatabaseError {
    pub(crate) fn new(source: impl Error + Send + Sync + 'static) -> Self {
        let execution = execution_error(&source);
        Self {
            source: Box::new(source),
            execution,
        }
    }

    pub(crate) fn with_context(context: impl Into<String>, source: Self) -> Self {
        let execution = source.execution;
        Self {
            source: Box::new(ContextError {
                context: context.into(),
                source,
            }),
            execution,
        }
    }

    /// Typed execution failure retained through domain-specific context layers.
    #[must_use]
    pub const fn execution_error(&self) -> Option<crate::ExecutionError> {
        self.execution
    }
}

fn execution_error(error: &(dyn Error + 'static)) -> Option<crate::ExecutionError> {
    let mut source = Some(error);
    while let Some(error) = source {
        if let Some(failure) = error.downcast_ref::<crate::ExecutionError>() {
            return Some(*failure);
        }
        source = error.source();
    }
    None
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
        let execution = match &source {
            crate::rcdb::RCDBError::Execution(failure) => Some(*failure),
            _ => None,
        };
        Self {
            source: Box::new(source),
            execution,
        }
    }
}

impl From<crate::ccdb::CCDBError> for DatabaseError {
    fn from(source: crate::ccdb::CCDBError) -> Self {
        let execution = match &source {
            crate::ccdb::CCDBError::Execution(failure) => Some(*failure),
            crate::ccdb::CCDBError::DatabaseError(error) => error.execution_error(),
            _ => None,
        };
        Self {
            source: Box::new(source),
            execution,
        }
    }
}

impl From<crate::GlueXCoreError> for DatabaseError {
    fn from(source: crate::GlueXCoreError) -> Self {
        Self::new(source)
    }
}

/// Result returned by unified run and calibration requests.
pub type DatabaseResult<T> = Result<T, DatabaseError>;
