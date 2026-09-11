use pyo3::{create_exception, prelude::*};
use std::error::Error;

create_exception!(
    gluex,
    MissingCapabilityError,
    pyo3::exceptions::PyRuntimeError
);
create_exception!(gluex, ConfigurationError, pyo3::exceptions::PyValueError);
create_exception!(gluex, QueryError, pyo3::exceptions::PyRuntimeError);
create_exception!(gluex, DecodeError, QueryError);
create_exception!(gluex, MissingDataError, QueryError);
create_exception!(gluex, CancellationError, QueryError);
create_exception!(
    gluex,
    DatabaseTimeoutError,
    pyo3::exceptions::PyTimeoutError
);

pub(crate) fn map(error: &(dyn Error + 'static)) -> PyErr {
    let message = error.to_string();
    let mut source = Some(error);
    while let Some(current) = source {
        if let Some(database) = current.downcast_ref::<crate::DatabaseError>()
            && let Some(execution) = database.execution_error()
        {
            return execution_error(execution, &message);
        }
        if let Some(crate::RawError::Execution(execution)) =
            current.downcast_ref::<crate::RawError>()
        {
            return execution_error(*execution, &message);
        }
        if let Some(workflow) = current.downcast_ref::<crate::WorkflowError>() {
            match workflow {
                crate::WorkflowError::Timeout => {
                    return DatabaseTimeoutError::new_err(message);
                }
                crate::WorkflowError::Cancelled => return CancellationError::new_err(message),
                crate::WorkflowError::Interrupted => {
                    return pyo3::exceptions::PyKeyboardInterrupt::new_err(message);
                }
                _ => {}
            }
        }
        if let Some(execution) = current.downcast_ref::<crate::ExecutionError>() {
            return execution_error(*execution, &message);
        }
        if let Some(session) = current.downcast_ref::<crate::GlueXError>() {
            return match session {
                crate::GlueXError::MissingCapability(_) => MissingCapabilityError::new_err(message),
                crate::GlueXError::Configuration { .. } => ConfigurationError::new_err(message),
            };
        }
        if matches!(
            current.downcast_ref::<crate::ccdb::CCDBError>(),
            Some(crate::ccdb::CCDBError::MissingData(_))
        ) || matches!(
            current.downcast_ref::<crate::rcdb::RCDBError>(),
            Some(crate::rcdb::RCDBError::MissingData(_))
        ) || matches!(
            current.downcast_ref::<crate::lumi::LuminosityError>(),
            Some(crate::lumi::LuminosityError::MissingRunInput { .. })
        ) {
            return MissingDataError::new_err(message);
        }
        if matches!(
            current.downcast_ref::<crate::ccdb::CCDBError>(),
            Some(
                crate::ccdb::CCDBError::InvalidMetadata(_)
                    | crate::ccdb::CCDBError::ColumnCountMismatch { .. }
                    | crate::ccdb::CCDBError::ParseError { .. }
                    | crate::ccdb::CCDBError::GlueXCoreError(_)
            )
        ) || matches!(
            current.downcast_ref::<crate::rcdb::RCDBError>(),
            Some(
                crate::rcdb::RCDBError::InvalidValue(_)
                    | crate::rcdb::RCDBError::MalformedValue { .. }
                    | crate::rcdb::RCDBError::GlueXCoreError(_)
                    | crate::rcdb::RCDBError::UnknownValueType(_)
            )
        ) {
            return DecodeError::new_err(message);
        }
        source = current.source();
    }
    QueryError::new_err(message)
}

fn execution_error(error: crate::ExecutionError, message: &str) -> PyErr {
    match error {
        crate::ExecutionError::Timeout => DatabaseTimeoutError::new_err(message.to_owned()),
        crate::ExecutionError::Cancelled => CancellationError::new_err(message.to_owned()),
        crate::ExecutionError::Interrupted => {
            pyo3::exceptions::PyKeyboardInterrupt::new_err(message.to_owned())
        }
    }
}
