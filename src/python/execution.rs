use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use pyo3::{
    exceptions::{PyKeyboardInterrupt, PyRuntimeError, PyTimeoutError, PyValueError},
    prelude::*,
};

#[derive(Clone)]
pub(crate) struct PythonExecution {
    interrupted: Arc<AtomicBool>,
}

impl PythonExecution {
    pub(crate) fn new() -> Self {
        Self {
            interrupted: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn checker(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let signal = Arc::clone(&self.interrupted);
        move || {
            Python::attach(|py| {
                if py.check_signals().is_err() {
                    signal.store(true, Ordering::Release);
                    true
                } else {
                    false
                }
            })
        }
    }

    pub(crate) fn finish<T, E: std::fmt::Display>(&self, result: Result<T, E>) -> PyResult<T> {
        if self.interrupted.load(Ordering::Acquire) {
            Err(PyKeyboardInterrupt::new_err(
                "database execution interrupted",
            ))
        } else {
            result.map_err(|error| {
                let message = error.to_string();
                if message.contains("execution timed out") {
                    PyTimeoutError::new_err(message)
                } else {
                    PyRuntimeError::new_err(message)
                }
            })
        }
    }

    pub(crate) fn execute<Q, T>(
        py: Python<'_>,
        query: &Q,
        execute: impl FnOnce(&Q) -> Result<T, Q::Error> + Send,
    ) -> PyResult<T>
    where
        Q: crate::execution::TerminalQuery + Send + Sync,
        Q::Error: std::fmt::Display + Send,
        T: Send,
    {
        let signals = Self::new();
        let query = query.with_execution_options(
            query
                .execution_options()
                .clone()
                .with_interrupt_check(signals.checker()),
        );
        signals.finish(py.detach(move || execute(&query)))
    }

    pub(crate) fn execute_options<T, E>(
        py: Python<'_>,
        options: crate::ExecutionOptions,
        execute: impl FnOnce(crate::ExecutionOptions) -> Result<T, E> + Send,
    ) -> PyResult<T>
    where
        T: Send,
        E: std::fmt::Display + Send,
    {
        let signals = Self::new();
        let options = options.with_interrupt_check(signals.checker());
        signals.finish(py.detach(move || execute(options)))
    }

    pub(crate) fn stream<Q, S>(
        query: &Q,
        stream: impl FnOnce(&Q) -> crate::DatabaseResult<S>,
    ) -> PyResult<(S, Self)>
    where
        Q: crate::execution::TerminalQuery,
    {
        let signals = Self::new();
        let query = query.with_execution_options(
            query
                .execution_options()
                .clone()
                .with_interrupt_check(signals.checker()),
        );
        stream(&query)
            .map(|stream| (stream, signals))
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    pub(crate) fn next<T>(
        &self,
        py: Python<'_>,
        next: impl FnOnce() -> crate::DatabaseResult<Option<T>> + Send,
    ) -> PyResult<Option<T>>
    where
        T: Send,
    {
        self.finish(py.detach(next))
    }
}

pub(crate) fn timeout(seconds: f64) -> PyResult<std::time::Duration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(PyValueError::new_err(
            "timeout must be a finite non-negative number of seconds",
        ));
    }
    Ok(std::time::Duration::from_secs_f64(seconds))
}
