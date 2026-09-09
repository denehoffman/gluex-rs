use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use pyo3::{
    exceptions::{PyKeyboardInterrupt, PyRuntimeError, PyValueError},
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
            result.map_err(|error| PyRuntimeError::new_err(error.to_string()))
        }
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
