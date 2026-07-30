mod ccdb;
mod core;
mod generation;
mod lumi;
mod rcdb;

use pyo3::prelude::*;

#[pyfunction]
fn _console_main(py: Python<'_>) -> PyResult<u8> {
    let args: Vec<String> = py.import("sys")?.getattr("argv")?.extract()?;
    Ok(crate::cli::exit_code_with_args(args))
}

#[pyo3::pymodule(name = "gluex")]
mod gluex {
    #[pymodule_export]
    use super::_console_main;
    #[pymodule_export]
    use super::ccdb::ccdb;
    #[pymodule_export]
    use super::core::{
        PyCharge, PyDetectorSystem, PyHistogram, PyParticle, PyPolarization,
        PyRESTVersionSelection, PyRunPeriod, coherent_peak, parse_timestamp,
    };
    #[pymodule_export]
    use super::generation::generation;
    #[pymodule_export]
    use super::lumi::lumi;
    #[pymodule_export]
    use super::rcdb::rcdb;

    #[allow(non_upper_case_globals)]
    #[pymodule_export]
    const __version__: &str = env!("CARGO_PKG_VERSION");
}
