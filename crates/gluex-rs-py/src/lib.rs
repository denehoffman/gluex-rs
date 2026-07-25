mod ccdb;
mod core;
mod generation;
mod lumi;
mod rcdb;

use pyo3::prelude::*;

#[pyfunction(name = "_console_main")]
fn py_console_main(py: Python<'_>) -> PyResult<u8> {
    let args: Vec<String> = py.import("sys")?.getattr("argv")?.extract()?;
    Ok(gluex_rs::cli::exit_code_with_args(args))
}

#[pyo3::pymodule(name = "_gluex")]
mod gluex {
    #[pymodule_export]
    use crate::ccdb::ccdb;
    #[pymodule_export]
    use crate::core::{
        PyCharge, PyDetectorSystem, PyHistogram, PyParticle, PyPolarization,
        PyRESTVersionSelection, PyRunPeriod, py_coherent_peak, py_parse_timestamp,
    };
    #[pymodule_export]
    use crate::generation::generation;
    #[pymodule_export]
    use crate::lumi::lumi;
    #[pymodule_export]
    use crate::py_console_main;
    #[pymodule_export]
    use crate::rcdb::rcdb;

    #[allow(non_upper_case_globals)]
    #[pymodule_export]
    const __version__: &str = env!("CARGO_PKG_VERSION");
}
