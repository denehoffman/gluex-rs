mod ccdb;
mod core;
mod lumi;
mod rcdb;

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
    use crate::lumi::lumi;
    #[pymodule_export]
    use crate::rcdb::rcdb;

    #[allow(non_upper_case_globals)]
    #[pymodule_export]
    const __version__: &str = env!("CARGO_PKG_VERSION");
}
