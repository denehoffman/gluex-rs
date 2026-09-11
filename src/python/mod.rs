mod calibrations;
mod ccdb;
mod core;
mod exceptions;
mod execution;
mod generation;
mod lumi;
mod raw;
mod rcdb;
mod runs;
mod session;
mod tuple;
mod workflows;

use pyo3::prelude::*;

#[pyfunction]
fn _console_main(py: Python<'_>) -> PyResult<u8> {
    let args: Vec<String> = py.import("sys")?.getattr("argv")?.extract()?;
    Ok(crate::cli::exit_code_with_args(args))
}

#[pyo3::pymodule(name = "gluex")]
mod gluex {
    use pyo3::types::PyAnyMethods;

    #[pymodule_export]
    use super::_console_main;
    #[pymodule_export]
    use super::ccdb::ccdb;
    #[pymodule_export]
    use super::core::{
        PyCalibratedRunPeriod, PyCharge, PyDetectorSystem, PyHistogram, PyParticle, PyPolarization,
        PyRESTVersionSelection, PyRunPeriod, coherent_peak, parse_timestamp,
    };
    #[pymodule_export]
    use super::exceptions::{
        CancellationError, ConfigurationError, DatabaseTimeoutError, DecodeError,
        MissingCapabilityError, MissingDataError, QueryError,
    };
    #[pymodule_export]
    use super::generation::generation;
    #[pymodule_export]
    use super::lumi::lumi;
    #[pymodule_export]
    use super::rcdb::rcdb;

    #[pymodule_export]
    use super::session::{PyCacheInfo, PyCapabilities, PyDisabled, PyGlueX, PySources, open};

    #[pymodule_export]
    use super::runs::{
        PyConditionCatalog, PyConditionDefinition, PyConditionOmission, PyConditionProvenance,
        PyConditionQuery, PyConditionReport, PyConditionResults, PyConditionStream,
        PyMissingDataConfig, PyRunAccounting, PyRunAliases, PyRunOmission, PyRunOmissionReason,
        PyRunPredicate, PyRunProvenance, PyRunQuery, PyRunReport, PyRunSelection, PyRunSet,
        PyRunStream, PyRuns, PySourceIdentity,
    };

    #[pymodule_export]
    use super::calibrations::{
        PyCalibrationCatalog, PyCalibrationColumn, PyCalibrationDirectory, PyCalibrationEntry,
        PyCalibrationPayload, PyCalibrationProvenance, PyCalibrationQuery, PyCalibrationReport,
        PyCalibrationSeries, PyCalibrationStream, PyCalibrationTable, PyReconstructionSelection,
    };

    #[pymodule_export]
    use super::raw::{PyRawColumn, PyRawResults, PyRawRow};
    #[pymodule_export]
    use super::workflows::{
        PyLuminosityProvenance, PyLuminosityQuery, PyLuminosityReport, PyLuminosityResult,
        PyOperations, PyWorkflows,
    };

    #[pymodule_export]
    const DISABLED: super::session::PyDisabled = super::session::PyDisabled;

    #[allow(non_upper_case_globals)]
    #[pymodule_export]
    const __version__: &str = env!("CARGO_PKG_VERSION");

    #[pymodule_init]
    fn init(module: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
        let modules = module.py().import("sys")?.getattr("modules")?;
        for name in ["ccdb", "generation", "lumi", "rcdb"] {
            modules.set_item(format!("gluex.{name}"), module.getattr(name)?)?;
        }
        Ok(())
    }
}
