#[pyo3::pymodule(submodule)]
pub(crate) mod lumi {
    use std::{collections::HashMap, env};

    use ::gluex_lumi::{FluxHistograms, Luminosity, LuminosityContext, LuminosityError};
    use chrono::{DateTime, Utc};
    use gluex_core::{
        RESTVersion, RESTVersionSelection, RunNumber, run_periods::RunPeriod, utils::resolve_path,
    };
    use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyDict};

    use crate::core::{
        PyHistogram, PyRESTVersionSelection, histogram_to_dict, histogram_to_py,
        parse_run_period_object,
    };

    /// Flux and luminosity histograms aggregated across selected runs.
    #[pyclass(name = "FluxHistograms", module = "gluex.lumi")]
    pub struct PyFluxHistograms(FluxHistograms);

    #[pymethods]
    impl PyFluxHistograms {
        #[getter]
        fn tagged_flux(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagged_flux)
        }

        #[getter]
        fn tagm_flux(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagm_flux)
        }

        #[getter]
        fn tagh_flux(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagh_flux)
        }

        #[getter]
        fn tagged_luminosity(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagged_luminosity)
        }

        /// Return every histogram as serializable lists.
        pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
            let dict = PyDict::new(py);
            dict.set_item("tagged_flux", histogram_to_dict(py, &self.0.tagged_flux)?)?;
            dict.set_item("tagm_flux", histogram_to_dict(py, &self.0.tagm_flux)?)?;
            dict.set_item("tagh_flux", histogram_to_dict(py, &self.0.tagh_flux)?)?;
            dict.set_item(
                "tagged_luminosity",
                histogram_to_dict(py, &self.0.tagged_luminosity)?,
            )?;
            Ok(dict.unbind())
        }
    }

    fn parse_rest_versions(
        obj: Option<Bound<'_, PyAny>>,
    ) -> PyResult<HashMap<RunPeriod, RESTVersionSelection>> {
        let Some(obj) = obj else {
            return Ok(HashMap::new());
        };
        let mapping = obj.cast::<PyDict>().map_err(|_| {
            PyRuntimeError::new_err(
                "rest_version must map run-period names to REST versions (int), datetime, or None",
            )
        })?;
        let mut selection = HashMap::with_capacity(mapping.len());
        for (name, rest_version) in mapping.iter() {
            let period = parse_run_period_object(&name)?;
            let request = if rest_version.is_none() {
                RESTVersionSelection::Current
            } else if let Ok(selection) =
                rest_version.extract::<PyRef<'_, PyRESTVersionSelection>>()
            {
                selection.0
            } else if let Ok(version) = rest_version.extract::<RESTVersion>() {
                RESTVersionSelection::try_new(period, version)
                    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
            } else if let Ok(timestamp) = rest_version.extract::<DateTime<Utc>>() {
                RESTVersionSelection::from_timestamp(timestamp)
            } else {
                return Err(PyRuntimeError::new_err(
                    "rest_version must map RunPeriod or string keys to RESTVersionSelection, REST versions (int), datetime, or None",
                ));
            };
            selection.insert(period, request);
        }
        Ok(selection)
    }

    fn resolve_connection_path(value: Option<String>, env_var: &str) -> PyResult<String> {
        let raw_path = match value {
            Some(path) if !path.is_empty() => path,
            _ => env::var(env_var).map_err(|_| {
                PyRuntimeError::new_err(format!("{env_var} is not set and no path was provided"))
            })?,
        };
        resolve_path(raw_path)
            .map(|path| path.to_string_lossy().to_string())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))
    }

    /// Entry point for GlueX photon-flux and luminosity calculations.
    #[pyclass(name = "Luminosity", module = "gluex.lumi")]
    pub struct PyLuminosity(Luminosity);

    #[pymethods]
    impl PyLuminosity {
        #[new]
        #[pyo3(signature = (rcdb=None, ccdb=None))]
        fn new(rcdb: Option<String>, ccdb: Option<String>) -> PyResult<Self> {
            let rcdb = resolve_connection_path(rcdb, "RCDB_CONNECTION")?;
            let ccdb = resolve_connection_path(ccdb, "CCDB_CONNECTION")?;
            Ok(Self(Luminosity::new(rcdb, ccdb)))
        }

        /// Calculate flux and luminosity histograms for selected runs.
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (edges, *, runs, rest_version=None, coherent_peak=false, polarized=false, exclude_runs=None))]
        fn fetch(
            &self,
            py: Python<'_>,
            edges: Vec<f64>,
            runs: Vec<RunNumber>,
            rest_version: Option<Bound<'_, PyAny>>,
            coherent_peak: bool,
            polarized: bool,
            exclude_runs: Option<Vec<RunNumber>>,
        ) -> PyResult<Py<PyFluxHistograms>> {
            let mut context = LuminosityContext::new(runs, parse_rest_versions(rest_version)?)
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
                .with_coherent_peak(coherent_peak)
                .with_polarized(polarized);
            if let Some(excluded) = exclude_runs {
                context = context.with_exclude_runs(excluded);
            }
            let histograms = self
                .0
                .fetch(&edges, &context)
                .map_err(|err: LuminosityError| PyRuntimeError::new_err(err.to_string()))?;
            Py::new(py, PyFluxHistograms(histograms))
        }
    }
}
