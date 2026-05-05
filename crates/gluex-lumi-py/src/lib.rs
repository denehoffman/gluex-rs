use std::{collections::HashMap, env, error::Error, str::FromStr};

use ::gluex_lumi as lumi_crate;
use chrono::{DateTime, Utc};
use gluex_core::{
    RESTVersion, RunNumber,
    histograms::{Histogram, validate_edges},
    run_periods::RunPeriod,
    utils::resolve_path,
};
use lumi_crate::{
    FluxHistograms as RustFluxHistograms, Luminosity as RustLuminosity,
    LuminosityContext as RustContext, LuminosityError, RESTVersionSelection,
};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyDict, PyModule},
};

#[pyclass(module = "gluex_lumi", name = "Histogram")]
pub struct PyHistogram {
    #[pyo3(get)]
    counts: Vec<f64>,
    #[pyo3(get)]
    edges: Vec<f64>,
    #[pyo3(get)]
    errors: Vec<f64>,
}

impl PyHistogram {
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("counts", self.counts.clone())?;
        dict.set_item("edges", self.edges.clone())?;
        dict.set_item("errors", self.errors.clone())?;
        Ok(dict.unbind())
    }
}

#[pymethods]
impl PyHistogram {
    #[new]
    fn new(counts: Vec<f64>, edges: Vec<f64>, errors: Vec<f64>) -> Self {
        Self {
            counts,
            edges,
            errors,
        }
    }

    pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.to_dict(py)
    }
}

#[pyclass(module = "gluex_lumi", name = "FluxHistograms")]
pub struct PyFluxHistograms {
    #[pyo3(get)]
    tagged_flux: Py<PyHistogram>,
    #[pyo3(get)]
    tagm_flux: Py<PyHistogram>,
    #[pyo3(get)]
    tagh_flux: Py<PyHistogram>,
    #[pyo3(get)]
    tagged_luminosity: Py<PyHistogram>,
}

impl PyFluxHistograms {
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        let tagged_flux = self.tagged_flux.bind(py);
        let tagm_flux = self.tagm_flux.bind(py);
        let tagh_flux = self.tagh_flux.bind(py);
        let tagged_luminosity = self.tagged_luminosity.bind(py);
        dict.set_item("tagged_flux", tagged_flux.borrow().to_dict(py)?)?;
        dict.set_item("tagm_flux", tagm_flux.borrow().to_dict(py)?)?;
        dict.set_item("tagh_flux", tagh_flux.borrow().to_dict(py)?)?;
        dict.set_item("tagged_luminosity", tagged_luminosity.borrow().to_dict(py)?)?;
        Ok(dict.unbind())
    }
}

#[pymethods]
impl PyFluxHistograms {
    #[new]
    fn new(
        tagged_flux: Py<PyHistogram>,
        tagm_flux: Py<PyHistogram>,
        tagh_flux: Py<PyHistogram>,
        tagged_luminosity: Py<PyHistogram>,
    ) -> Self {
        Self {
            tagged_flux,
            tagm_flux,
            tagh_flux,
            tagged_luminosity,
        }
    }

    pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.to_dict(py)
    }
}

fn py_lumi_error(err: LuminosityError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn parse_run_periods(obj: &Bound<'_, PyAny>) -> PyResult<HashMap<RunPeriod, RESTVersionSelection>> {
    let mapping = obj.cast::<PyDict>().map_err(|_| {
        PyRuntimeError::new_err(
            "run_periods must map run-period names to REST versions (int), datetime, or None",
        )
    })?;
    let mut selection = HashMap::with_capacity(mapping.len());
    for (name, rest_version) in mapping.iter() {
        let name = name
            .extract::<String>()
            .map_err(|_| PyRuntimeError::new_err("run_period names must be strings"))?;
        let period =
            RunPeriod::from_str(&name).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let request = if rest_version.is_none() {
            RESTVersionSelection::Current
        } else if let Ok(version) = rest_version.extract::<RESTVersion>() {
            RESTVersionSelection::try_new(period, version)
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
        } else if let Ok(timestamp) = rest_version.extract::<DateTime<Utc>>() {
            RESTVersionSelection::from_timestamp(timestamp)
        } else {
            return Err(PyRuntimeError::new_err(
                "run_periods must map run-period names to REST versions (int), datetime, or None",
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

/// Luminosity query context.
///
/// Parameters
/// ----------
/// runs : Sequence[int]
///     Explicit run numbers to include in the calculation.
/// rest_version : Mapping[str, int | datetime | None], optional
///     Mapping from run-period short names (e.g. "f18") to REST versions, timestamps, or None.
/// coherent_peak : bool, optional
///     If true, only retain photons in the coherent peak for each run.
/// polarized : bool, optional
///     Use the polarized flux calibration constants when true.
/// exclude_runs : Sequence[int], optional
///     Run numbers to skip when computing the histograms.
#[pyclass(module = "gluex_lumi", name = "Context")]
pub struct PyContext {
    inner: RustContext,
}

#[pymethods]
impl PyContext {
    #[new]
    #[pyo3(signature = (runs, rest_version=None, *, coherent_peak=false, polarized=false, exclude_runs=None))]
    fn new(
        runs: Vec<RunNumber>,
        rest_version: Option<Bound<'_, PyAny>>,
        coherent_peak: bool,
        polarized: bool,
        exclude_runs: Option<Vec<RunNumber>>,
    ) -> PyResult<Self> {
        let rest_version_map = match rest_version {
            Some(value) => parse_run_periods(&value)?,
            None => HashMap::new(),
        };
        let mut ctx = RustContext::new(runs, rest_version_map)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
            .with_coherent_peak(coherent_peak)
            .with_polarized(polarized);
        if let Some(runs) = exclude_runs {
            ctx = ctx.with_exclude_runs(runs);
        }
        Ok(Self { inner: ctx })
    }
}

/// Luminosity calculator entry point.
///
/// Parameters
/// ----------
/// rcdb : str, optional
///     Path to the RCDB SQLite database. Defaults to ``RCDB_CONNECTION``.
/// ccdb : str, optional
///     Path to the CCDB SQLite database. Defaults to ``CCDB_CONNECTION``.
#[pyclass(module = "gluex_lumi", name = "Luminosity")]
pub struct PyLuminosity {
    inner: RustLuminosity,
}

#[pymethods]
impl PyLuminosity {
    #[new]
    #[pyo3(signature = (rcdb=None, ccdb=None), text_signature = "(rcdb=None, ccdb=None)")]
    fn new(rcdb: Option<String>, ccdb: Option<String>) -> PyResult<Self> {
        let rcdb = resolve_connection_path(rcdb, "RCDB_CONNECTION")?;
        let ccdb = resolve_connection_path(ccdb, "CCDB_CONNECTION")?;
        Ok(Self {
            inner: RustLuminosity::new(rcdb, ccdb),
        })
    }

    /// fetch(self, edges, ctx)
    ///
    /// Parameters
    /// ----------
    /// edges : Sequence[float]
    ///     Monotonically increasing photon-energy bin edges.
    /// ctx : Context
    ///     Luminosity run context.
    ///
    /// Returns
    /// -------
    /// FluxHistograms
    ///     Flux and luminosity histograms for the selected runs.
    pub fn fetch(
        &self,
        py: Python<'_>,
        edges: Vec<f64>,
        ctx: &PyContext,
    ) -> PyResult<Py<PyFluxHistograms>> {
        validate_edges(&edges).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let histograms = self
            .inner
            .fetch(&edges, &ctx.inner)
            .map_err(py_lumi_error)?;
        flux_histograms_to_py(py, &histograms)
    }
}

fn histogram_to_py(py: Python<'_>, hist: &Histogram) -> PyResult<Py<PyHistogram>> {
    Py::new(
        py,
        PyHistogram {
            counts: hist.counts.clone(),
            edges: hist.edges.clone(),
            errors: hist.errors.clone(),
        },
    )
}

fn flux_histograms_to_py(
    py: Python<'_>,
    flux: &RustFluxHistograms,
) -> PyResult<Py<PyFluxHistograms>> {
    let tagged_flux = histogram_to_py(py, &flux.tagged_flux)?;
    let tagm_flux = histogram_to_py(py, &flux.tagm_flux)?;
    let tagh_flux = histogram_to_py(py, &flux.tagh_flux)?;
    let tagged_luminosity = histogram_to_py(py, &flux.tagged_luminosity)?;
    Py::new(
        py,
        PyFluxHistograms {
            tagged_flux,
            tagm_flux,
            tagh_flux,
            tagged_luminosity,
        },
    )
}

/// cli()
///
/// Notes
/// -----
/// Mirrors the Rust ``gluex-lumi`` executable so that ``python -m pip install gluex-lumi``
/// also exposes the command-line interface.
#[pyfunction(name = "cli")]
pub fn py_cli(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    let argv: Vec<String> = sys.getattr("argv")?.extract()?;
    lumi_crate::cli::run_with_args(argv)
        .map_err(|err: Box<dyn Error>| PyRuntimeError::new_err(err.to_string()))
}

#[pymodule]
/// gluex_lumi
///
/// Python bindings for the GlueX luminosity utilities.
pub fn gluex_lumi(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_cli, m)?)?;
    m.add_class::<PyContext>()?;
    m.add_class::<PyLuminosity>()?;
    m.add_class::<PyHistogram>()?;
    m.add_class::<PyFluxHistograms>()?;
    let version = env!("CARGO_PKG_VERSION");
    m.add("__version__", version)?;
    Ok(())
}
