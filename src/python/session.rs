use std::path::PathBuf;

use pyo3::prelude::*;

use super::{ccdb::ccdb::PyCCDB, rcdb::rcdb::PyRCDB};
use crate::{CacheInfo, Capabilities, GlueX, GlueXError, SourceConfig, Sources};

/// Inspectable cache bounds and current occupancy.
#[pyclass(name = "CacheInfo", module = "gluex", frozen)]
pub struct PyCacheInfo(CacheInfo);

#[pymethods]
impl PyCacheInfo {
    #[getter]
    fn calibration_payload_capacity(&self) -> usize {
        self.0.calibration_payload_capacity()
    }
    #[getter]
    fn ccdb_metadata_entries(&self) -> usize {
        self.0.ccdb_metadata_entries()
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

fn session_error(error: GlueXError) -> PyErr {
    super::exceptions::map(&error)
}

/// Sentinel type for explicitly disabling a database; use gluex.DISABLED.
#[pyclass(name = "DisabledSource", module = "gluex", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyDisabled;

#[pymethods]
impl PyDisabled {
    fn __repr__(&self) -> &'static str {
        "gluex.DISABLED"
    }
}

#[derive(FromPyObject)]
pub enum SourceArgument {
    Disabled(PyDisabled),
    Path(PathBuf),
}

impl From<SourceArgument> for SourceConfig {
    fn from(value: SourceArgument) -> Self {
        match value {
            SourceArgument::Disabled(_disabled) => Self::Disabled,
            SourceArgument::Path(path) => Self::Sqlite(path),
        }
    }
}

/// Read-only description of configured database capabilities.
#[pyclass(name = "Capabilities", module = "gluex", frozen)]
pub struct PyCapabilities(Capabilities);

#[pymethods]
impl PyCapabilities {
    /// Whether recorded runs and conditions are available.
    #[getter]
    fn rcdb(&self) -> bool {
        self.0.rcdb()
    }

    /// Whether calibration metadata and payloads are available.
    #[getter]
    fn ccdb(&self) -> bool {
        self.0.ccdb()
    }

    fn __repr__(&self) -> String {
        format!(
            "Capabilities(rcdb={}, ccdb={})",
            if self.0.rcdb() { "True" } else { "False" },
            if self.0.ccdb() { "True" } else { "False" }
        )
    }
}

/// Database-native readers sharing the session's connections and metadata.
#[pyclass(name = "Sources", module = "gluex", frozen)]
pub struct PySources(Sources);

#[pymethods]
impl PySources {
    /// RCDB reader. Raises RuntimeError if RCDB is unconfigured.
    #[getter]
    fn rcdb(&self) -> PyResult<PyRCDB> {
        self.0.rcdb().cloned().map(PyRCDB).map_err(session_error)
    }

    /// CCDB reader. Raises RuntimeError if CCDB is unconfigured.
    #[getter]
    fn ccdb(&self) -> PyResult<PyCCDB> {
        self.0.ccdb().cloned().map(PyCCDB).map_err(session_error)
    }

    fn __repr__(&self) -> String {
        format!(
            "Sources(rcdb={}, ccdb={})",
            self.0
                .rcdb()
                .map_or("unavailable", crate::rcdb::RCDB::connection_path),
            self.0
                .ccdb()
                .map_or("unavailable", crate::ccdb::CCDB::connection_path)
        )
    }
}

/// GlueX session with independently optional RCDB and CCDB sources.
///
/// Paths override environment variables. None consults RCDB_CONNECTION or
/// CCDB_CONNECTION independently; DISABLED suppresses that lookup. Invalid
/// configured sources raise ValueError immediately. Keep SQLite files
/// unchanged while using a session or its readers. Reference types such as
/// gluex.RunPeriod and gluex.Particle remain available without databases.
#[pyclass(name = "GlueX", module = "gluex")]
pub struct PyGlueX(GlueX);

#[pymethods]
impl PyGlueX {
    /// Inspect cache bounds and current shared metadata occupancy.
    #[getter]
    fn cache_info(&self) -> PyCacheInfo {
        PyCacheInfo(self.0.cache_info())
    }

    /// Set the per-stream decoded calibration payload budget; the minimum is one.
    fn set_calibration_payload_cache_capacity(&self, capacity: usize) {
        self.0.set_calibration_payload_cache_capacity(capacity);
    }

    /// Clear disposable CCDB metadata caches; collected results remain immutable.
    fn clear_caches(&self) {
        self.0.clear_caches();
    }
    /// Canonical GlueX workflows bound to this session's captured sources.
    #[getter]
    fn workflows(&self) -> super::workflows::PyWorkflows {
        super::workflows::PyWorkflows(self.0.workflows())
    }
    /// Build the canonical luminosity workflow from explicit inputs.
    #[pyo3(signature = (runs, *, reconstruction: "CalibratedRunPeriod | RESTVersionSelection | ReconstructionSelection | Mapping[RunPeriod | str, int | RESTVersionSelection]", edges))]
    fn luminosity(
        &self,
        runs: &super::runs::PyRunSet,
        reconstruction: &Bound<'_, PyAny>,
        edges: Vec<f64>,
    ) -> PyResult<super::workflows::PyLuminosityQuery> {
        Ok(super::workflows::PyLuminosityQuery(
            self.0.workflows().luminosity(
                &runs.0,
                super::calibrations::parse_reconstruction(reconstruction)?,
                edges,
            ),
        ))
    }
    /// Discover the vocabulary reserved for future experiment operations.
    #[getter]
    fn operations(&self) -> super::workflows::PyOperations {
        super::workflows::PyOperations
    }
    /// Reopen captured paths and renew defaults for new queries. Releases the GIL.
    /// Existing handles/results keep their bindings. Environment is not re-read.
    /// Failure raises ValueError and leaves this session unchanged. Keep files unchanged
    /// while in use; finish old-file work before replacement. No historical copies are kept.
    fn refresh(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.0.refresh()).map_err(session_error)
    }

    /// Full-path calibration catalog. Requires CCDB; never requires RCDB.
    #[getter]
    fn calibrations(&self) -> PyResult<super::calibrations::PyCalibrationCatalog> {
        self.0
            .calibrations()
            .map(super::calibrations::PyCalibrationCatalog)
            .map_err(session_error)
    }

    #[new]
    #[pyo3(signature = (*, rcdb=None, ccdb=None))]
    fn new(
        py: Python<'_>,
        rcdb: Option<SourceArgument>,
        ccdb: Option<SourceArgument>,
    ) -> PyResult<Self> {
        open(py, rcdb, ccdb)
    }

    /// Discover and query recorded runs. Attribute access performs no run-value query.
    #[getter]
    fn runs(&self) -> super::runs::PyRuns {
        super::runs::PyRuns(self.0.clone())
    }

    /// Inspect source availability without querying database contents.
    #[getter]
    fn capabilities(&self) -> PyCapabilities {
        PyCapabilities(self.0.capabilities())
    }

    /// Access the database-native reading APIs through shared source handles.
    #[getter]
    fn sources(&self) -> PySources {
        PySources(self.0.sources().clone())
    }

    fn __repr__(&self) -> String {
        self.0.to_string()
    }
}

/// Open a GlueX session from optional local SQLite paths.
///
/// Each omitted/None argument consults its own RCDB_CONNECTION or
/// CCDB_CONNECTION variable. DISABLED explicitly turns that source off. Missing
/// variables leave capabilities unavailable; broken configured files raise
/// ValueError. Strings and pathlib.Path values are accepted. No
/// database is required for using database-independent reference information.
#[pyfunction]
#[pyo3(signature = (*, rcdb=None, ccdb=None))]
pub fn open(
    py: Python<'_>,
    rcdb: Option<SourceArgument>,
    ccdb: Option<SourceArgument>,
) -> PyResult<PyGlueX> {
    let rcdb = rcdb.map_or(SourceConfig::FromEnv, Into::into);
    let ccdb = ccdb.map_or(SourceConfig::FromEnv, Into::into);
    py.detach(move || GlueX::open(rcdb, ccdb))
        .map(PyGlueX)
        .map_err(session_error)
}
