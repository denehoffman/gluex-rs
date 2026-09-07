use std::path::PathBuf;

use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};

use super::{ccdb::ccdb::PyCCDB, rcdb::rcdb::PyRCDB};
use crate::{Capabilities, GlueX, GlueXError, SourceConfig, Sources};

fn session_error(error: GlueXError) -> PyErr {
    match error {
        GlueXError::MissingCapability(_) => PyRuntimeError::new_err(error.to_string()),
        GlueXError::Configuration { .. } => PyValueError::new_err(error.to_string()),
    }
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
#[pyclass(name = "GlueX", module = "gluex", frozen)]
pub struct PyGlueX(GlueX);

#[pymethods]
impl PyGlueX {
    #[new]
    #[pyo3(signature = (*, rcdb=None, ccdb=None))]
    fn new(
        py: Python<'_>,
        rcdb: Option<SourceArgument>,
        ccdb: Option<SourceArgument>,
    ) -> PyResult<Self> {
        open(py, rcdb, ccdb)
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
