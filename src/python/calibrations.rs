use super::{
    runs::PyRunSelection,
    tuple::{TypedIterator, TypedTuple},
};
use crate::calibrations::*;
use crate::{Id, RunNumber};
use pyo3::{
    exceptions::{PyKeyError, PyRuntimeError},
    prelude::*,
};

fn error(e: crate::ccdb::CCDBError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Immutable full-path mapping of calibration definitions; discovery does not fetch constants.
#[pyclass(name = "CalibrationCatalog", module = "gluex", frozen)]
pub struct PyCalibrationCatalog(pub(crate) CalibrationCatalog);
#[pymethods]
impl PyCalibrationCatalog {
    fn keys(&self) -> TypedTuple<String> {
        TypedTuple(self.0.keys().cloned().collect())
    }
    fn items(&self) -> TypedTuple<(String, PyCalibrationTable)> {
        TypedTuple(
            self.0
                .items()
                .map(|(k, v)| (k.clone(), PyCalibrationTable(v.clone())))
                .collect(),
        )
    }
    fn __getitem__(&self, path: &str) -> PyResult<PyCalibrationTable> {
        self.0
            .get(path)
            .cloned()
            .map(PyCalibrationTable)
            .ok_or_else(|| PyKeyError::new_err(path.to_owned()))
    }
    fn __contains__(&self, path: &str) -> bool {
        self.0.get(path).is_some()
    }
    fn __iter__(&self) -> TypedIterator<String> {
        TypedIterator(self.0.keys().cloned().collect())
    }
    fn __len__(&self) -> usize {
        self.0.keys().len()
    }
    /// Independent directory mapping keyed by absolute paths.
    #[getter]
    fn directories(&self) -> std::collections::BTreeMap<String, PyCalibrationDirectory> {
        self.0
            .directories()
            .iter()
            .map(|(k, v)| (k.clone(), PyCalibrationDirectory(v.clone())))
            .collect()
    }
    fn __repr__(&self) -> String {
        format!("CalibrationCatalog(tables={})", self.__len__())
    }
}

/// Immutable calibration directory definition with independent child mappings.
#[pyclass(name = "CalibrationDirectory", module = "gluex", frozen)]
pub struct PyCalibrationDirectory(CalibrationDirectory);
#[pymethods]
impl PyCalibrationDirectory {
    #[getter]
    fn path(&self) -> &str {
        self.0.path()
    }
    /// Child names mapped to absolute directory paths; editing the copy cannot change the catalog.
    #[getter]
    fn directories(&self) -> std::collections::BTreeMap<String, String> {
        self.0.directories().clone()
    }
    /// Local names mapped to immutable table definitions.
    #[getter]
    fn tables(&self) -> std::collections::BTreeMap<String, PyCalibrationTable> {
        self.0
            .tables()
            .iter()
            .map(|(k, v)| (k.clone(), PyCalibrationTable(v.clone())))
            .collect()
    }
    fn __repr__(&self) -> String {
        format!("CalibrationDirectory({:?})", self.0.path())
    }
}

/// Source-bound table definition; columns and metadata do not fetch constants.
#[pyclass(name = "CalibrationTable", module = "gluex", frozen)]
pub struct PyCalibrationTable(CalibrationTable);
#[pymethods]
impl PyCalibrationTable {
    #[getter]
    fn path(&self) -> String {
        self.0.path()
    }
    #[getter]
    fn id(&self) -> Id {
        self.0.metadata().id()
    }
    #[getter]
    fn description(&self) -> &str {
        self.0.metadata().comment()
    }
    #[getter]
    fn n_rows(&self) -> i64 {
        self.0.metadata().n_rows()
    }
    /// Ordered named columns; raises RuntimeError for invalid metadata.
    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<TypedTuple<PyCalibrationColumn>> {
        py.detach(|| self.0.columns())
            .map(|c| TypedTuple(c.into_iter().map(PyCalibrationColumn).collect()))
            .map_err(error)
    }
    /// Build a lazy numeric query using captured opening defaults; no RCDB membership check.
    fn for_runs(&self, selection: &PyRunSelection) -> PyResult<PyCalibrationQuery> {
        self.0
            .for_runs(selection.0.clone())
            .map(PyCalibrationQuery)
            .map_err(error)
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationTable(path={:?}, rows={})",
            self.path(),
            self.n_rows()
        )
    }
}

/// Immutable named calibration column definition.
#[pyclass(name = "CalibrationColumn", module = "gluex", frozen)]
pub struct PyCalibrationColumn(crate::ccdb::ColumnMeta);
#[pymethods]
impl PyCalibrationColumn {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }
    #[getter]
    fn value_type(&self) -> String {
        self.0.column_type().to_string()
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationColumn(name={:?}, type={})",
            self.name(),
            self.value_type()
        )
    }
}

/// Captured source, table, numeric scope, variation and effective time.
#[pyclass(name = "CalibrationProvenance", module = "gluex", frozen)]
pub struct PyCalibrationProvenance(CalibrationProvenance);
#[pymethods]
impl PyCalibrationProvenance {
    #[getter]
    fn source(&self) -> &str {
        self.0.source()
    }
    #[getter]
    fn table(&self) -> &str {
        self.0.table()
    }
    #[getter]
    fn selection(&self) -> PyRunSelection {
        PyRunSelection(self.0.selection().clone())
    }
    #[getter]
    fn variation(&self) -> &str {
        self.0.variation()
    }
    #[getter]
    fn as_of(&self) -> chrono::DateTime<chrono::Utc> {
        self.0.as_of()
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

/// Reusable lazy request. Call collect explicitly to retrieve constants.
#[pyclass(name = "CalibrationQuery", module = "gluex", frozen)]
pub struct PyCalibrationQuery(CalibrationQuery);
#[pymethods]
impl PyCalibrationQuery {
    #[getter]
    fn provenance(&self) -> PyCalibrationProvenance {
        PyCalibrationProvenance(self.0.provenance().clone())
    }
    /// Collect numeric assignments and report missing runs. Releases the GIL.
    /// Execution and malformed-payload errors raise RuntimeError.
    fn collect(&self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        py.detach(|| self.0.collect())
            .map(PyCalibrationSeries)
            .map_err(error)
    }
    fn __repr__(&self) -> String {
        format!("CalibrationQuery({:?})", self.0.provenance())
    }
}

/// Immutable run-to-assignment association. Runs are numeric; RCDB is not consulted.
#[pyclass(name = "CalibrationSeries", module = "gluex", frozen)]
pub struct PyCalibrationSeries(CalibrationSeries);
#[pymethods]
impl PyCalibrationSeries {
    #[getter]
    fn runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.items().map(|(r, _)| *r).collect())
    }
    fn __len__(&self) -> usize {
        self.0.items().len()
    }
    fn __getitem__(&self, run: RunNumber) -> PyResult<PyCalibrationEntry> {
        self.0
            .get(run)
            .cloned()
            .map(PyCalibrationEntry)
            .ok_or_else(|| PyKeyError::new_err(run))
    }
    fn __iter__(&self) -> TypedIterator<RunNumber> {
        TypedIterator(self.0.items().map(|(r, _)| *r).collect())
    }
    fn items(&self) -> TypedTuple<(RunNumber, PyCalibrationEntry)> {
        TypedTuple(
            self.0
                .items()
                .map(|(r, e)| (*r, PyCalibrationEntry(e.clone())))
                .collect(),
        )
    }
    #[getter]
    fn provenance(&self) -> PyCalibrationProvenance {
        PyCalibrationProvenance(self.0.provenance().clone())
    }
    #[getter]
    fn report(&self) -> PyCalibrationReport {
        PyCalibrationReport(self.0.report().clone())
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationSeries(runs={}, table={:?})",
            self.__len__(),
            self.0.provenance().table()
        )
    }
}

/// Immutable effective assignment metadata and shared payload.
#[pyclass(name = "CalibrationEntry", module = "gluex", frozen)]
pub struct PyCalibrationEntry(CalibrationEntry);
#[pymethods]
impl PyCalibrationEntry {
    #[getter]
    fn assignment_id(&self) -> Id {
        self.0.assignment_id()
    }
    #[getter]
    fn constant_set_id(&self) -> Id {
        self.0.constant_set_id()
    }
    #[getter]
    fn created(&self) -> chrono::DateTime<chrono::Utc> {
        self.0.created()
    }
    #[getter]
    fn variation(&self) -> &str {
        self.0.variation()
    }
    #[getter]
    fn run_range(&self) -> (RunNumber, RunNumber) {
        self.0.run_range()
    }
    #[getter]
    fn payload(&self) -> PyCalibrationPayload {
        PyCalibrationPayload(self.0.clone())
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationEntry(assignment_id={}, constant_set_id={})",
            self.assignment_id(),
            self.constant_set_id()
        )
    }
}

#[derive(IntoPyObject)]
pub enum CalibrationScalar {
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Text(String),
}

/// Immutable tabular constants shared between entries for the same constant set.
#[pyclass(name = "CalibrationPayload", module = "gluex", frozen)]
pub struct PyCalibrationPayload(CalibrationEntry);
#[pymethods]
impl PyCalibrationPayload {
    #[getter]
    fn columns(&self) -> TypedTuple<String> {
        TypedTuple(self.0.payload().column_names().to_vec())
    }
    fn __len__(&self) -> usize {
        self.0.payload().n_rows()
    }
    /// Return a named column as an immutable tuple; unknown names raise KeyError.
    fn column(&self, name: &str) -> PyResult<TypedTuple<CalibrationScalar>> {
        use crate::ccdb::Column;
        let c = self
            .0
            .payload()
            .named_column(name)
            .ok_or_else(|| PyKeyError::new_err(name.to_owned()))?;
        Ok(TypedTuple(match c {
            Column::Int(v) => v
                .iter()
                .map(|v| CalibrationScalar::Int(i64::from(*v)))
                .collect(),
            Column::UInt(v) => v
                .iter()
                .map(|v| CalibrationScalar::UInt(u64::from(*v)))
                .collect(),
            Column::Long(v) => v.iter().map(|v| CalibrationScalar::Int(*v)).collect(),
            Column::ULong(v) => v.iter().map(|v| CalibrationScalar::UInt(*v)).collect(),
            Column::Double(v) => v.iter().map(|v| CalibrationScalar::Float(*v)).collect(),
            Column::Bool(v) => v.iter().map(|v| CalibrationScalar::Bool(*v)).collect(),
            Column::String(v) => v
                .iter()
                .map(|v| CalibrationScalar::Text(v.clone()))
                .collect(),
        }))
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationPayload(rows={}, columns={:?})",
            self.__len__(),
            self.0.payload().column_names()
        )
    }
}

/// Completed missing-assignment diagnostics.
#[pyclass(name = "CalibrationReport", module = "gluex", frozen)]
pub struct PyCalibrationReport(CalibrationReport);
#[pymethods]
impl PyCalibrationReport {
    #[getter]
    fn missing_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.missing_runs().to_vec())
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}
