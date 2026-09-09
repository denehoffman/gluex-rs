use super::{
    runs::{PyRunProvenance, PyRunQuery, PyRunReport, PyRunSelection, PyRunSet},
    tuple::{TypedIterator, TypedTuple},
};
use crate::calibrations::*;
use crate::{Id, RunNumber};
use pyo3::{
    exceptions::{PyKeyError, PyRuntimeError},
    prelude::*,
    types::PyDict,
};

#[derive(FromPyObject)]
enum PyCalibrationInput {
    Selection(PyRunSelection),
    Set(PyRunSet),
    Query(PyRunQuery),
}

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
    fn for_runs(&self, selection: PyCalibrationInput) -> PyResult<PyCalibrationQuery> {
        match selection {
            PyCalibrationInput::Selection(selection) => self.0.for_runs(selection.0),
            PyCalibrationInput::Set(runs) => self.0.for_run_set(&runs.0),
            PyCalibrationInput::Query(query) => self.0.for_query(&query.0),
        }
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

/// Explicit latest or per-period REST reconstruction selection.
#[pyclass(
    name = "ReconstructionSelection",
    module = "gluex",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyReconstructionSelection(pub(crate) ReconstructionSelection);
#[pymethods]
impl PyReconstructionSelection {
    #[staticmethod]
    fn latest() -> Self {
        Self(ReconstructionSelection::latest())
    }
    #[staticmethod]
    fn periods(selections: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut native = Vec::with_capacity(selections.len());
        for (key, value) in selections.iter() {
            let period = key.extract::<PyRef<'_, super::core::PyRunPeriod>>()?;
            let selection = value.extract::<PyRef<'_, super::core::PyRESTVersionSelection>>()?;
            native.push((period.0, selection.0));
        }
        Ok(Self(ReconstructionSelection::periods(native)))
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
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
    fn runs(&self) -> Option<PyRunProvenance> {
        self.0.runs().cloned().map(PyRunProvenance)
    }
    #[getter]
    fn run_report(&self) -> Option<PyRunReport> {
        self.0.run_report().cloned().map(PyRunReport)
    }
    #[getter]
    fn variation(&self) -> &str {
        self.0.variation()
    }
    #[getter]
    fn as_of(&self) -> chrono::DateTime<chrono::Utc> {
        self.0.as_of()
    }
    #[getter]
    fn resolved_reconstruction(
        &self,
    ) -> std::collections::BTreeMap<String, (String, chrono::DateTime<chrono::Utc>)> {
        self.0
            .resolved_reconstruction()
            .iter()
            .map(|(period, context)| {
                (
                    period.data_name().to_owned(),
                    (context.variation.clone(), context.timestamp),
                )
            })
            .collect()
    }
    #[getter]
    fn missing_policy(&self) -> &'static str {
        self.0.policy().as_str()
    }
    #[getter]
    fn fallback_run(&self) -> Option<RunNumber> {
        self.0.fallback_run()
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
    /// Return an immutable query with a timeout in seconds.
    fn timeout(&self, seconds: f64) -> PyResult<Self> {
        Ok(Self(
            self.0
                .with_timeout(crate::python::execution::timeout(seconds)?),
        ))
    }
    /// Return a new query requesting this variation; invalid variations fail on collection.
    fn with_variation(&self, variation: String) -> Self {
        Self(self.0.with_variation(variation))
    }
    /// Return a new query with an inclusive cutoff. Requires a timezone-aware datetime.
    fn as_of(&self, timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        Self(self.0.as_of(timestamp))
    }
    /// Resolve calibration selectors independently for each run period.
    fn with_reconstruction(&self, selection: &PyReconstructionSelection) -> Self {
        Self(self.0.with_reconstruction(selection.0.clone()))
    }
    /// Explicitly use source-opening defaults for all run periods.
    fn latest_reconstruction(&self) -> Self {
        Self(
            self.0
                .with_reconstruction(ReconstructionSelection::latest()),
        )
    }
    fn strict(&self) -> Self {
        Self(self.0.strict())
    }
    fn fallback_to(&self, run: RunNumber) -> Self {
        Self(self.0.fallback_to(run))
    }

    #[getter]
    fn provenance(&self) -> PyCalibrationProvenance {
        PyCalibrationProvenance(self.0.provenance().clone())
    }
    /// Collect numeric assignments and report missing runs. Releases the GIL.
    /// Execution and malformed-payload errors raise RuntimeError.
    fn collect(&self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        let signals = crate::python::execution::PythonExecution::new();
        let query = self.0.with_interrupt_check(signals.checker());
        signals
            .finish(py.detach(|| query.collect()))
            .map(PyCalibrationSeries)
    }
    #[pyo3(signature = (*, chunk_size=1024))]
    fn stream(&self, chunk_size: usize) -> PyResult<PyCalibrationStream> {
        let signals = crate::python::execution::PythonExecution::new();
        self.0
            .with_interrupt_check(signals.checker())
            .stream(chunk_size)
            .map(|stream| PyCalibrationStream(stream, signals))
            .map_err(error)
    }
    fn first(&self, py: Python<'_>) -> PyResult<Option<PyCalibrationSeries>> {
        let signals = crate::python::execution::PythonExecution::new();
        let query = self.0.with_interrupt_check(signals.checker());
        signals
            .finish(py.detach(|| query.first()))
            .map(|value| value.map(PyCalibrationSeries))
    }
    fn one(&self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        let signals = crate::python::execution::PythonExecution::new();
        let query = self.0.with_interrupt_check(signals.checker());
        signals
            .finish(py.detach(|| query.one()))
            .map(PyCalibrationSeries)
    }
    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        let signals = crate::python::execution::PythonExecution::new();
        let query = self.0.with_interrupt_check(signals.checker());
        signals.finish(py.detach(|| query.count()))
    }
    fn __repr__(&self) -> String {
        format!("CalibrationQuery({:?})", self.0.provenance())
    }
}

/// Iterator yielding bounded Calibration Series chunks.
#[pyclass(name = "CalibrationStream", module = "gluex")]
pub struct PyCalibrationStream(CalibrationStream, crate::python::execution::PythonExecution);
#[pymethods]
impl PyCalibrationStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyCalibrationSeries>> {
        Ok(self
            .1
            .finish(py.detach(|| self.0.next().transpose()))?
            .map(PyCalibrationSeries))
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
    #[getter]
    fn substitutions(&self) -> TypedTuple<(RunNumber, RunNumber)> {
        TypedTuple(self.0.substitutions().to_vec())
    }
    #[getter]
    fn evaluated_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.evaluated_runs().to_vec())
    }
    #[getter]
    fn complete(&self) -> bool {
        self.0.complete()
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}
