use super::{
    runs::{PyRunProvenance, PyRunQuery, PyRunReport, PyRunSelection, PyRunSet, coerce_run_scope},
    tuple::{TypedIterator, TypedTuple},
};
use crate::calibrations::*;
use crate::{Id, RESTVersionSelection, RunNumber};
use pyo3::{
    exceptions::{PyKeyError, PyStopIteration, PyValueError},
    prelude::*,
    types::{PyAny, PyDict, PyTuple},
};

fn error(error: crate::DatabaseError) -> PyErr {
    super::exceptions::map(&error)
}

fn reconstruction_from_dict(selections: &Bound<'_, PyDict>) -> PyResult<ReconstructionSelection> {
    let mut native = std::collections::BTreeMap::new();
    for (key, value) in selections.iter() {
        let period = super::core::parse_run_period_object(&key)?;
        let selection = if let Ok(selection) =
            value.extract::<PyRef<'_, super::core::PyRESTVersionSelection>>()
        {
            if selection
                .1
                .is_some_and(|selected_period| selected_period != period)
            {
                return Err(PyValueError::new_err(format!(
                    "REST version selection conflicts with mapping key {}",
                    period.short_name()
                )));
            }
            let mut requested = ReconstructionPeriod::new(selection.0);
            if let Some(variation) = &selection.2 {
                requested = requested.with_variation(variation);
            }
            requested
        } else {
            let version = value.extract()?;
            ReconstructionPeriod::new(
                crate::RESTVersionSelection::try_new(period, version)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
        };
        if native.insert(period, selection).is_some() {
            return Err(PyValueError::new_err(format!(
                "duplicate reconstruction meaning for run period {}",
                period.short_name()
            )));
        }
    }
    Ok(ReconstructionSelection::periods(native))
}

pub(crate) fn parse_reconstruction(value: &Bound<'_, PyAny>) -> PyResult<ReconstructionSelection> {
    if let Ok(selection) = value.extract::<PyReconstructionSelection>() {
        return Ok(selection.0);
    }
    if let Ok(selections) = value.cast::<PyDict>() {
        return reconstruction_from_dict(selections);
    }
    if let Ok(period) = value.extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>() {
        return Ok(ReconstructionSelection::periods([(
            period.0.period(),
            period.0.reconstruction().clone(),
        )]));
    }
    if let Ok(selection) = value.extract::<PyRef<'_, super::core::PyRESTVersionSelection>>() {
        let period = selection.1.ok_or_else(|| {
            pyo3::exceptions::PyTypeError::new_err(
                "a direct REST selection must be created by RunPeriod.rest(...) so its period is known",
            )
        })?;
        let mut requested = ReconstructionPeriod::new(selection.0);
        if let Some(variation) = &selection.2 {
            requested = requested.with_variation(variation);
        }
        return Ok(ReconstructionSelection::periods([(period, requested)]));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "reconstruction must be a RunPeriod.rest(...) selection, ReconstructionSelection, or period-to-version mapping",
    ))
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
    fn description(&self) -> String {
        self.0.metadata().description().to_owned()
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
    /// Build a lazy query from a Python run scope, with optional keyword selectors.
    #[pyo3(signature = (
        run_scope: "int | Sequence[int] | range | RunPeriod | str | RunSelection | RunQuery | RunSet",
        *,
        variation=None,
        as_of=None,
        reconstruction: "CalibratedRunPeriod | RESTVersionSelection | ReconstructionSelection | Mapping[RunPeriod | str, int | RESTVersionSelection] | None"=None,
        missing_policy: "Literal['report', 'strict', 'fallback']"="report",
        fallback_run=None
    ))]
    fn for_runs(
        &self,
        run_scope: &Bound<'_, PyAny>,
        variation: Option<String>,
        as_of: Option<chrono::DateTime<chrono::Utc>>,
        reconstruction: Option<&Bound<'_, PyAny>>,
        missing_policy: &str,
        fallback_run: Option<RunNumber>,
    ) -> PyResult<PyCalibrationQuery> {
        let period_reconstruction = run_scope
            .extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>()
            .ok()
            .map(|period| {
                ReconstructionSelection::periods([(
                    period.0.period(),
                    period.0.reconstruction().clone(),
                )])
            });
        if (reconstruction.is_some() || period_reconstruction.is_some())
            && (variation.is_some() || as_of.is_some())
        {
            return Err(PyValueError::new_err(
                "reconstruction conflicts with direct variation or as_of selectors",
            ));
        }
        if reconstruction.is_some() && period_reconstruction.is_some() {
            return Err(PyValueError::new_err(
                "a configured RunPeriod already supplies reconstruction; do not repeat it",
            ));
        }
        let mut query = if let Ok(runs) = run_scope.extract::<PyRunSet>() {
            self.0.for_run_set(&runs.0)
        } else if let Ok(query) = run_scope.extract::<PyRunQuery>() {
            self.0.for_query(&query.0)
        } else {
            self.0.for_runs(coerce_run_scope(run_scope)?)
        }
        .map_err(error)?;
        if let Some(variation) = variation {
            query = query.with_variation(variation);
        }
        if let Some(as_of) = as_of {
            query = query.as_of(as_of);
        }
        if let Some(reconstruction) = reconstruction {
            query = query.with_reconstruction(parse_reconstruction(reconstruction)?);
        } else if let Some(reconstruction) = period_reconstruction {
            query = query.with_reconstruction(reconstruction);
        }
        query = match (missing_policy, fallback_run) {
            ("report", None) => query,
            ("strict", None) => query.strict(),
            ("fallback", Some(run)) => query.fallback_to(run),
            ("fallback", None) => {
                return Err(PyValueError::new_err(
                    "missing_policy='fallback' requires fallback_run",
                ));
            }
            (_, Some(_)) => {
                return Err(PyValueError::new_err(
                    "fallback_run requires missing_policy='fallback'",
                ));
            }
            (policy, None) => {
                return Err(PyValueError::new_err(format!(
                    "missing_policy must be 'report', 'strict', or 'fallback', got {policy:?}"
                )));
            }
        };
        Ok(PyCalibrationQuery(query))
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationTable(path={:?}, rows={}, query=for_runs(run_scope, *, selectors))",
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
    #[pyo3(signature = (*selections: "CalibratedRunPeriod | RESTVersionSelection | Mapping[RunPeriod | str, int | RESTVersionSelection]"))]
    fn periods(selections: &Bound<'_, PyTuple>) -> PyResult<Self> {
        if selections.len() == 1
            && let Ok(mapping) = selections.get_item(0)?.cast::<PyDict>()
        {
            return reconstruction_from_dict(mapping).map(Self);
        }
        let mut native = std::collections::BTreeMap::new();
        for value in selections.iter() {
            if let Ok(period) = value.extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>() {
                if native
                    .insert(period.0.period(), period.0.reconstruction().clone())
                    .is_some()
                {
                    return Err(PyValueError::new_err(format!(
                        "duplicate reconstruction meaning for run period {}",
                        period.0.period().short_name()
                    )));
                }
                continue;
            }
            let selection = value.extract::<PyRef<'_, super::core::PyRESTVersionSelection>>()?;
            let period = selection.1.ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err(
                    "period selections must be created by RunPeriod.rest(...) so their periods are known",
                )
            })?;
            let mut requested = ReconstructionPeriod::new(selection.0);
            if let Some(variation) = &selection.2 {
                requested = requested.with_variation(variation);
            }
            if native.insert(period, requested).is_some() {
                return Err(PyValueError::new_err(format!(
                    "duplicate reconstruction meaning for run period {}",
                    period.short_name()
                )));
            }
        }
        Ok(Self(ReconstructionSelection::periods(native)))
    }
    /// Resolve a period to the exact CCDB variation and effective timestamp.
    #[pyo3(signature = (period: "RunPeriod | str"))]
    fn resolve(&self, period: &Bound<'_, PyAny>) -> PyResult<(String, String)> {
        let period = super::core::parse_run_period_object(period)?;
        let selection = match &self.0 {
            ReconstructionSelection::Latest => {
                ReconstructionPeriod::new(RESTVersionSelection::Current)
            }
            ReconstructionSelection::Periods(selections) => selections
                .get(&period)
                .ok_or_else(|| {
                    PyKeyError::new_err(format!(
                        "no reconstruction selection for {}",
                        period.short_name()
                    ))
                })?
                .clone(),
        };
        let context = selection
            .resolve(period)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok((context.variation, context.timestamp.to_rfc3339()))
    }
    fn __repr__(&self) -> String {
        match &self.0 {
            ReconstructionSelection::Latest => "ReconstructionSelection.latest()".to_owned(),
            ReconstructionSelection::Periods(periods) => format!(
                "ReconstructionSelection.periods(periods={:?})",
                periods
                    .keys()
                    .map(crate::RunPeriod::short_name)
                    .collect::<Vec<_>>()
            ),
        }
    }
}

/// Immutable named calibration column definition.
#[pyclass(name = "CalibrationColumn", module = "gluex", frozen)]
pub struct PyCalibrationColumn(crate::CalibrationColumn);
#[pymethods]
impl PyCalibrationColumn {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }
    #[getter]
    fn value_type(&self) -> String {
        self.0.value_type().as_str().to_owned()
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
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::collect)
            .map(PyCalibrationSeries)
    }
    #[pyo3(signature = (*, chunk_size=1024))]
    fn stream(&self, chunk_size: usize) -> PyResult<PyCalibrationStream> {
        crate::python::execution::PythonExecution::stream(&self.0, |query| query.stream(chunk_size))
            .map(|(stream, signals)| PyCalibrationStream(stream, signals))
    }
    fn first(&self, py: Python<'_>) -> PyResult<Option<PyCalibrationSeries>> {
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::first)
            .map(|value| value.map(PyCalibrationSeries))
    }
    fn one(&self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::one)
            .map(PyCalibrationSeries)
    }
    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::count)
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
    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        self.1
            .finish(py.detach(|| self.0.next().transpose()))?
            .map(PyCalibrationSeries)
            .ok_or_else(|| PyStopIteration::new_err(()))
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
        let c = self
            .0
            .payload()
            .column(name)
            .ok_or_else(|| PyKeyError::new_err(name.to_owned()))?;
        Ok(TypedTuple(match c {
            crate::CalibrationColumnValues::Int(v) => v
                .iter()
                .map(|v| CalibrationScalar::Int(i64::from(*v)))
                .collect(),
            crate::CalibrationColumnValues::UInt(v) => v
                .iter()
                .map(|v| CalibrationScalar::UInt(u64::from(*v)))
                .collect(),
            crate::CalibrationColumnValues::Long(v) => {
                v.iter().map(|v| CalibrationScalar::Int(*v)).collect()
            }
            crate::CalibrationColumnValues::ULong(v) => {
                v.iter().map(|v| CalibrationScalar::UInt(*v)).collect()
            }
            crate::CalibrationColumnValues::Double(v) => {
                v.iter().map(|v| CalibrationScalar::Float(*v)).collect()
            }
            crate::CalibrationColumnValues::Bool(v) => {
                v.iter().map(|v| CalibrationScalar::Bool(*v)).collect()
            }
            crate::CalibrationColumnValues::String(v) => v
                .iter()
                .map(|v| CalibrationScalar::Text(v.clone()))
                .collect(),
        }))
    }
    fn __getitem__(&self, name: &str) -> PyResult<TypedTuple<CalibrationScalar>> {
        self.column(name)
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
