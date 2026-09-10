use super::{
    core::PyRunPeriod,
    tuple::{TypedIterator, TypedTuple},
};
use crate::{
    ConditionCatalog, ConditionDefinition, RunNumber, RunProvenance, RunQuery, RunSelection, RunSet,
};
use pyo3::{
    exceptions::{PyIndexError, PyKeyError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyAny, PyBool, PyTuple},
};

/// Discoverable run-domain entry point bound to one GlueX session.
#[pyclass(name = "Runs", module = "gluex", frozen)]
pub struct PyRuns(pub(crate) crate::GlueX);

fn coerce_run_scope(scope: &Bound<'_, PyAny>) -> PyResult<RunSelection> {
    if let Ok(selection) = scope.extract::<PyRunSelection>() {
        return Ok(selection.0);
    }
    if let Ok(period) = scope.extract::<PyRef<'_, PyRunPeriod>>() {
        return Ok(RunSelection::period(period.0));
    }
    if let Ok(name) = scope.extract::<String>() {
        let period = name
            .parse::<crate::RunPeriod>()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        return Ok(RunSelection::period(period));
    }
    if scope.get_type().name()?.to_str()? == "range" {
        let start = scope.getattr("start")?.extract::<RunNumber>()?;
        let stop = scope.getattr("stop")?.extract::<RunNumber>()?;
        let step = scope.getattr("step")?.extract::<RunNumber>()?;
        if step != 1 {
            return scope
                .try_iter()?
                .map(|value| value?.extract::<RunNumber>())
                .collect::<PyResult<Vec<_>>>()
                .map(RunSelection::runs);
        }
        if start >= stop {
            return Ok(RunSelection::runs([]));
        }
        return Ok(RunSelection::range(start, stop.saturating_sub(1)));
    }
    if !scope.is_instance_of::<PyBool>()
        && let Ok(run) = scope.extract::<RunNumber>()
    {
        return Ok(RunSelection::runs([run]));
    }
    if let Ok(runs) = scope.extract::<Vec<RunNumber>>() {
        return Ok(RunSelection::runs(runs));
    }
    Err(PyTypeError::new_err(
        "run scope must be an integer, integer sequence, range, RunPeriod, period short name, RunSelection, or RunQuery",
    ))
}

#[pymethods]
impl PyRuns {
    /// Inspect condition definitions without issuing a run-value query.
    #[getter]
    pub(crate) fn conditions(&self) -> PyResult<PyConditionCatalog> {
        self.0
            .conditions()
            .map(PyConditionCatalog)
            .map_err(|error| super::exceptions::map(&error))
    }

    /// Build a lazy Run Query from a common Python run scope.
    fn select(&self, scope: &Bound<'_, PyAny>) -> PyResult<PyRunQuery> {
        if let Ok(query) = scope.extract::<PyRef<'_, PyRunQuery>>() {
            return Ok(query.clone());
        }
        self.0
            .runs(coerce_run_scope(scope)?)
            .map(PyRunQuery)
            .map_err(|error| super::exceptions::map(&error))
    }

    /// Build a lazy Run Query over inclusive numeric bounds.
    fn between(&self, start: RunNumber, end: RunNumber) -> PyResult<PyRunQuery> {
        self.0
            .runs(RunSelection::range(start, end))
            .map(PyRunQuery)
            .map_err(|error| super::exceptions::map(&error))
    }

    /// Compatibility call form; prefer `runs.select(scope)`.
    fn __call__(&self, scope: &Bound<'_, PyAny>) -> PyResult<PyRunQuery> {
        self.select(scope)
    }

    fn __repr__(&self) -> &'static str {
        "Runs(select=available, conditions=available)"
    }
}

/// Immutable validated database source identity.
#[pyclass(name = "SourceIdentity", module = "gluex", frozen)]
pub struct PySourceIdentity(crate::SourceIdentity);
#[pymethods]
impl PySourceIdentity {
    #[getter]
    fn value(&self) -> &str {
        self.0.as_str()
    }
    fn __str__(&self) -> &str {
        self.0.as_str()
    }
    fn __repr__(&self) -> String {
        format!("SourceIdentity({:?})", self.0.as_str())
    }
}

/// Numeric scope only; construction performs no I/O or approval selection.
#[pyclass(name = "RunSelection", module = "gluex", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyRunSelection(pub(crate) RunSelection);

#[pymethods]
impl PyRunSelection {
    /// Select explicit numbers, sorted and deduplicated.
    #[staticmethod]
    fn runs(numbers: Vec<RunNumber>) -> Self {
        Self(RunSelection::runs(numbers))
    }
    /// Select inclusive bounds without expanding the range. Reversed bounds are empty.
    #[staticmethod]
    fn range(start: RunNumber, end: RunNumber) -> Self {
        Self(RunSelection::range(start, end))
    }
    /// Select a run period's numeric bounds without a scientific cut.
    #[staticmethod]
    fn period(period: &PyRunPeriod) -> Self {
        Self(RunSelection::period(period.0))
    }
    fn __repr__(&self) -> String {
        format!("RunSelection({:?})", self.0)
    }
}

/// Source identity and numeric scope used to resolve recorded membership.
/// This identifies inputs but does not preserve historical file contents.
#[pyclass(name = "RunProvenance", module = "gluex", frozen)]
pub struct PyRunProvenance(pub(crate) RunProvenance);
#[pymethods]
impl PyRunProvenance {
    /// Explicit scientific predicates used by this query.
    #[getter]
    fn predicates(&self) -> TypedTuple<String> {
        TypedTuple(
            self.0
                .predicates()
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }
    /// RCDB filesystem identity.
    #[getter]
    fn source(&self) -> &str {
        self.0.source()
    }
    /// Validated source identity.
    #[getter]
    fn source_identity(&self) -> PySourceIdentity {
        PySourceIdentity(self.0.source_identity().clone())
    }
    /// Numeric scope requested before membership resolution.
    #[getter]
    fn selection(&self) -> PyRunSelection {
        PyRunSelection(self.0.selection().clone())
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

/// Immutable recorded run numbers, sorted and unique, with resolution provenance.
#[pyclass(name = "RunSet", module = "gluex", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyRunSet(pub(crate) RunSet);
#[pymethods]
impl PyRunSet {
    /// Completed evaluation diagnostics.
    #[getter]
    fn report(&self) -> PyRunReport {
        PyRunReport(self.0.report().clone())
    }
    /// Recorded run numbers as an immutable tuple.
    #[getter]
    fn numbers(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.numbers().to_vec())
    }
    /// Inputs used for the completed membership resolution.
    #[getter]
    fn provenance(&self) -> PyRunProvenance {
        PyRunProvenance(self.0.provenance().clone())
    }
    fn __len__(&self) -> usize {
        self.0.numbers().len()
    }
    fn __contains__(&self, run: RunNumber) -> bool {
        self.0.numbers().binary_search(&run).is_ok()
    }
    fn __getitem__(&self, index: isize) -> PyResult<RunNumber> {
        let index = if index < 0 {
            self.0.numbers().len() as isize + index
        } else {
            index
        };
        usize::try_from(index)
            .ok()
            .and_then(|index| self.0.numbers().get(index))
            .copied()
            .ok_or_else(|| PyIndexError::new_err("RunSet index out of range"))
    }
    fn __iter__(&self) -> TypedIterator<RunNumber> {
        TypedIterator(self.0.numbers().to_vec())
    }
    fn __repr__(&self) -> String {
        format!(
            "RunSet(len={}, source={:?})",
            self.0.numbers().len(),
            self.0.provenance().source()
        )
    }
}

/// Reusable lazy query; inspection does not retrieve runs. Call collect explicitly.
#[pyclass(name = "RunQuery", module = "gluex", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyRunQuery(pub(crate) RunQuery);
#[pymethods]
impl PyRunQuery {
    /// Return an immutable query with a timeout in seconds.
    fn timeout(&self, seconds: f64) -> PyResult<Self> {
        Ok(Self(
            self.0
                .with_timeout(crate::python::execution::timeout(seconds)?),
        ))
    }
    /// Project named conditions without reading values. Invalid names raise ValueError.
    fn select(&self, fields: Vec<String>) -> PyResult<PyConditionQuery> {
        self.0
            .select(fields)
            .map(PyConditionQuery)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Project named condition columns without retrieving their values.
    #[pyo3(signature = (*fields))]
    fn columns(&self, fields: &Bound<'_, PyTuple>) -> PyResult<PyConditionQuery> {
        self.0
            .select(fields.extract::<Vec<String>>()?)
            .map(PyConditionQuery)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    /// Return a new query with an additional predicate; no data is retrieved.
    fn r#where(&self, predicate: &PyRunPredicate) -> Self {
        Self(self.0.where_predicate(predicate.0.clone()))
    }
    /// Numeric scope, inspected without execution.
    #[getter]
    fn selection(&self) -> PyRunSelection {
        PyRunSelection(self.0.selection().clone())
    }
    /// Bound inputs, inspected without execution; not a completion report.
    #[getter]
    fn provenance(&self) -> PyRunProvenance {
        PyRunProvenance(self.0.provenance())
    }
    /// Collect recorded runs without implicit production cuts. Releases the GIL.
    /// Database execution errors raise RuntimeError.
    fn collect(&self, py: Python<'_>) -> PyResult<PyRunSet> {
        crate::python::execution::PythonExecution::execute(py, &self.0, RunQuery::collect)
            .map(PyRunSet)
    }
    /// Iterate bounded result chunks. Abandoning the iterator releases its reader.
    #[pyo3(signature = (*, chunk_size=1024))]
    fn stream(&self, chunk_size: usize) -> PyResult<PyRunStream> {
        crate::python::execution::PythonExecution::stream(&self.0, |query| query.stream(chunk_size))
            .map(|(stream, signals)| PyRunStream(stream, signals))
    }
    fn first(&self, py: Python<'_>) -> PyResult<Option<RunNumber>> {
        crate::python::execution::PythonExecution::execute(py, &self.0, RunQuery::first)
    }
    fn one(&self, py: Python<'_>) -> PyResult<RunNumber> {
        crate::python::execution::PythonExecution::execute(py, &self.0, RunQuery::one)
    }
    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        crate::python::execution::PythonExecution::execute(py, &self.0, RunQuery::count)
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

/// Iterator yielding bounded Run Set chunks.
#[pyclass(name = "RunStream", module = "gluex")]
pub struct PyRunStream(crate::RunStream, crate::python::execution::PythonExecution);
#[pymethods]
impl PyRunStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyRunSet>> {
        Ok(self.1.next(py, || self.0.next().transpose())?.map(PyRunSet))
    }
}

/// Immutable database-defined condition name, type, description and native identifier.
#[pyclass(name = "ConditionDefinition", module = "gluex", frozen)]
pub struct PyConditionDefinition(ConditionDefinition);
#[pymethods]
impl PyConditionDefinition {
    fn __eq__(&self, _other: &Bound<'_, PyAny>) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "Use definition.eq(value) to build an equality predicate",
        ))
    }
    fn __ne__(&self, _other: &Bound<'_, PyAny>) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "Use definition.ne(value) to build an inequality predicate",
        ))
    }

    /// True for a recorded non-null value.
    fn is_present(&self) -> PyRunPredicate {
        PyRunPredicate(self.0.is_present())
    }
    /// True for an absent or null value.
    fn is_missing(&self) -> PyRunPredicate {
        PyRunPredicate(self.0.is_missing())
    }
    /// Build a typed equality predicate; missing inputs stay unknown.
    fn eq(&self, py: Python<'_>, value: Operand) -> PyResult<PyRunPredicate> {
        self.0
            .eq(value.native(py, &self.0)?)
            .map(PyRunPredicate)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    /// Build a typed inequality predicate; missing inputs stay unknown.
    fn ne(&self, py: Python<'_>, value: Operand) -> PyResult<PyRunPredicate> {
        self.0
            .ne(value.native(py, &self.0)?)
            .map(PyRunPredicate)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    fn __gt__(&self, py: Python<'_>, value: Operand) -> PyResult<PyRunPredicate> {
        self.0
            .gt(value.native(py, &self.0)?)
            .map(PyRunPredicate)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    fn __ge__(&self, py: Python<'_>, value: Operand) -> PyResult<PyRunPredicate> {
        self.0
            .ge(value.native(py, &self.0)?)
            .map(PyRunPredicate)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    fn __lt__(&self, py: Python<'_>, value: Operand) -> PyResult<PyRunPredicate> {
        self.0
            .lt(value.native(py, &self.0)?)
            .map(PyRunPredicate)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    fn __le__(&self, py: Python<'_>, value: Operand) -> PyResult<PyRunPredicate> {
        self.0
            .le(value.native(py, &self.0)?)
            .map(PyRunPredicate)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Database-native identifier.
    #[getter]
    fn id(&self) -> i64 {
        self.0.id()
    }
    /// Dynamic condition name.
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }
    /// RCDB value type identifier.
    #[getter]
    fn value_type(&self) -> &str {
        self.0.value_type().as_str()
    }
    /// Available description, or an empty string.
    #[getter]
    fn description(&self) -> &str {
        self.0.description()
    }
    /// Native creation timestamp text, or an empty string.
    #[getter]
    fn created(&self) -> String {
        self.0.created()
    }
    fn __repr__(&self) -> String {
        format!(
            "ConditionDefinition(name={:?}, value_type={:?})",
            self.0.name(),
            self.0.value_type().as_str()
        )
    }
}

/// Immutable mapping of dynamic condition names to definitions, in lexical order.
#[pyclass(name = "ConditionCatalog", module = "gluex", frozen)]
pub struct PyConditionCatalog(pub(crate) ConditionCatalog);
#[pymethods]
impl PyConditionCatalog {
    /// Return names in lexical order as an immutable tuple.
    fn keys(&self) -> TypedTuple<String> {
        TypedTuple(self.0.keys().cloned().collect())
    }
    /// Return name/definition pairs in lexical order as an immutable tuple.
    fn items(&self) -> TypedTuple<(String, PyConditionDefinition)> {
        TypedTuple(
            self.0
                .items()
                .map(|(name, value)| (name.clone(), PyConditionDefinition(value.clone())))
                .collect(),
        )
    }
    fn __len__(&self) -> usize {
        self.0.len()
    }
    fn __contains__(&self, name: &str) -> bool {
        self.0.get(name).is_some()
    }
    fn __getitem__(&self, name: &str) -> PyResult<PyConditionDefinition> {
        self.0
            .get(name)
            .cloned()
            .map(PyConditionDefinition)
            .ok_or_else(|| PyKeyError::new_err(name.to_owned()))
    }
    fn __iter__(&self) -> TypedIterator<String> {
        TypedIterator(self.0.keys().cloned().collect())
    }
    fn __repr__(&self) -> String {
        format!("ConditionCatalog(len={})", self.0.len())
    }
}

/// Composable three-valued predicate. Use &, | and ~; Python truth conversion is forbidden.
#[pyclass(name = "RunPredicate", module = "gluex", frozen)]
pub struct PyRunPredicate(crate::RunPredicate);
#[pymethods]
impl PyRunPredicate {
    fn __and__(&self, other: &Self) -> Self {
        Self(self.0.clone() & other.0.clone())
    }
    fn __or__(&self, other: &Self) -> Self {
        Self(self.0.clone() | other.0.clone())
    }
    fn __invert__(&self) -> Self {
        Self(!self.0.clone())
    }
    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "Use &, | and ~ to compose predicates, then query.collect(); predicates have no Python truth value",
        ))
    }
    fn __repr__(&self) -> String {
        format!("RunPredicate({})", self.0)
    }
}

/// Completed evaluation diagnostics; these runs had final-unknown predicates.
#[pyclass(name = "RunReport", module = "gluex", frozen)]
pub struct PyRunReport(pub(crate) crate::RunReport);
#[pymethods]
impl PyRunReport {
    /// Recorded runs excluded because the complete predicate was unknown.
    #[getter]
    fn unknown_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.unknown_runs().to_vec())
    }
    #[getter]
    fn evaluated_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.evaluated_runs().to_vec())
    }
    #[getter]
    fn complete(&self) -> bool {
        self.0.complete()
    }
    /// Structured evaluated-run, omission and completion accounting.
    #[getter]
    fn accounting(&self) -> PyRunAccounting {
        PyRunAccounting(self.0.accounting())
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

#[pyclass(name = "RunAccounting", module = "gluex", frozen)]
pub struct PyRunAccounting(crate::RunAccounting);
#[pymethods]
impl PyRunAccounting {
    #[getter]
    fn evaluated_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.evaluated_runs().to_vec())
    }
    #[getter]
    fn omissions(&self) -> TypedTuple<PyRunOmission> {
        TypedTuple(
            self.0
                .omissions()
                .iter()
                .copied()
                .map(PyRunOmission)
                .collect(),
        )
    }
    #[getter]
    fn complete(&self) -> bool {
        self.0.complete()
    }
    fn __repr__(&self) -> String {
        format!(
            "RunAccounting(evaluated_runs={}, omissions={}, complete={})",
            self.0.evaluated_runs().len(),
            self.0.omissions().len(),
            self.0.complete()
        )
    }
}

#[pyclass(name = "RunOmission", module = "gluex", frozen)]
pub struct PyRunOmission(crate::RunOmission);
#[pymethods]
impl PyRunOmission {
    #[getter]
    fn run(&self) -> RunNumber {
        self.0.run()
    }
    #[getter]
    fn reason(&self) -> PyRunOmissionReason {
        self.0.reason().into()
    }
    fn __repr__(&self) -> String {
        format!(
            "RunOmission(run={}, reason={:?})",
            self.0.run(),
            self.0.reason()
        )
    }
}

#[pyclass(
    name = "RunOmissionReason",
    module = "gluex",
    frozen,
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyRunOmissionReason {
    UnknownPredicate = 0,
}
impl From<crate::RunOmissionReason> for PyRunOmissionReason {
    fn from(value: crate::RunOmissionReason) -> Self {
        match value {
            crate::RunOmissionReason::UnknownPredicate => Self::UnknownPredicate,
        }
    }
}

#[derive(FromPyObject)]
pub enum Operand {
    Bool(Py<pyo3::types::PyBool>),
    Int(Py<pyo3::types::PyInt>),
    Float(f64),
    Text(String),
    Time(chrono::DateTime<chrono::Utc>),
}
impl Operand {
    fn native(
        self,
        py: Python<'_>,
        definition: &ConditionDefinition,
    ) -> PyResult<crate::ConditionOperand> {
        Ok(match self {
            Self::Bool(v) => crate::ConditionOperand::Bool(v.bind(py).extract()?),
            Self::Int(v) => {
                if definition.value_type() == crate::ConditionValueType::Float {
                    crate::ConditionOperand::Float(v.bind(py).extract()?)
                } else {
                    crate::ConditionOperand::Int(v.bind(py).extract()?)
                }
            }
            Self::Float(v) => crate::ConditionOperand::Float(v),
            Self::Text(v) => crate::ConditionOperand::Text(v),
            Self::Time(v) => crate::ConditionOperand::Time(v),
        })
    }
}

/// Explicit named approved-production cut for a supported period; never applied automatically.
#[pyfunction]
pub fn approved_production(period: &PyRunPeriod) -> PyResult<PyRunPredicate> {
    crate::approved_production(period.0)
        .map(PyRunPredicate)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Lazy condition projection; collect returns ConditionResults and releases the GIL.
#[pyclass(name = "ConditionQuery", module = "gluex", frozen)]
pub struct PyConditionQuery(crate::ConditionQuery);
#[pymethods]
impl PyConditionQuery {
    /// Return an immutable query with a timeout in seconds.
    fn timeout(&self, seconds: f64) -> PyResult<Self> {
        Ok(Self(self.0.query_with_timeout(
            crate::python::execution::timeout(seconds)?,
        )))
    }
    /// Captured source, predicates and fields, without evaluation.
    #[getter]
    fn provenance(&self) -> PyConditionProvenance {
        PyConditionProvenance(self.0.provenance())
    }
    /// Collect optional columns; malformed values and execution failures raise RuntimeError.
    fn collect(&self, py: Python<'_>) -> PyResult<PyConditionResults> {
        crate::python::execution::PythonExecution::execute(py, &self.0, |query| query.collect())
            .map(PyConditionResults)
    }
    #[pyo3(signature = (*, chunk_size=1024))]
    fn stream(&self, chunk_size: usize) -> PyResult<PyConditionStream> {
        crate::python::execution::PythonExecution::stream(&self.0, |query| query.stream(chunk_size))
            .map(|(stream, signals)| PyConditionStream(stream, signals))
    }
    fn first(&self, py: Python<'_>) -> PyResult<Option<PyConditionResults>> {
        crate::python::execution::PythonExecution::execute(py, &self.0, |query| query.first())
            .map(|value| value.map(PyConditionResults))
    }
    fn one(&self, py: Python<'_>) -> PyResult<PyConditionResults> {
        crate::python::execution::PythonExecution::execute(py, &self.0, |query| query.one())
            .map(PyConditionResults)
    }
    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        crate::python::execution::PythonExecution::execute(py, &self.0, |query| query.count())
    }
    fn strict(&self) -> Self {
        Self(self.0.strict())
    }
    #[pyo3(signature = (name, *, value))]
    fn fill(&self, py: Python<'_>, name: &str, value: Operand) -> PyResult<Self> {
        let definition = self
            .0
            .definition(name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        self.0
            .fill(name, value.native(py, &definition)?)
            .map(Self)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

/// Iterator yielding bounded condition-result chunks.
#[pyclass(name = "ConditionStream", module = "gluex")]
pub struct PyConditionStream(
    crate::ConditionStream,
    crate::python::execution::PythonExecution,
);
#[pymethods]
impl PyConditionStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyConditionResults>> {
        Ok(self
            .1
            .next(py, || self.0.next().transpose())?
            .map(PyConditionResults))
    }
}

/// Source, membership query and ordered projected names.
#[pyclass(name = "ConditionProvenance", module = "gluex", frozen)]
pub struct PyConditionProvenance(crate::ConditionProvenance);
#[pymethods]
impl PyConditionProvenance {
    /// Source, numeric scope and predicates for recorded membership.
    #[getter]
    fn runs(&self) -> PyRunProvenance {
        PyRunProvenance(self.0.runs().clone())
    }
    /// Ordered names of projected conditions.
    #[getter]
    fn fields(&self) -> TypedTuple<String> {
        TypedTuple(self.0.fields().to_vec())
    }
    #[getter]
    fn missing_policy(&self) -> &'static str {
        self.0.policy().as_str()
    }
    #[getter]
    fn fallback_fields(&self) -> TypedTuple<String> {
        TypedTuple(self.0.fallback_fields().to_vec())
    }
    /// Immutable structured Missing Data Policy configuration.
    #[getter]
    fn missing_data(&self) -> PyMissingDataConfig {
        PyMissingDataConfig(self.0.missing_data())
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

#[pyclass(name = "MissingDataConfig", module = "gluex", frozen)]
pub struct PyMissingDataConfig(crate::MissingDataConfig);
#[pymethods]
impl PyMissingDataConfig {
    #[getter]
    fn policy(&self) -> &'static str {
        self.0.policy().as_str()
    }
    #[getter]
    fn fallback_fields(&self) -> TypedTuple<String> {
        TypedTuple(self.0.fallback_fields().to_vec())
    }
    fn __repr__(&self) -> String {
        format!(
            "MissingDataConfig(policy={:?}, fallback_fields={:?})",
            self.0.policy().as_str(),
            self.0.fallback_fields()
        )
    }
}

/// Missing cells from a completed condition projection.
#[pyclass(name = "ConditionReport", module = "gluex", frozen)]
pub struct PyConditionReport(crate::ConditionReport);
#[pymethods]
impl PyConditionReport {
    /// Missing (run number, condition name) pairs in run/name order.
    #[getter]
    fn missing_values(&self) -> TypedTuple<(RunNumber, String)> {
        TypedTuple(self.0.missing_values().to_vec())
    }
    #[getter]
    fn substitutions(&self) -> TypedTuple<(RunNumber, String)> {
        TypedTuple(self.0.substitutions().to_vec())
    }
    /// Structured missing cells.
    #[getter]
    fn omissions(&self) -> TypedTuple<PyConditionOmission> {
        TypedTuple(
            self.0
                .omissions()
                .into_iter()
                .map(PyConditionOmission)
                .collect(),
        )
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

#[pyclass(name = "ConditionOmission", module = "gluex", frozen)]
pub struct PyConditionOmission(crate::ConditionOmission);
#[pymethods]
impl PyConditionOmission {
    #[getter]
    fn run(&self) -> RunNumber {
        self.0.run()
    }
    #[getter]
    fn condition(&self) -> &str {
        self.0.condition()
    }
    fn __repr__(&self) -> String {
        format!(
            "ConditionOmission(run={}, condition={:?})",
            self.0.run(),
            self.0.condition()
        )
    }
}

#[derive(IntoPyObject)]
pub enum ConditionScalar {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Time(chrono::DateTime<chrono::Utc>),
}
impl From<&crate::ConditionValue> for ConditionScalar {
    fn from(value: &crate::ConditionValue) -> Self {
        use crate::ConditionValueType;
        match value.value_type() {
            ConditionValueType::Bool => Self::Bool(value.as_bool().unwrap()),
            ConditionValueType::Int => Self::Int(value.as_int().unwrap()),
            ConditionValueType::Float => Self::Float(value.as_float().unwrap()),
            ConditionValueType::Time => Self::Time(value.as_time().unwrap()),
            _ => Self::Text(value.as_string().unwrap().into()),
        }
    }
}

/// Immutable optional condition values indexed by (run, name), with aligned columns.
#[pyclass(name = "ConditionResults", module = "gluex", frozen)]
pub struct PyConditionResults(crate::ConditionResults);
#[pymethods]
impl PyConditionResults {
    /// Recorded runs in column order, including predicate-exclusion diagnostics.
    #[getter]
    fn runs(&self) -> PyRunSet {
        PyRunSet(self.0.runs().clone())
    }
    /// Inputs used for the completed collection.
    #[getter]
    fn provenance(&self) -> PyConditionProvenance {
        PyConditionProvenance(self.0.provenance().clone())
    }
    /// Missing requested cells, represented by None in values and columns.
    #[getter]
    fn report(&self) -> PyConditionReport {
        PyConditionReport(self.0.report().clone())
    }
    /// Return an immutable column aligned with runs.numbers; unknown names raise KeyError.
    fn column(&self, name: &str) -> PyResult<TypedTuple<Option<ConditionScalar>>> {
        self.0
            .column(name)
            .map(|values| TypedTuple(values.iter().map(|v| v.as_ref().map(Into::into)).collect()))
            .map_err(|e| PyKeyError::new_err(e.to_string()))
    }
    fn __getitem__(&self, key: (RunNumber, String)) -> PyResult<Option<ConditionScalar>> {
        self.0
            .get(key.0, &key.1)
            .map(|v| v.map(Into::into))
            .map_err(|e| PyKeyError::new_err(e.to_string()))
    }
    fn __len__(&self) -> usize {
        self.0.runs().numbers().len()
    }
    fn __repr__(&self) -> String {
        format!(
            "ConditionResults(runs={}, fields={:?})",
            self.0.runs().numbers().len(),
            self.0.provenance().fields()
        )
    }
}
