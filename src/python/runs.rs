use super::{
    core::PyRunPeriod,
    tuple::{TypedIterator, TypedTuple},
};
use crate::{
    ConditionCatalog, ConditionDefinition, RunNumber, RunProvenance, RunQuery, RunSelection, RunSet,
};
use pyo3::{
    exceptions::{PyIndexError, PyKeyError, PyRuntimeError},
    prelude::*,
};

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
pub struct PyRunProvenance(RunProvenance);
#[pymethods]
impl PyRunProvenance {
    /// RCDB filesystem identity.
    #[getter]
    fn source(&self) -> &str {
        self.0.source()
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
#[pyclass(name = "RunSet", module = "gluex", frozen)]
pub struct PyRunSet(RunSet);
#[pymethods]
impl PyRunSet {
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
#[pyclass(name = "RunQuery", module = "gluex", frozen)]
pub struct PyRunQuery(pub(crate) RunQuery);
#[pymethods]
impl PyRunQuery {
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
        py.detach(|| self.0.collect())
            .map(PyRunSet)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

/// Immutable database-defined condition name, type, description and native identifier.
#[pyclass(name = "ConditionDefinition", module = "gluex", frozen)]
pub struct PyConditionDefinition(ConditionDefinition);
#[pymethods]
impl PyConditionDefinition {
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
