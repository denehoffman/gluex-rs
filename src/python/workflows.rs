use pyo3::{prelude::*, types::PyDict};

use super::{
    calibrations::PyReconstructionSelection,
    execution::PythonExecution,
    lumi::lumi::PyFluxHistograms,
    runs::{PyRunProvenance, PyRunSet},
    tuple::TypedTuple,
};
use crate::{LuminosityQuery, LuminosityResult, RunNumber, Workflows};

/// Canonical GlueX workflows bound to one source generation.
#[pyclass(name = "Workflows", module = "gluex", frozen)]
pub struct PyWorkflows(pub(crate) Workflows);

#[pymethods]
impl PyWorkflows {
    /// Build a lazy luminosity request from a resolved RunSet and explicit reconstruction.
    #[pyo3(signature = (runs, *, reconstruction, edges))]
    fn luminosity(
        &self,
        runs: &PyRunSet,
        reconstruction: &PyReconstructionSelection,
        edges: Vec<f64>,
    ) -> PyLuminosityQuery {
        PyLuminosityQuery(self.0.luminosity(&runs.0, reconstruction.0.clone(), edges))
    }

    fn __repr__(&self) -> &'static str {
        "Workflows(luminosity=available)"
    }
}

/// Lazy canonical luminosity request; strict missing-data behavior is the default.
#[pyclass(name = "LuminosityQuery", module = "gluex", frozen)]
pub struct PyLuminosityQuery(LuminosityQuery);

#[pymethods]
impl PyLuminosityQuery {
    #[pyo3(signature = (*, enabled))]
    fn coherent_peak(&self, enabled: bool) -> Self {
        Self(self.0.with_coherent_peak(enabled))
    }

    #[pyo3(signature = (*, enabled))]
    fn polarized(&self, enabled: bool) -> Self {
        Self(self.0.with_polarized(enabled))
    }

    /// Exclude and report genuinely missing inputs instead of rejecting the result.
    fn report_missing(&self) -> Self {
        Self(self.0.report_missing())
    }

    /// Substitute an explicitly chosen run when a selected run lacks luminosity inputs.
    #[pyo3(signature = (*, run))]
    fn fallback_to(&self, run: RunNumber) -> Self {
        Self(self.0.fallback_to(run))
    }

    /// Return an immutable request with a timeout in seconds.
    #[pyo3(signature = (*, seconds))]
    fn timeout(&self, seconds: f64) -> PyResult<Self> {
        Ok(Self(
            self.0.with_timeout(super::execution::timeout(seconds)?),
        ))
    }

    /// Evaluate while releasing the GIL and polling Python signals.
    fn collect(&self, py: Python<'_>) -> PyResult<PyLuminosityResult> {
        PythonExecution::execute(py, &self.0, LuminosityQuery::collect).map(PyLuminosityResult)
    }

    fn __repr__(&self) -> &'static str {
        "LuminosityQuery(lazy=True)"
    }
}

/// Completed canonical luminosity histograms and retained evidence.
#[pyclass(name = "LuminosityResult", module = "gluex", frozen)]
pub struct PyLuminosityResult(LuminosityResult);

#[pymethods]
impl PyLuminosityResult {
    #[getter]
    fn histograms(&self) -> PyFluxHistograms {
        PyFluxHistograms(self.0.histograms().clone())
    }

    #[getter]
    fn report(&self) -> PyLuminosityReport {
        PyLuminosityReport(self.0.report().clone())
    }

    #[getter]
    fn provenance(&self) -> PyLuminosityProvenance {
        PyLuminosityProvenance(self.0.provenance().clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "LuminosityResult(used_runs={}, procedure_version={:?})",
            self.0.report().used_runs().len(),
            self.0.provenance().procedure_version()
        )
    }
}

#[pyclass(name = "LuminosityReport", module = "gluex", frozen)]
pub struct PyLuminosityReport(crate::LuminosityReport);

#[pymethods]
impl PyLuminosityReport {
    #[getter]
    fn selected_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.selected_runs().to_vec())
    }
    #[getter]
    fn used_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.used_runs().to_vec())
    }
    #[getter]
    fn excluded_runs(&self) -> TypedTuple<(RunNumber, String)> {
        TypedTuple(self.0.excluded_runs().to_vec())
    }
    #[getter]
    fn substitutions(&self) -> TypedTuple<(RunNumber, RunNumber)> {
        TypedTuple(self.0.substitutions().to_vec())
    }
    #[getter]
    fn complete(&self) -> bool {
        self.0.complete()
    }
}

#[pyclass(name = "LuminosityProvenance", module = "gluex", frozen)]
pub struct PyLuminosityProvenance(crate::LuminosityProvenance);

#[pymethods]
impl PyLuminosityProvenance {
    #[getter]
    fn runs(&self) -> PyRunProvenance {
        PyRunProvenance(self.0.runs().clone())
    }
    #[getter]
    fn rcdb_source(&self) -> &str {
        self.0.rcdb_source()
    }
    #[getter]
    fn ccdb_source(&self) -> &str {
        self.0.ccdb_source()
    }
    #[getter]
    fn requested_reconstruction(&self) -> PyReconstructionSelection {
        PyReconstructionSelection(self.0.requested_reconstruction().clone())
    }
    #[getter]
    fn calibration_default_as_of(&self) -> chrono::DateTime<chrono::Utc> {
        self.0.calibration_default_as_of()
    }
    #[getter]
    fn procedure_version(&self) -> &str {
        self.0.procedure_version()
    }
    #[getter]
    fn procedure_status(&self) -> &str {
        self.0.procedure_status()
    }
    #[getter]
    fn references(&self) -> TypedTuple<String> {
        TypedTuple(
            self.0
                .references()
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }
    #[getter]
    fn assumptions(&self) -> TypedTuple<String> {
        TypedTuple(
            self.0
                .assumptions()
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }
    #[getter]
    fn exceptions(&self) -> TypedTuple<String> {
        TypedTuple(
            self.0
                .exceptions()
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }
    #[getter]
    fn validation_gaps(&self) -> TypedTuple<String> {
        TypedTuple(
            self.0
                .validation_gaps()
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }
    #[getter]
    fn missing_policy(&self) -> &str {
        self.0.missing_policy().as_str()
    }
    #[getter]
    fn coherent_peak(&self) -> bool {
        self.0.coherent_peak()
    }
    #[getter]
    fn polarized(&self) -> bool {
        self.0.polarized()
    }
    #[getter]
    fn resolved_reconstruction(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);
        for (period, context) in self.0.resolved_reconstruction() {
            result.set_item(
                period.short_name(),
                (context.variation.clone(), context.timestamp),
            )?;
        }
        Ok(result.unbind())
    }
}
