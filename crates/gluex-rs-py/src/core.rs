use gluex_core::Histogram;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};

/// A one-dimensional histogram with per-bin uncertainties.
#[pyclass(name = "Histogram", module = "gluex")]
pub struct PyHistogram(pub(crate) Histogram);

#[pymethods]
impl PyHistogram {
    #[new]
    #[pyo3(signature = (counts, edges, errors=None))]
    fn new(counts: Vec<f64>, edges: Vec<f64>, errors: Option<Vec<f64>>) -> PyResult<Self> {
        Histogram::new(&counts, &edges, errors.as_deref())
            .map(Self)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    #[getter]
    fn counts(&self) -> Vec<f64> {
        self.0.counts.clone()
    }

    #[getter]
    fn edges(&self) -> Vec<f64> {
        self.0.edges.clone()
    }

    #[getter]
    fn errors(&self) -> Vec<f64> {
        self.0.errors.clone()
    }

    /// Return the histogram data as serializable lists.
    pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        histogram_to_dict(py, &self.0)
    }
}

pub(crate) fn histogram_to_dict(py: Python<'_>, histogram: &Histogram) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("counts", histogram.counts.clone())?;
    dict.set_item("edges", histogram.edges.clone())?;
    dict.set_item("errors", histogram.errors.clone())?;
    Ok(dict.unbind())
}

pub(crate) fn histogram_to_py(py: Python<'_>, histogram: &Histogram) -> PyResult<Py<PyHistogram>> {
    Py::new(py, PyHistogram(histogram.clone()))
}
