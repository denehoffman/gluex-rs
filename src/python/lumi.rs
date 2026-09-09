#[pyo3::pymodule(submodule)]
pub(crate) mod lumi {
    use crate::lumi::FluxHistograms;
    use pyo3::{prelude::*, types::PyDict};

    use crate::python::core::{PyHistogram, histogram_to_dict, histogram_to_py};

    /// Flux and luminosity histograms aggregated across selected runs.
    #[pyclass(name = "FluxHistograms", module = "gluex.lumi")]
    pub struct PyFluxHistograms(pub(crate) FluxHistograms);

    #[pymethods]
    impl PyFluxHistograms {
        #[getter]
        fn tagged_flux(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagged_flux)
        }
        #[getter]
        fn tagm_flux(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagm_flux)
        }
        #[getter]
        fn tagh_flux(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagh_flux)
        }
        #[getter]
        fn tagged_luminosity(&self, py: Python<'_>) -> PyResult<Py<PyHistogram>> {
            histogram_to_py(py, &self.0.tagged_luminosity)
        }
        /// Return every histogram as serializable lists.
        pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
            let dict = PyDict::new(py);
            dict.set_item("tagged_flux", histogram_to_dict(py, &self.0.tagged_flux)?)?;
            dict.set_item("tagm_flux", histogram_to_dict(py, &self.0.tagm_flux)?)?;
            dict.set_item("tagh_flux", histogram_to_dict(py, &self.0.tagh_flux)?)?;
            dict.set_item(
                "tagged_luminosity",
                histogram_to_dict(py, &self.0.tagged_luminosity)?,
            )?;
            Ok(dict.unbind())
        }
    }
}
