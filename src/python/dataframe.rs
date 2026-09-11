use pyo3::{inspect::PyStaticExpr, prelude::*};
use pyo3_polars::PyDataFrame;

/// Shared zero-copy boundary from native Polars frames to Python.
pub(crate) struct PolarsDataFrame(pub(crate) PyDataFrame);

impl<'py> IntoPyObject<'py> for PolarsDataFrame {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    const OUTPUT_TYPE: PyStaticExpr = pyo3::type_hint_identifier!("polars", "DataFrame");

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_pyobject(py)
    }
}
